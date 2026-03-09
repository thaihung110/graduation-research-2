"""
DAG: crypto-ohlcv-silver-batch

Batch job: reads data from Bronze layer, aggregates OHLCV, writes to Silver layer.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict

import jinja2
import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import BaseOperatorLink, XCom
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstanceKey
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from spark_lifecycle_trigger import SparkLifecycleTrigger

logger = logging.getLogger("airflow.task")


# ==============================================================================
# SPARK HISTORY LINK
# ==============================================================================


class SparkHistoryLink(BaseOperatorLink):
    name = "Spark History"

    def get_link(self, operator, *, ti_key: TaskInstanceKey) -> str:
        HISTORY_HOST = "https://openhouse.spark-history.test"
        try:
            spark_app_id = XCom.get_value(key="spark_app_id", ti_key=ti_key)
            if spark_app_id:
                return f"{HISTORY_HOST}/history/{spark_app_id}"
        except Exception:
            pass
        return HISTORY_HOST


# ==============================================================================
# SPARK LIFECYCLE TRIGGER (Async)
# ==============================================================================
# NOTE: SparkLifecycleTrigger is imported from spark_lifecycle_trigger.py
# The trigger class MUST NOT be defined inside the DAG file because Airflow
# loads DAGs with a hashed module prefix 'unusual_prefix_...' which prevents
# the Triggerer from importing it.


# ==============================================================================
# SPARK LIFECYCLE SENSOR
# ==============================================================================


class SparkLifecycleSensor(BaseSensorOperator):
    operator_extra_links = (SparkHistoryLink(),)
    template_fields = ("name", "namespace")

    def __init__(self, name: str, namespace: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.namespace = namespace

    def execute(self, context: Context):
        self.defer(
            trigger=SparkLifecycleTrigger(
                name=self.name, namespace=self.namespace
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]):
        status = event.get("status")
        app_id = event.get("spark_app_id")
        msg = event.get("message", "")

        if app_id:
            context["ti"].xcom_push(key="spark_app_id", value=app_id)

        if status == "success":
            logger.info(f"[OK] Spark Job Succeeded. App ID: {app_id}")
            return

        logger.warning(
            f"Trigger reported: {status}. Verifying via Worker API..."
        )
        real_state, real_id = self._verify_status_sync()

        if real_state in ["COMPLETED", "SUCCEEDED"]:
            logger.info(
                f"[OK] Verified Success via Worker API. App ID: {real_id}"
            )
            if real_id:
                context["ti"].xcom_push(key="spark_app_id", value=real_id)
            return

        raise AirflowException(
            f"Spark Job Failed. Final State: {real_state}. Details: {msg}"
        )

    def _verify_status_sync(self):
        try:
            hook = KubernetesHook(conn_id="kubernetes_default")
            crd = hook.get_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=self.name,
            )
            status = crd.get("status", {})
            state = status.get("applicationState", {}).get("state", "UNKNOWN")
            app_id = status.get("sparkApplicationId") or status.get(
                "applicationState", {}
            ).get("sparkApplicationId")
            return state, app_id
        except Exception as e:
            logger.error(f"Sync Verification Failed: {e}")
            return "UNKNOWN", None


# ==============================================================================
# DICT SPARK KUBERNETES OPERATOR
# ==============================================================================


class DictSparkKubernetesOperator(SparkKubernetesOperator):
    template_fields = list(SparkKubernetesOperator.template_fields) + [
        "dry_run"
    ]

    def __init__(self, dry_run=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dry_run = dry_run

    def execute(self, context):
        if isinstance(self.application_file, dict):
            body = self.application_file
            meta = body.get("metadata", {})
            name = meta.get("name")
            ns = self.namespace or meta.get("namespace", "default")

            if str(self.dry_run).lower() in ["true", "1", "yes"]:
                return {"job_name": name, "namespace": ns}

            hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
            logger.info(f"Submitting SparkApplication: {name}")
            hook.create_custom_object(
                "sparkoperator.k8s.io", "v1beta2", "sparkapplications", body, ns
            )

            context["ti"].xcom_push(key="job_name", value=name)
            context["ti"].xcom_push(key="namespace", value=ns)

            return {"job_name": name, "namespace": ns}
        else:
            return super().execute(context)


# ==============================================================================
# FAILURE CALLBACK
# ==============================================================================


def delete_spark_job_on_failure(context):
    ti = context["ti"]
    job_details = ti.xcom_pull(task_ids="submit_spark_job", key="return_value")

    if not job_details:
        logger.warning(
            "[WARN] No job details in XCom. Job might not have started."
        )
        return

    name = job_details.get("job_name")
    namespace = job_details.get("namespace")

    logger.info(f"[ACTION] Deleting Spark Job on failure: {name}...")
    try:
        hook = KubernetesHook(conn_id="kubernetes_default")
        hook.delete_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=namespace,
            plural="sparkapplications",
            name=name,
        )
        logger.info(f"[OK] Spark Job Deleted: {name}")
    except Exception as e:
        logger.error(f"[ERROR] Failed to delete job: {e}")


# ==============================================================================
# SPARK YAML MANIFEST TEMPLATE
# Embedded from: manifests/transform-crypto-silver-batch.yaml.j2
# ==============================================================================
SPARK_YAML_TEMPLATE = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
    name: "{{ job_name }}"
    namespace: "default"
    labels:
        app: "{{ job_name_prefix }}"
        component: etl
        layer: silver
spec:
    type: Python
    mode: cluster
    pythonVersion: "3"
    sparkVersion: "3.5.0"

    image: "{{ image_repo }}:{{ image_tag }}"
    imagePullPolicy: Always

    mainApplicationFile: "{{ main_file_path }}"

    # JARs are baked into the Docker image - no spark.jars.packages needed

    sparkConf:
        "spark.eventLog.enabled": "true"
        "spark.eventLog.dir": "s3a://spark-logs/event-logs"
        "spark.eventLog.compress": "true"

    hadoopConf:
        # Global settings
        "fs.s3a.endpoint": "http://openhouse-minio:9000"
        "fs.s3a.path.style.access": "true"
        "fs.s3a.connection.ssl.enabled": "false"
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        "fs.s3a.metadatastore.impl": "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore"

        # Per-bucket: bronze (endpoint only — credentials handled by vended credentials)
        "fs.s3a.bucket.bronze.endpoint": "http://openhouse-minio:9000"

        # Per-bucket: silver (endpoint only — credentials handled by vended credentials)
        "fs.s3a.bucket.silver.endpoint": "http://openhouse-minio:9000"

        # Per-bucket: spark-logs (not managed by Iceberg catalog, needs explicit credentials)
        "fs.s3a.bucket.spark-logs.endpoint":   "http://openhouse-minio-log:9000"
        "fs.s3a.bucket.spark-logs.access.key": "admin"
        "fs.s3a.bucket.spark-logs.secret.key": "admin123"

    driver:
        cores: {{ driver_cores }}
        coreLimit: "{{ driver_cores }}200m"
        memory: "{{ driver_memory }}"
        memoryOverhead: "512m"
        serviceAccount: "openhouse-spark-operator-spark"
        labels:
            version: 3.5.0
        env:
            - name: HOME
              value: "/tmp"
            - name: SPARK_MINOR_VERSION
              value: "3.5"
            - name: ICEBERG_VERSION
              value: "1.10.1"
            - name: BRONZE_CATALOG_URL
              value: "http://openhouse-lakekeeper:8181/catalog"
            - name: BRONZE_CLIENT_ID
              value: "spark"
            - name: BRONZE_CLIENT_SECRET
              value: "YeG2U2zPQqnLoIfD3Bc3c55pfIUnDNFd"
            - name: BRONZE_WAREHOUSE
              value: "bronze"
            - name: SILVER_CATALOG_URL
              value: "http://openhouse-lakekeeper:8181/catalog"
            - name: SILVER_CLIENT_ID
              value: "spark"
            - name: SILVER_CLIENT_SECRET
              value: "YeG2U2zPQqnLoIfD3Bc3c55pfIUnDNFd"
            - name: SILVER_WAREHOUSE
              value: "silver"
            - name: KEYCLOAK_TOKEN_ENDPOINT
              value: "http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token"
            - name: AWS_ACCESS_KEY_ID
              value: "admin"
            - name: AWS_SECRET_ACCESS_KEY
              value: "admin123"
            {% for key, value in user_env_vars.items() %}
            - name: {{ key }}
              value: "{{ value }}"
            {% endfor %}

    executor:
        cores: {{ executor_cores }}
        instances: {{ executor_instances }}
        memory: "{{ executor_memory }}"
        memoryOverhead: "512m"
        labels:
            version: 3.5.0
        env:
            - name: HOME
              value: "/tmp"
            - name: SPARK_MINOR_VERSION
              value: "3.5"
            - name: ICEBERG_VERSION
              value: "1.10.1"
            - name: BRONZE_CATALOG_URL
              value: "http://openhouse-lakekeeper:8181/catalog"
            - name: BRONZE_CLIENT_ID
              value: "spark"
            - name: BRONZE_CLIENT_SECRET
              value: "YeG2U2zPQqnLoIfD3Bc3c55pfIUnDNFd"
            - name: BRONZE_WAREHOUSE
              value: "bronze"
            - name: SILVER_CATALOG_URL
              value: "http://openhouse-lakekeeper:8181/catalog"
            - name: SILVER_CLIENT_ID
              value: "spark"
            - name: SILVER_CLIENT_SECRET
              value: "YeG2U2zPQqnLoIfD3Bc3c55pfIUnDNFd"
            - name: SILVER_WAREHOUSE
              value: "silver"
            - name: KEYCLOAK_TOKEN_ENDPOINT
              value: "http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token"
            - name: AWS_ACCESS_KEY_ID
              value: "admin"
            - name: AWS_SECRET_ACCESS_KEY
              value: "admin123"
            {% for key, value in user_env_vars.items() %}
            - name: {{ key }}
              value: "{{ value }}"
            {% endfor %}

    deps: {}

    restartPolicy:
        type: Never
        onFailureRetries: 2
        onFailureRetryInterval: 10
        onSubmissionFailureRetries: 3
        onSubmissionFailureRetryInterval: 20

    timeToLiveSeconds: 3600
"""

# ==============================================================================
# DAG DEFINITION
# ==============================================================================

with DAG(
    dag_id="spark-job-template",
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 0,
    },
    description="Daily batch: aggregate crypto trades (OHLCV) from Bronze to Silver",
    start_date=datetime(2025, 12, 25),
    schedule="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    tags=["spark", "silver", "ohlcv", "crypto"],
    max_active_runs=1,
    params={
        "dry_run": Param(
            False, type="boolean", description="Skip K8s submission."
        ),
        # ---------- Job Identity ----------
        "job_name_prefix": Param(
            None,
            type=["string", "null"],
            description="[REQUIRED] Prefix for the SparkApplication name on K8s, e.g. 'transform-crypto-silver-batch'.",
        ),
        # ---------- Application ----------
        "image_repo": Param(
            None,
            type=["string", "null"],
            description="[REQUIRED] Docker image repository, e.g. 'myrepo/my-spark-job'.",
        ),
        "image_tag": Param(
            "latest",
            type="string",
            description="Docker image tag, e.g. 'v1.0'.",
        ),
        "main_file_path": Param(
            None,
            type=["string", "null"],
            description="[REQUIRED] Path to the main entrypoint file inside the container, e.g. 'local:///app/src/main.py'.",
        ),
        "main_class": Param(
            None,
            type=["string", "null"],
            description="Main class for Java/Scala jobs. Leave null for Python jobs.",
        ),
        # ---------- Resources ----------
        "driver_cores": Param(1, type="integer"),
        "driver_memory": Param("2g", type="string"),
        "executor_cores": Param(2, type="integer"),
        "executor_memory": Param("2g", type="string"),
        "executor_instances": Param(2, type="integer"),
        # ---------- Advanced Overrides ----------
        "app_arguments": Param(
            None,
            type=["array", "null"],
            description="List of arguments passed to the Spark job (overrides 'arguments' defined in the manifest).",
        ),
        "user_env_vars": Param(
            None,
            type=["object", "null"],
            description='Extra env vars injected into driver & executor, e.g. {"MY_KEY": "value"}.',
        ),
        "spark_conf": Param(
            None,
            type=["object", "null"],
            description='Spark config key-values to merge/override into the manifest sparkConf, e.g. {"spark.sql.shuffle.partitions": "400"}.',
        ),
    },
) as dag:

    # --------------------------------------------------------------------------
    # Task 1: Render Manifest
    # --------------------------------------------------------------------------
    @task
    def render_manifest(**context):
        params = context["params"]
        ts = context["ts_nodash"].lower()
        job_name = f"{params['job_name_prefix']}-{ts}"

        # Read and render the embedded YAML template
        template = jinja2.Template(SPARK_YAML_TEMPLATE)
        rendered = template.render(
            job_name=job_name,
            job_name_prefix=params["job_name_prefix"],
            image_repo=params["image_repo"],
            image_tag=params["image_tag"],
            main_file_path=params["main_file_path"],
            main_class=params.get("main_class"),
            driver_cores=params["driver_cores"],
            driver_memory=params["driver_memory"],
            executor_cores=params["executor_cores"],
            executor_memory=params["executor_memory"],
            executor_instances=params["executor_instances"],
            app_arguments=params.get("app_arguments") or [],
            user_env_vars=params.get("user_env_vars") or {},
        )

        manifest_dict = yaml.safe_load(rendered)

        # ------------------------------------------------------------------
        # Merge spark_conf param into spec.sparkConf of the manifest:
        #   - New key   → add
        #   - Existing  → override
        # ------------------------------------------------------------------
        extra_spark_conf = params.get("spark_conf") or {}
        if extra_spark_conf:
            existing_conf = manifest_dict.get("spec", {}).get("sparkConf", {})
            merged_conf = {**existing_conf, **extra_spark_conf}
            manifest_dict.setdefault("spec", {})["sparkConf"] = merged_conf
            logger.info(
                f"[spark_conf] Merged {len(extra_spark_conf)} override(s) into sparkConf."
            )

        # ------------------------------------------------------------------
        # Override arguments if app_arguments param is provided
        # ------------------------------------------------------------------
        app_arguments = params.get("app_arguments")
        if app_arguments is not None:
            manifest_dict.setdefault("spec", {})["arguments"] = app_arguments

        logger.info("\n" + "=" * 60)
        logger.info(f"[MANIFEST] job_name={job_name}")
        logger.info(json.dumps(manifest_dict, indent=2))
        logger.info("=" * 60)

        return manifest_dict

    # --------------------------------------------------------------------------
    # Task 2: Submit
    # --------------------------------------------------------------------------
    spark_manifest = render_manifest()

    submit_spark_job = DictSparkKubernetesOperator(
        task_id="submit_spark_job",
        kubernetes_conn_id="kubernetes_default",
        namespace=None,  # read from manifest metadata at execution time
        application_file=spark_manifest,
        dry_run="{{ params.dry_run }}",
        do_xcom_push=True,
    )

    # --------------------------------------------------------------------------
    # Task 3: Monitor (Async Sensor - runs on Triggerer, does not occupy a Worker)
    # --------------------------------------------------------------------------
    monitor_spark_job = SparkLifecycleSensor(
        task_id="monitor_spark_job",
        name=submit_spark_job.output["job_name"],
        namespace=submit_spark_job.output["namespace"],  # dynamic from XCom
        on_failure_callback=delete_spark_job_on_failure,
    )

    spark_manifest >> submit_spark_job >> monitor_spark_job
