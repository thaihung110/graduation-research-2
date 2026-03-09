"""
DAG: spark-streaming-job-template

Generic DAG for submitting Spark Structured Streaming jobs to Kubernetes via Spark Operator.
This is a "fire-and-forget" DAG: it does not monitor the job after submission.
"""

import json
import logging
from datetime import datetime

import jinja2
import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

logger = logging.getLogger("airflow.task")


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
                logger.info(f"[DRY RUN] Would submit {name}")
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
# SPARK YAML MANIFEST TEMPLATE
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
        type: streaming
spec:
    type: Python
    mode: cluster
    pythonVersion: "3"
    sparkVersion: "3.5.0"

    image: "{{ image_repo }}:{{ image_tag }}"
    imagePullPolicy: Always

    mainApplicationFile: "{{ main_file_path }}"

    sparkConf:
        "spark.eventLog.enabled": "true"
        "spark.eventLog.dir": "s3a://spark-logs/event-logs"
        "spark.eventLog.compress": "true"

    hadoopConf:
        "fs.s3a.endpoint": "http://openhouse-minio:9000"
        "fs.s3a.path.style.access": "true"
        "fs.s3a.connection.ssl.enabled": "false"
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        "fs.s3a.metadatastore.impl": "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore"
        "fs.s3a.bucket.bronze.endpoint": "http://openhouse-minio:9000"
        "fs.s3a.bucket.silver.endpoint": "http://openhouse-minio:9000"
        "fs.s3a.bucket.spark-logs.endpoint":   "http://openhouse-minio-log:9000"

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
            {% for key, value in user_env_vars.items() %}
            - name: {{ key }}
              value: "{{ value }}"
            {% endfor %}

    deps: {}

    restartPolicy:
        type: Always

    timeToLiveSeconds: 3600
"""

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id="spark-streaming-job-template",
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 0,
    },
    description="Fire-and-forget streaming DAG (No monitoring/sensor)",
    start_date=datetime(2025, 12, 25),
    schedule=None,  # Streaming jobs runs indefinitely, should trigger manually
    catchup=False,
    tags=["spark", "streaming"],
    max_active_runs=1,
    params={
        "dry_run": Param(
            False, type="boolean", description="Skip K8s submission."
        ),
        "job_name_prefix": Param(None, type=["string", "null"]),
        "image_repo": Param(None, type=["string", "null"]),
        "image_tag": Param("latest", type="string"),
        "main_file_path": Param(None, type=["string", "null"]),
        "driver_cores": Param(1, type="integer"),
        "driver_memory": Param("2g", type="string"),
        "executor_cores": Param(2, type="integer"),
        "executor_memory": Param("2g", type="string"),
        "executor_instances": Param(2, type="integer"),
        "app_arguments": Param(None, type=["array", "null"]),
        "user_env_vars": Param(None, type=["object", "null"]),
        "spark_conf": Param(None, type=["object", "null"]),
        "hadoop_conf": Param(None, type=["object", "null"]),
    },
) as dag:

    @task
    def render_manifest(**context):
        params = context["params"]
        ts = context["ts_nodash"].lower()
        job_name = f"{params['job_name_prefix']}-{ts}"

        template = jinja2.Template(SPARK_YAML_TEMPLATE)
        rendered = template.render(
            job_name=job_name,
            job_name_prefix=params["job_name_prefix"],
            image_repo=params["image_repo"],
            image_tag=params["image_tag"],
            main_file_path=params["main_file_path"],
            driver_cores=params["driver_cores"],
            driver_memory=params["driver_memory"],
            executor_cores=params["executor_cores"],
            executor_memory=params["executor_memory"],
            executor_instances=params["executor_instances"],
            app_arguments=params.get("app_arguments") or [],
            user_env_vars=params.get("user_env_vars") or {},
        )

        manifest_dict = yaml.safe_load(rendered)
        # params.spark_conf override các giá trị trong spec.sparkConf của manifest
        extra_spark_conf = params.get("spark_conf") or {}
        if extra_spark_conf:
            existing_conf = manifest_dict.get("spec", {}).get("sparkConf", {})
            merged_conf = {**existing_conf, **extra_spark_conf}
            manifest_dict.setdefault("spec", {})["sparkConf"] = merged_conf

        # params.hadoop_conf override các giá trị trong spec.hadoopConf của manifest
        extra_hadoop_conf = params.get("hadoop_conf") or {}
        if extra_hadoop_conf:
            existing_hadoop = manifest_dict.get("spec", {}).get(
                "hadoopConf", {}
            )
            merged_hadoop = {**existing_hadoop, **extra_hadoop_conf}
            manifest_dict.setdefault("spec", {})["hadoopConf"] = merged_hadoop

        app_arguments = params.get("app_arguments")
        if app_arguments is not None:
            manifest_dict.setdefault("spec", {})["arguments"] = app_arguments

        logger.info(f"[MANIFEST] job_name={job_name}")
        return manifest_dict

    spark_manifest = render_manifest()

    submit_spark_job = DictSparkKubernetesOperator(
        task_id="submit_spark_job",
        kubernetes_conn_id="kubernetes_default",
        namespace=None,
        application_file=spark_manifest,
        dry_run="{{ params.dry_run }}",
        do_xcom_push=True,
    )

    spark_manifest >> submit_spark_job
