"""
Airflow DAG for Crypto OHLCV Silver Batch Transformation

Simple ConfigMap-based approach (no template rendering).

This DAG:
1. Applies SparkApplication manifest from ConfigMap
2. Monitors job execution
3. Cleans up after completion

Prerequisites:
- ConfigMap 'spark-manifests' must exist with 'transform-crypto-silver-batch.yaml'
- Run: infra/k8s/compute/scripts/create_spark_manifests_configmap.sh
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from kubernetes.client import models as k8s

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 25),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="crypto-ohlcv-silver-batch",
    default_args=default_args,
    description="Daily batch job to aggregate crypto trades (OHLCV) from Bronze to Silver",
    schedule="0 2 * * *",  # Run daily at 2 AM
    catchup=False,
    tags=["data-transformation", "spark", "crypto", "silver", "ohlcv"],
    max_active_runs=1,
) as dag:

    # Task 1: Apply SparkApplication manifest from ConfigMap
    submit_spark_job = KubernetesPodOperator(
        task_id="submit_crypto_silver_batch_job",
        name="crypto-silver-submit",
        namespace="default",
        image="bitnamilegacy/kubectl:1.33.4-debian-12-r0",
        cmds=["kubectl"],
        arguments=[
            "apply",
            "-f",
            "/mnt/manifests/transform-crypto-silver-batch.yaml",
        ],
        volumes=[
            k8s.V1Volume(
                name="spark-manifests",
                config_map=k8s.V1ConfigMapVolumeSource(
                    name="spark-manifests",
                ),
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="spark-manifests",
                mount_path="/mnt/manifests",
                read_only=True,
            ),
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        service_account_name="openhouse-spark-operator-spark",
    )

    # Task 2: Monitor SparkApplication execution
    monitor_spark_job = SparkKubernetesSensor(
        task_id="monitor_crypto_silver_batch_job",
        namespace="default",
        application_name="transform-crypto-silver-batch",
        poke_interval=30,
        timeout=7200,  # 2 hours
        mode="poke",
        attach_log=True,
    )

    # Task 3: Cleanup - Delete SparkApplication after completion
    cleanup_spark_job = KubernetesPodOperator(
        task_id="cleanup_crypto_silver_batch_job",
        name="crypto-silver-cleanup",
        namespace="default",
        image="bitnamilegacy/kubectl:1.33.4-debian-12-r0",
        cmds=["kubectl"],
        arguments=[
            "delete",
            "sparkapplication",
            "transform-crypto-silver-batch",
            "-n",
            "default",
            "--ignore-not-found=true",
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        service_account_name="openhouse-spark-operator-spark",
        trigger_rule="all_done",  # Run even if previous tasks fail
    )

    # Define task dependencies
    submit_spark_job >> monitor_spark_job >> cleanup_spark_job
