"""
Configuration loader for Streaming Test Job.

Reads all runtime settings from environment variables.
"""

import os
from typing import Any, Dict


def load_spark_config() -> Dict[str, Any]:
    """Load Spark / Iceberg / Kafka configuration from environment variables."""
    catalog_name = os.getenv("CATALOG_NAME", "lakekeeper")
    catalog_url = os.getenv(
        "CATALOG_URL", "http://openhouse-gravitino:9001/iceberg"
    )
    client_id = os.getenv("CLIENT_ID", "spark")
    client_secret = os.getenv(
        "CLIENT_SECRET", "YeG2U2zPQqnLoIfD3Bc3c55pfIUnDNFd"
    )
    warehouse = os.getenv("WAREHOUSE", "bronze")
    keycloak_endpoint = os.getenv(
        "KEYCLOAK_TOKEN_ENDPOINT",
        "http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token",
    )
    catalog_scope = os.getenv("CATALOG_SCOPE", "email")
    catalog_s3_endpoint = os.getenv(
        "CATALOG_S3_ENDPOINT", "http://storage-minio:9000"
    )
    # Whether the streaming engine should execute micro-batches even when there
    # is no new data (noData micro-batches). In Spark, the default is **true**:
    #   spark.sql.streaming.noDataMicroBatches.enabled = true
    # true  → engine still runs triggers with empty input, mainly to advance
    #          state/watermarks in stateful queries.
    # false → engine skips batches when there is no data; only runs when new data
    #          arrives. This is more resource-efficient but worse for eager state
    #          management and for our "keep some activity" goal.
    no_data_micro_batches_enabled = os.getenv(
        "SPARK_NO_DATA_MICRO_BATCHES_ENABLED", "true"
    )

    return {
        # ── Iceberg extensions ──────────────────────────────────────────────
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        # ── Lakekeeper (Iceberg REST) catalog ───────────────────────────────
        f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog_name}.type": "rest",
        f"spark.sql.catalog.{catalog_name}.uri": catalog_url,
        f"spark.sql.catalog.{catalog_name}.credential": f"{client_id}:{client_secret}",
        f"spark.sql.catalog.{catalog_name}.warehouse": warehouse,
        f"spark.sql.catalog.{catalog_name}.scope": catalog_scope,
        f"spark.sql.catalog.{catalog_name}.oauth2-server-uri": keycloak_endpoint,
        f"spark.sql.catalog.{catalog_name}.token-exchange-enabled": "false",
        # ── Credential Vending (Gravitino → Iceberg S3FileIO) ───────────────
        f"spark.sql.catalog.{catalog_name}.header.X-Iceberg-Access-Delegation": "vended-credentials",
        f"spark.sql.catalog.{catalog_name}.s3.endpoint": catalog_s3_endpoint,
        f"spark.sql.catalog.{catalog_name}.s3.path-style-access": "true",
        # ── Kafka consumer ──────────────────────────────────────────────────
        "spark.sql.streaming.kafka.useDeprecatedOffsetFetching": "false",
        # Run empty micro-batches to keep progress/events flowing when idle.
        "spark.sql.streaming.noDataMicroBatches.enabled": no_data_micro_batches_enabled,
    }


def load_job_config() -> Dict[str, Any]:
    """Load job-specific configuration from environment variables."""
    return {
        "kafka_topic": os.getenv("KAFKA_TOPIC", "streaming-test"),
        "kafka_bootstrap_servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "openhouse-kafka:9092"
        ),
        "kafka_sasl_username": os.getenv("KAFKA_SASL_USERNAME"),
        "kafka_sasl_password": os.getenv("KAFKA_SASL_PASSWORD"),
        "kafka_security_protocol": os.getenv(
            "KAFKA_SECURITY_PROTOCOL", "SASL_SSL"
        ),
        "kafka_sasl_mechanism": os.getenv(
            "KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"
        ),
        "kafka_ssl_truststore_location": os.getenv(
            "KAFKA_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/certs/ca/ca.crt"
        ),
        "kafka_starting_offsets": os.getenv("KAFKA_STARTING_OFFSETS", "latest"),
        "kafka_group_id": os.getenv("KAFKA_GROUP_ID"),
        "catalog": os.getenv("CATALOG_NAME", "lakekeeper"),
        "schema": os.getenv("SCHEMA", "bronze"),
        "table": os.getenv("TABLE", "streaming_test_raw"),
        "checkpoint_location": os.getenv(
            "CHECKPOINT_LOCATION", "s3a://bronze/checkpoints/streaming-test"
        ),
        "trigger_interval": os.getenv("TRIGGER_INTERVAL", "30 seconds"),
        "max_offsets_per_trigger": int(
            os.getenv("MAX_OFFSETS_PER_TRIGGER", "1000")
        ),
    }
