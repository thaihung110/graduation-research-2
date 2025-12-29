import os
from typing import Any, Dict


def load_spark_config() -> Dict[str, Any]:
    """Load Spark configuration from environment variables."""
    spark_minor_version = os.getenv("SPARK_MINOR_VERSION", "3.5")
    iceberg_version = os.getenv("ICEBERG_VERSION", "1.5.2")

    catalog_config = {
        "url": os.getenv(
            "CATALOG_URL", "http://openhouse-lakekeeper:8181/catalog"
        ),
        "client_id": os.getenv("CLIENT_ID", "spark"),
        "client_secret": os.getenv(
            "CLIENT_SECRET", "3FfkvrupMYsojoT2RnXqknvjCsljwFWl"
        ),
        "warehouse": os.getenv("WAREHOUSE", "bronze"),
    }

    keycloak_endpoint = os.getenv(
        "KEYCLOAK_TOKEN_ENDPOINT",
        "http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token",
    )

    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "openhouse-kafka:9092"
    )

    conf = {
        "spark.jars.packages": (
            f"org.apache.iceberg:iceberg-spark-runtime-{spark_minor_version}_2.12:{iceberg_version},"
            f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version},"
            f"org.apache.spark:spark-sql-kafka-0-10_{spark_minor_version}:3.5.0"
        ),
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakekeeper.type": "rest",
        "spark.sql.catalog.lakekeeper.uri": catalog_config["url"],
        "spark.sql.catalog.lakekeeper.credential": f"{catalog_config['client_id']}:{catalog_config['client_secret']}",
        "spark.sql.catalog.lakekeeper.warehouse": catalog_config["warehouse"],
        "spark.sql.catalog.lakekeeper.scope": "lakekeeper",
        "spark.sql.catalog.lakekeeper.oauth2-server-uri": keycloak_endpoint,
        # Kafka consumer config
        "spark.sql.streaming.kafka.useDeprecatedOffsetFetching": "false",
        "spark.kafka.consumer.bootstrap.servers": kafka_bootstrap_servers,
        # Streaming config
        "spark.sql.streaming.schemaInference": "true",
    }

    return conf


def load_job_config() -> Dict[str, Any]:
    """Load job-specific configuration."""
    return {
        "kafka_topic": os.getenv(
            "KAFKA_TOPIC", "market-data.finnhub.crypto-trades.bronze"
        ),
        "database": os.getenv("DATABASE", "bronze"),
        "table": os.getenv("TABLE", "crypto_trades_raw"),
        "checkpoint_location": os.getenv(
            "CHECKPOINT_LOCATION", "/tmp/checkpoints/crypto-bronze"
        ),
        "trigger_interval": os.getenv("TRIGGER_INTERVAL", "10 seconds"),
        "kafka_bootstrap_servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "openhouse-kafka:9092"
        ),
        "max_offsets_per_trigger": int(
            os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000")
        ),
    }
