import os
from typing import Any, Dict


def load_spark_config() -> Dict[str, Any]:
    """Load Spark configuration from environment variables."""
    spark_minor_version = os.getenv("SPARK_MINOR_VERSION", "3.5")
    iceberg_version = os.getenv("ICEBERG_VERSION", "1.5.2")

    # Bronze catalog
    bronze_config = {
        "url": os.getenv(
            "BRONZE_CATALOG_URL", "http://openhouse-lakekeeper:8181/catalog"
        ),
        "client_id": os.getenv("BRONZE_CLIENT_ID", "spark"),
        "client_secret": os.getenv("BRONZE_CLIENT_SECRET", ""),
        "warehouse": os.getenv("BRONZE_WAREHOUSE", "bronze"),
    }

    # Silver catalog
    silver_config = {
        "url": os.getenv(
            "SILVER_CATALOG_URL", "http://openhouse-lakekeeper:8181/catalog"
        ),
        "client_id": os.getenv("SILVER_CLIENT_ID", "spark"),
        "client_secret": os.getenv("SILVER_CLIENT_SECRET", ""),
        "warehouse": os.getenv("SILVER_WAREHOUSE", "silver"),
    }

    keycloak_endpoint = os.getenv(
        "KEYCLOAK_TOKEN_ENDPOINT",
        "http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token",
    )

    conf = {
        "spark.jars.packages": (
            f"org.apache.iceberg:iceberg-spark-runtime-{spark_minor_version}_2.12:{iceberg_version},"
            f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version},"
            f"org.apache.hadoop:hadoop-aws:3.3.4"
        ),
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.streaming.schemaInference": "true",
        # Bronze catalog
        "spark.sql.catalog.bronze": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.bronze.type": "rest",
        "spark.sql.catalog.bronze.uri": bronze_config["url"],
        "spark.sql.catalog.bronze.credential": f"{bronze_config['client_id']}:{bronze_config['client_secret']}",
        "spark.sql.catalog.bronze.warehouse": bronze_config["warehouse"],
        "spark.sql.catalog.bronze.scope": "lakekeeper",
        "spark.sql.catalog.bronze.oauth2-server-uri": keycloak_endpoint,
        # Silver catalog
        "spark.sql.catalog.silver": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.silver.type": "rest",
        "spark.sql.catalog.silver.uri": silver_config["url"],
        "spark.sql.catalog.silver.credential": f"{silver_config['client_id']}:{silver_config['client_secret']}",
        "spark.sql.catalog.silver.warehouse": silver_config["warehouse"],
        "spark.sql.catalog.silver.scope": "lakekeeper",
        "spark.sql.catalog.silver.oauth2-server-uri": keycloak_endpoint,
    }

    return conf


def load_job_config() -> Dict[str, Any]:
    """Load job-specific configuration."""
    return {
        "bronze_table": os.getenv(
            "BRONZE_TABLE", "bronze.bronze.crypto_trades_raw"
        ),
        "silver_table": os.getenv(
            "SILVER_TABLE", "silver.silver.crypto_trades"
        ),
        "checkpoint_location": os.getenv(
            "CHECKPOINT_LOCATION", "/tmp/checkpoints/crypto-silver"
        ),
        "trigger_interval": os.getenv("TRIGGER_INTERVAL", "5 seconds"),
        "bronze_buffer_seconds": int(os.getenv("BRONZE_BUFFER_SECONDS", "10")),
        "filter_invalid_records": os.getenv(
            "FILTER_INVALID_RECORDS", "false"
        ).lower()
        == "true",
    }
