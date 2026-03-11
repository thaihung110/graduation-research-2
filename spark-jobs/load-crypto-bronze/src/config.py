import os
from typing import Any, Dict


def _load_catalog_and_oauth_env() -> Dict[str, Any]:
    """
    Load shared catalog and OAuth-related environment variables.

    This helper is used both for Spark configuration and for debugging
    OAuth tokens (so we don't duplicate env parsing logic).
    """
    catalog_config = {
        "url": os.getenv(
            "CATALOG_URL", "http://openhouse-gravitino:9001/iceberg"
        ),
        "client_id": os.getenv("CLIENT_ID", "spark"),
        "client_secret": os.getenv(
            "CLIENT_SECRET", "YeG2U2zPQqnLoIfD3Bc3c55pfIUnDNFd"
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

    return {
        "catalog_config": catalog_config,
        "keycloak_endpoint": keycloak_endpoint,
        "kafka_bootstrap_servers": kafka_bootstrap_servers,
    }


def load_spark_config() -> Dict[str, Any]:
    """Load Spark configuration from environment variables."""
    # Currently unused but kept for future version-specific config needs
    _ = os.getenv("SPARK_MINOR_VERSION", "3.5")
    _ = os.getenv("ICEBERG_VERSION", "1.10.1")

    shared = _load_catalog_and_oauth_env()
    catalog_config = shared["catalog_config"]
    keycloak_endpoint = shared["keycloak_endpoint"]
    kafka_bootstrap_servers = shared["kafka_bootstrap_servers"]

    conf = {
        # Packages are now baked into the Docker image, so we don't declare spark.jars.packages here
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakekeeper.type": "rest",
        "spark.sql.catalog.lakekeeper.uri": catalog_config["url"],
        "spark.sql.catalog.lakekeeper.credential": f"{catalog_config['client_id']}:{catalog_config['client_secret']}",
        "spark.sql.catalog.lakekeeper.warehouse": catalog_config["warehouse"],
        "spark.sql.catalog.lakekeeper.scope": "gravitino",
        "spark.sql.catalog.lakekeeper.oauth2-server-uri": keycloak_endpoint,
        # Kafka consumer config
        "spark.sql.streaming.kafka.useDeprecatedOffsetFetching": "false",
        "spark.kafka.consumer.bootstrap.servers": kafka_bootstrap_servers,
        # Streaming config
        "spark.sql.streaming.schemaInference": "true",
        "spark.sql.catalog.lakekeeper.token-exchange-enabled": "false",
        # ── [1] Bật Credential Vending ─────────────────────────────────────
        # Yêu cầu Gravitino REST vend S3 credentials thay vì dùng credentials tĩnh.
        # Xác nhận bởi docs: "set spark.sql.catalog.rest.header.X-Iceberg-Access-Delegation
        #                      = vended-credentials in the client side"
        # (Bước 3 — gravitino-credential-vending-minio.md)
        "spark.sql.catalog.lakekeeper.header.X-Iceberg-Access-Delegation": "vended-credentials",
        # ── [2] MinIO FileIO configuration (PHẢI set thủ công) ─────────────
        # Credential vending chỉ trả về access key + secret key.
        # Endpoint và path-style-access KHÔNG tự động chuyển từ server sang client.
        # Iceberg S3FileIO (không phải Hadoop S3A) cần các config này để kết nối MinIO.
        # (Bước 3 — gravitino-credential-vending-minio.md)
        "spark.sql.catalog.lakekeeper.s3.endpoint": "http://openhouse-minio:9000",
        "spark.sql.catalog.lakekeeper.s3.path-style-access": "true",
    }

    return conf


def load_oauth_debug_config() -> Dict[str, str]:
    """
    Load minimal OAuth configuration needed to fetch an access token
    from Keycloak for debugging purposes.
    """
    shared = _load_catalog_and_oauth_env()
    catalog_config = shared["catalog_config"]
    keycloak_endpoint = shared["keycloak_endpoint"]

    return {
        "token_endpoint": keycloak_endpoint,
        "client_id": catalog_config["client_id"],
        "client_secret": catalog_config["client_secret"],
        # Scope used by Spark / Iceberg REST to talk to Gravitino
        "scope": "gravitino",
    }


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
