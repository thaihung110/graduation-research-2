from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def get_avro_schema() -> StructType:
    """Define Avro schema structure for Finnhub crypto trade messages."""
    return StructType(
        [
            StructField("message_type", StringType(), nullable=False),
            StructField(
                "trades",
                ArrayType(
                    StructType(
                        [
                            StructField("symbol", StringType(), nullable=False),
                            StructField(
                                "exchange", StringType(), nullable=False
                            ),
                            StructField(
                                "base_currency", StringType(), nullable=False
                            ),
                            StructField(
                                "quote_currency", StringType(), nullable=False
                            ),
                            StructField("price", DoubleType(), nullable=False),
                            StructField("volume", DoubleType(), nullable=False),
                            StructField(
                                "timestamp_ms", LongType(), nullable=False
                            ),
                            StructField(
                                "conditions",
                                ArrayType(StringType(), containsNull=False),
                                nullable=True,
                            ),
                            StructField(
                                "ingestion_timestamp_ms",
                                LongType(),
                                nullable=False,
                            ),
                        ]
                    ),
                    containsNull=False,
                ),
                nullable=False,
            ),
            StructField(
                "producer_metadata",
                StructType(
                    [
                        StructField(
                            "producer_id", StringType(), nullable=False
                        ),
                        StructField(
                            "kafka_topic", StringType(), nullable=False
                        ),
                        StructField(
                            "schema_version", StringType(), nullable=False
                        ),
                    ]
                ),
                nullable=False,
            ),
        ]
    )


def get_avro_schema_json() -> str:
    """Get Avro schema JSON string."""
    return """
    {
        "type": "record",
        "name": "CryptoTradeMessage",
        "namespace": "com.finnhub.crypto",
        "fields": [
        {
            "name": "message_type",
            "type": "string"
        },
        {
            "name": "trades",
            "type": {
            "type": "array",
            "items": {
                "type": "record",
                "name": "Trade",
                "fields": [
                {"name": "symbol", "type": "string"},
                {"name": "exchange", "type": "string"},
                {"name": "base_currency", "type": "string"},
                {"name": "quote_currency", "type": "string"},
                {"name": "price", "type": "double"},
                {"name": "volume", "type": "double"},
                {"name": "timestamp_ms", "type": "long"},
                {
                    "name": "conditions",
                    "type": ["null", {"type": "array", "items": "string"}],
                    "default": null
                },
                {"name": "ingestion_timestamp_ms", "type": "long"}
                ]
            }
            }
        },
        {
            "name": "producer_metadata",
            "type": {
            "type": "record",
            "name": "ProducerMetadata",
            "fields": [
                {"name": "producer_id", "type": "string"},
                {"name": "kafka_topic", "type": "string"},
                {"name": "schema_version", "type": "string"}
            ]
            }
        }
        ]
    }
    """


def get_create_table_sql(database: str, table: str) -> str:
    """Generate CREATE TABLE SQL for Bronze table."""
    return f"""
    CREATE TABLE IF NOT EXISTS lakekeeper.{database}.{table} (
        symbol STRING,
        exchange STRING,
        base_currency STRING,
        quote_currency STRING,
        price DOUBLE,
        volume DOUBLE,
        trade_datetime TIMESTAMP,
        trade_date DATE,
        timestamp_ms BIGINT,
        conditions ARRAY<STRING>,
        ingestion_timestamp_ms BIGINT,
        message_type STRING,
        producer_id STRING,
        kafka_topic STRING,
        schema_version STRING,
        kafka_partition INT,
        kafka_offset BIGINT,
        bronze_ingestion_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (trade_date, exchange)
    TBLPROPERTIES (
        'format-version'='2',
        -- Snapshot management properties
        'write.snapshot-id-inheritance.enabled'='true',
        'write.metadata.delete-after-commit.enabled'='true',
        'write.metadata.previous-versions-max'='10',
        'history.expire.max-snapshot-age-ms'='604800000',
        'history.expire.min-snapshots-to-keep'='5'
    )
    """
