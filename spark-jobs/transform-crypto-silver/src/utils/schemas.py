from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_silver_schema() -> StructType:
    """Define Silver table schema."""
    return StructType(
        [
            StructField("trade_id", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("exchange", StringType(), False),
            StructField("base_currency", StringType(), False),
            StructField("quote_currency", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("volume", DoubleType(), False),
            StructField("trade_value", DoubleType(), False),
            StructField("trade_datetime", TimestampType(), False),
            StructField("trade_date", DateType(), False),
            StructField("timestamp_ms", LongType(), False),
            StructField("hour_of_day", IntegerType(), False),
            StructField("day_of_week", IntegerType(), False),
            StructField("is_buy", BooleanType(), True),
            StructField("is_sell", BooleanType(), True),
            StructField("is_maker", BooleanType(), True),
            StructField("is_taker", BooleanType(), True),
            StructField("trade_side", StringType(), True),
            StructField("conditions", ArrayType(StringType()), True),
            StructField("is_valid", BooleanType(), False),
            StructField("validation_errors", ArrayType(StringType()), True),
            StructField("data_quality_score", DoubleType(), False),
            StructField("source_system", StringType(), False),
            StructField("bronze_table", StringType(), False),
            StructField("bronze_ingestion_timestamp", TimestampType(), False),
            StructField("silver_ingestion_timestamp", TimestampType(), False),
            StructField("silver_ingestion_date", DateType(), False),
            StructField("transformation_version", StringType(), False),
        ]
    )


def get_create_table_sql(table_name: str) -> str:
    """Generate CREATE TABLE SQL for Silver table."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        trade_id STRING NOT NULL,
        symbol STRING NOT NULL,
        exchange STRING NOT NULL,
        base_currency STRING NOT NULL,
        quote_currency STRING NOT NULL,
        price DOUBLE NOT NULL,
        volume DOUBLE NOT NULL,
        trade_value DOUBLE NOT NULL,
        trade_datetime TIMESTAMP NOT NULL,
        trade_date DATE NOT NULL,
        timestamp_ms BIGINT NOT NULL,
        hour_of_day INT NOT NULL,
        day_of_week INT NOT NULL,
        is_buy BOOLEAN,
        is_sell BOOLEAN,
        is_maker BOOLEAN,
        is_taker BOOLEAN,
        trade_side STRING,
        conditions ARRAY<STRING>,
        is_valid BOOLEAN NOT NULL,
        validation_errors ARRAY<STRING>,
        data_quality_score DOUBLE NOT NULL,
        source_system STRING NOT NULL,
        bronze_table STRING NOT NULL,
        bronze_ingestion_timestamp TIMESTAMP NOT NULL,
        silver_ingestion_timestamp TIMESTAMP NOT NULL,
        silver_ingestion_date DATE NOT NULL,
        transformation_version STRING NOT NULL
    )
    USING iceberg
    PARTITIONED BY (trade_date, exchange)
    TBLPROPERTIES (
        'format-version'='2',
        'write.target-file-size-bytes'='134217728',
        'write.parquet.compression-codec'='zstd',
        'write.parquet.compression-level'='6',
        -- Snapshot management properties
        'write.snapshot-id-inheritance.enabled'='true',
        'write.metadata.delete-after-commit.enabled'='true',
        'write.metadata.previous-versions-max'='10',
        'history.expire.max-snapshot-age-ms'='604800000',
        'history.expire.min-snapshots-to-keep'='5'
    )
    """

