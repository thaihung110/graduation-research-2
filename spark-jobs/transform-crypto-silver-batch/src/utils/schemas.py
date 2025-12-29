"""Schema definitions for Silver layer tables."""


def get_create_table_sql(database: str, table: str) -> str:
    """Generate CREATE TABLE SQL for Silver OHLCV 1-hour aggregation table."""
    return f"""
    CREATE TABLE IF NOT EXISTS silver.{database}.{table} (
        symbol STRING,
        exchange STRING,
        base_currency STRING,
        quote_currency STRING,
        hour_start TIMESTAMP,
        hour_end TIMESTAMP,
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        close_price DOUBLE,
        total_volume DOUBLE,
        trade_count BIGINT,
        avg_price DOUBLE,
        vwap DOUBLE,
        price_change DOUBLE,
        price_change_pct DOUBLE,
        agg_date DATE,
        silver_ingestion_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (agg_date, exchange)
    TBLPROPERTIES (
        'format-version'='2',
        'write.target-file-size-bytes'='134217728',
        'write.parquet.compression-codec'='zstd',
        'write.parquet.compression-level'='6',
        'write.metadata.delete-after-commit.enabled'='true',
        'write.metadata.previous-versions-max'='10',
        'history.expire.max-snapshot-age-ms'='604800000',
        'history.expire.min-snapshots-to-keep'='5'
    )
    """
