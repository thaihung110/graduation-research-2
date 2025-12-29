"""
PySpark application to transform crypto trades from Bronze to Silver layer (STREAMING).

This job:
1. Continuously reads new data from Iceberg table bronze.crypto_trades_raw (STREAMING)
2. Validates and enriches the data
3. Applies data quality checks
4. Transforms to Silver schema
5. Continuously loads data into Iceberg table silver.crypto_trades

This is a STREAMING job that runs continuously and processes new records as they arrive in Bronze.
"""

import hashlib
import os
import sys
from datetime import datetime, timedelta

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    array_contains,
    col,
    concat_ws,
    current_timestamp,
    dayofweek,
    expr,
    from_unixtime,
    hour,
    lit,
    md5,
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import (
    size,
    struct,
    to_date,
    unix_timestamp,
    upper,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def get_spark_session():
    """Create and configure Spark session with Iceberg and dual catalog (bronze + silver)."""

    # Get configuration from environment variables
    spark_minor_version = os.getenv("SPARK_MINOR_VERSION", "3.5")
    iceberg_version = os.getenv("ICEBERG_VERSION", "1.5.2")

    # Bronze catalog configuration
    bronze_catalog_url = os.getenv(
        "BRONZE_CATALOG_URL", "http://openhouse-lakekeeper:8181/catalog"
    )
    bronze_client_id = os.getenv("BRONZE_CLIENT_ID", "spark")
    bronze_client_secret = os.getenv(
        "BRONZE_CLIENT_SECRET", "7PwnKrR0cRhh9PKaQRPRx2KGQBgmOUxa"
    )
    bronze_warehouse = os.getenv("BRONZE_WAREHOUSE", "bronze")

    # Silver catalog configuration
    silver_catalog_url = os.getenv(
        "SILVER_CATALOG_URL", "http://openhouse-lakekeeper:8181/catalog"
    )
    silver_client_id = os.getenv("SILVER_CLIENT_ID", "spark")
    silver_client_secret = os.getenv(
        "SILVER_CLIENT_SECRET", "7PwnKrR0cRhh9PKaQRPRx2KGQBgmOUxa"
    )
    silver_warehouse = os.getenv("SILVER_WAREHOUSE", "silver")

    # Keycloak token endpoint (shared)
    keycloak_token_endpoint = os.getenv(
        "KEYCLOAK_TOKEN_ENDPOINT",
        "http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token",
    )

    # Spark configuration for Iceberg with dual catalogs
    conf = {
        "spark.jars.packages": f"org.apache.iceberg:iceberg-spark-runtime-{spark_minor_version}_2.12:{iceberg_version},org.apache.iceberg:iceberg-aws-bundle:{iceberg_version},org.apache.hadoop:hadoop-aws:3.3.4",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        # Streaming config
        "spark.sql.streaming.schemaInference": "true",
        # Bronze catalog configuration
        "spark.sql.catalog.bronze": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.bronze.type": "rest",
        "spark.sql.catalog.bronze.uri": bronze_catalog_url,
        "spark.sql.catalog.bronze.credential": f"{bronze_client_id}:{bronze_client_secret}",
        "spark.sql.catalog.bronze.warehouse": bronze_warehouse,
        "spark.sql.catalog.bronze.scope": "lakekeeper",
        "spark.sql.catalog.bronze.oauth2-server-uri": keycloak_token_endpoint,
        # Silver catalog configuration
        "spark.sql.catalog.silver": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.silver.type": "rest",
        "spark.sql.catalog.silver.uri": silver_catalog_url,
        "spark.sql.catalog.silver.credential": f"{silver_client_id}:{silver_client_secret}",
        "spark.sql.catalog.silver.warehouse": silver_warehouse,
        "spark.sql.catalog.silver.scope": "lakekeeper",
        "spark.sql.catalog.silver.oauth2-server-uri": keycloak_token_endpoint,
    }

    # Create Spark session via SparkConf
    spark_conf = SparkConf().setAppName(
        "Crypto Trades Bronze to Silver Transformation"
    )
    for key, value in conf.items():
        spark_conf = spark_conf.set(key, value)
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    return spark


def validate_trade(
    price, volume, timestamp_ms, symbol, base_currency, quote_currency
):
    """Validate trade record and return validation errors."""
    errors = []

    # Price validation
    if price is None or price <= 0:
        errors.append("price_invalid")
    elif price < 0.00000001 or price > 1e15:
        errors.append("price_out_of_range")

    # Volume validation
    if volume is None or volume <= 0:
        errors.append("volume_invalid")
    elif volume < 0.00000001 or volume > 1e15:
        errors.append("volume_out_of_range")

    # Timestamp validation
    if timestamp_ms is None or timestamp_ms <= 0:
        errors.append("timestamp_invalid")
    else:
        # Check if timestamp is reasonable (not too far in past/future)
        current_ms = int(datetime.now().timestamp() * 1000)
        if timestamp_ms < (current_ms - 31536000000):  # More than 1 year ago
            errors.append("timestamp_too_old")
        elif timestamp_ms > (
            current_ms + 86400000
        ):  # More than 1 day in future
            errors.append("timestamp_too_future")

    # Symbol validation
    if not symbol or (isinstance(symbol, str) and symbol.strip() == ""):
        errors.append("symbol_empty")
    elif isinstance(symbol, str) and ":" not in symbol:
        errors.append("symbol_invalid_format")

    # Currency validation
    if not base_currency or (
        isinstance(base_currency, str) and base_currency.strip() == ""
    ):
        errors.append("base_currency_empty")
    if not quote_currency or (
        isinstance(quote_currency, str) and quote_currency.strip() == ""
    ):
        errors.append("quote_currency_empty")
    if base_currency == quote_currency:
        errors.append("currencies_same")

    return errors


def calculate_quality_score(validation_errors):
    """Calculate data quality score (0.0-1.0)."""
    if not validation_errors or len(validation_errors) == 0:
        return 1.0

    # Each validation rule is worth 0.2 points
    max_score = 1.0
    error_count = len(validation_errors)

    # Deduct 0.2 for each error, minimum 0.0
    score = max(0.0, max_score - (error_count * 0.2))
    return round(score, 2)


def parse_trade_conditions(conditions):
    """Parse trade conditions array to extract flags."""
    if conditions is None:
        return {
            "is_buy": None,
            "is_sell": None,
            "is_maker": None,
            "is_taker": None,
            "trade_side": None,
        }

    conditions_lower = [c.lower() if c else "" for c in conditions]

    is_buy = any("buy" in c for c in conditions_lower)
    is_sell = any("sell" in c for c in conditions_lower)
    is_maker = any("maker" in c for c in conditions_lower)
    is_taker = any("taker" in c for c in conditions_lower)

    trade_side = None
    if is_buy:
        trade_side = "BUY"
    elif is_sell:
        trade_side = "SELL"

    return {
        "is_buy": is_buy if (is_buy or is_sell) else None,
        "is_sell": is_sell if (is_buy or is_sell) else None,
        "is_maker": is_maker if (is_maker or is_taker) else None,
        "is_taker": is_taker if (is_maker or is_taker) else None,
        "trade_side": trade_side,
    }


def generate_trade_id(exchange, symbol, timestamp_ms, offset=None):
    """Generate unique trade ID."""
    # Create hash from exchange, symbol, timestamp, and optional offset
    hash_input = f"{exchange}_{symbol}_{timestamp_ms}"
    if offset is not None:
        hash_input += f"_{offset}"

    hash_value = hashlib.md5(hash_input.encode()).hexdigest()[:8]
    return f"{exchange}_{symbol}_{timestamp_ms}_{hash_value}"


def transform_batch(
    batch_df, epoch_id, spark, silver_table, bronze_table, last_timestamp_ref
):
    """Transform each micro-batch from Bronze to Silver."""
    from pyspark.sql.functions import max as spark_max
    from pyspark.sql.functions import udf
    from pyspark.sql.types import ArrayType as SparkArrayType

    print(f"\n🔄 Processing batch {epoch_id}...")

    # Read new records from Bronze table based on timestamp
    # Get the last processed timestamp
    try:
        result = spark.sql(
            f"SELECT MAX(bronze_ingestion_timestamp) as max_ts FROM {silver_table}"
        ).collect()
        current_last_ts = (
            result[0]["max_ts"] if result and result[0]["max_ts"] else None
        )
        print(f"   📅 Last processed timestamp from Silver: {current_last_ts}")
    except Exception as e:
        current_last_ts = last_timestamp_ref[0] if last_timestamp_ref else None
        print(
            f"   ⚠️  Could not get last timestamp from Silver, using cached: {current_last_ts}"
        )

    # Read from Bronze table with timestamp filter
    bronze_df = spark.table(bronze_table)

    # Debug: Check total records in Bronze table
    total_bronze_count = bronze_df.count()
    print(f"   📊 Total records in Bronze table: {total_bronze_count}")

    if current_last_ts:
        # Apply buffer time to ensure Bronze has committed
        # Subtract buffer seconds from timestamp to account for processing delay
        buffer_seconds = int(os.getenv("BRONZE_BUFFER_SECONDS", "10"))

        # Convert timestamp to unix timestamp, subtract buffer_seconds, then convert back
        # This ensures we read records that were committed at least buffer_seconds ago
        buffer_timestamp_expr = from_unixtime(
            unix_timestamp(lit(current_last_ts)) - buffer_seconds
        )

        bronze_df = bronze_df.filter(
            col("bronze_ingestion_timestamp") > buffer_timestamp_expr
        )
        print(
            f"   📅 Filtering records after: {current_last_ts} (with {buffer_seconds}s buffer)"
        )

        # Debug: Check min/max timestamps in Bronze
        try:
            min_ts = bronze_df.agg(
                {"bronze_ingestion_timestamp": "min"}
            ).collect()[0][0]
            max_ts = bronze_df.agg(
                {"bronze_ingestion_timestamp": "max"}
            ).collect()[0][0]
            print(
                f"   📊 Bronze timestamp range (after filter): min={min_ts}, max={max_ts}"
            )
        except Exception:
            pass
    else:
        # First run: read all records
        print(f"   📅 First run: reading all records from Bronze table")

    batch_count = bronze_df.count()
    if batch_count == 0:
        print(f"   ⚠️  No new records in batch {epoch_id}")
        # Update last timestamp reference
        if current_last_ts:
            last_timestamp_ref[0] = current_last_ts
        return

    print(f"   📊 Processing {batch_count} new records in batch {epoch_id}")

    # Update last timestamp for next batch
    if batch_count > 0:
        max_ts = bronze_df.agg(
            spark_max(col("bronze_ingestion_timestamp"))
        ).collect()[0][0]
        last_timestamp_ref[0] = max_ts

    # Apply transformations
    # NOTE: Use bronze_df (from Bronze table), NOT batch_df (from rate source)
    silver_df = bronze_df.select(
        # Generate trade_id
        concat_ws(
            "_",
            upper(col("exchange")),
            col("symbol"),
            col("timestamp_ms"),
            md5(
                concat_ws(
                    "_",
                    col("exchange"),
                    col("symbol"),
                    col("timestamp_ms"),
                    col("kafka_offset"),
                )
            ).substr(lit(1), lit(8)),
        ).alias("trade_id"),
        # Primary fields (validated and standardized)
        col("symbol"),
        upper(col("exchange")).alias("exchange"),
        upper(col("base_currency")).alias("base_currency"),
        upper(col("quote_currency")).alias("quote_currency"),
        # Trade data (rounded to 8 decimal places)
        spark_round(col("price"), 8).alias("price"),
        spark_round(col("volume"), 8).alias("volume"),
        spark_round(col("price") * col("volume"), 8).alias("trade_value"),
        # Timestamps
        col("trade_datetime"),
        col("trade_date"),
        col("timestamp_ms"),
        hour(col("trade_datetime")).alias("hour_of_day"),
        dayofweek(col("trade_datetime")).alias("day_of_week"),
        # Trade conditions (parse from conditions array)
        when(
            array_contains(col("conditions"), lit("buy"))
            | array_contains(col("conditions"), lit("BUY")),
            lit(True),
        )
        .when(
            array_contains(col("conditions"), lit("sell"))
            | array_contains(col("conditions"), lit("SELL")),
            lit(False),
        )
        .otherwise(None)
        .alias("is_buy"),
        when(
            array_contains(col("conditions"), lit("sell"))
            | array_contains(col("conditions"), lit("SELL")),
            lit(True),
        )
        .when(
            array_contains(col("conditions"), lit("buy"))
            | array_contains(col("conditions"), lit("BUY")),
            lit(False),
        )
        .otherwise(None)
        .alias("is_sell"),
        when(
            array_contains(col("conditions"), lit("maker"))
            | array_contains(col("conditions"), lit("MAKER")),
            lit(True),
        )
        .otherwise(None)
        .alias("is_maker"),
        when(
            array_contains(col("conditions"), lit("taker"))
            | array_contains(col("conditions"), lit("TAKER")),
            lit(True),
        )
        .otherwise(None)
        .alias("is_taker"),
        when(
            array_contains(col("conditions"), lit("buy"))
            | array_contains(col("conditions"), lit("BUY")),
            lit("BUY"),
        )
        .when(
            array_contains(col("conditions"), lit("sell"))
            | array_contains(col("conditions"), lit("SELL")),
            lit("SELL"),
        )
        .otherwise(None)
        .alias("trade_side"),
        col("conditions"),
        # Metadata
        col("bronze_ingestion_timestamp"),
        current_timestamp().alias("silver_ingestion_timestamp"),
        to_date(current_timestamp()).alias("silver_ingestion_date"),
        lit("FINNHUB").alias("source_system"),
        lit("crypto_trades_raw").alias("bronze_table"),
        lit("1.0").alias("transformation_version"),
    )

    # Apply validation and data quality scoring
    validate_udf = udf(
        validate_trade,
        SparkArrayType(StringType()),
    )
    quality_score_udf = udf(
        lambda errors: calculate_quality_score(errors) if errors else 1.0,
        DoubleType(),
    )

    silver_df = (
        silver_df.withColumn(
            "validation_errors",
            validate_udf(
                col("price"),
                col("volume"),
                col("timestamp_ms"),
                col("symbol"),
                col("base_currency"),
                col("quote_currency"),
            ),
        )
        .withColumn(
            "is_valid",
            when(
                col("validation_errors").isNull()
                | (col("validation_errors") == array())
                | (size(col("validation_errors")) == 0),
                lit(True),
            ).otherwise(lit(False)),
        )
        .withColumn(
            "data_quality_score",
            quality_score_udf(col("validation_errors")),
        )
    )

    # Filter out invalid records (optional)
    filter_invalid = (
        os.getenv("FILTER_INVALID_RECORDS", "false").lower() == "true"
    )
    if filter_invalid:
        silver_df = silver_df.filter(col("is_valid") == True)
        print(f"   ⚠️  Filtered invalid records in batch {epoch_id}")

    # Final DataFrame
    final_df = silver_df.select(
        col("trade_id"),
        col("symbol"),
        col("exchange"),
        col("base_currency"),
        col("quote_currency"),
        col("price"),
        col("volume"),
        col("trade_value"),
        col("trade_datetime"),
        col("trade_date"),
        col("timestamp_ms"),
        col("hour_of_day"),
        col("day_of_week"),
        col("is_buy"),
        col("is_sell"),
        col("is_maker"),
        col("is_taker"),
        col("trade_side"),
        col("conditions"),
        col("is_valid"),
        col("validation_errors"),
        col("data_quality_score"),
        col("source_system"),
        col("bronze_table"),
        col("bronze_ingestion_timestamp"),
        col("silver_ingestion_timestamp"),
        col("silver_ingestion_date"),
        col("transformation_version"),
    )

    processed_count = final_df.count()
    print(f"   📊 Transformed {processed_count} records in batch {epoch_id}")

    if processed_count == 0:
        print(f"   ⚠️  No records to write in batch {epoch_id}")
        return

    # Write to Silver table
    print(f"   💾 Writing {processed_count} records to {silver_table}...")

    try:
        final_df.writeTo(silver_table).append()
        print(
            f"   ✅ Batch {epoch_id} completed successfully - wrote {processed_count} records"
        )
    except Exception as e:
        print(f"   ❌ Error writing batch {epoch_id}: {str(e)}")
        # Fallback to INSERT INTO
        try:
            final_df.createOrReplaceTempView(f"batch_{epoch_id}_temp")
            spark.sql(
                f"INSERT INTO {silver_table} SELECT * FROM batch_{epoch_id}_temp"
            )
            print(
                f"   ✅ Batch {epoch_id} completed using INSERT INTO - wrote {processed_count} records"
            )
        except Exception as e2:
            print(
                f"   ❌ Failed to write batch {epoch_id} using fallback: {str(e2)}"
            )
            raise


def main():
    """Main function to orchestrate the STREAMING transformation process."""
    try:
        # Get configuration from command line or environment
        bronze_table = (
            sys.argv[1]
            if len(sys.argv) > 1
            else "bronze.bronze.crypto_trades_raw"
        )
        silver_table = (
            sys.argv[2] if len(sys.argv) > 2 else "silver.silver.crypto_trades"
        )
        checkpoint_location = os.getenv(
            "CHECKPOINT_LOCATION",
            f"/tmp/checkpoints/crypto-silver-{bronze_table.replace('.', '-')}-{silver_table.replace('.', '-')}",
        )
        trigger_interval = os.getenv("TRIGGER_INTERVAL", "5 seconds")

        print("=" * 70)
        print("Crypto Trades Bronze to Silver Transformation (STREAMING)")
        print("=" * 70)
        print(f"Source: {bronze_table}")
        print(f"Target: {silver_table}")
        print(f"Checkpoint Location: {checkpoint_location}")
        print(f"Trigger Interval: {trigger_interval}")
        print("=" * 70)

        # Initialize Spark session
        spark = get_spark_session()

        # Create namespace and table if not exists
        silver_parts = silver_table.split(".")
        if len(silver_parts) >= 2:
            silver_catalog = silver_parts[0]
            silver_namespace = silver_parts[1]
            print(
                f"\n📦 Ensuring namespace exists: {silver_catalog}.{silver_namespace}"
            )
            spark.sql(
                f"CREATE NAMESPACE IF NOT EXISTS {silver_catalog}.{silver_namespace}"
            )

        print(f"📋 Ensuring table exists: {silver_table}")

        # Check if table exists, if not create it
        try:
            spark.sql(f"DESCRIBE TABLE {silver_table}").show()
            print(f"   ✅ Table {silver_table} already exists")
        except Exception:
            print(f"   📝 Creating table {silver_table}...")
            # Create empty table with schema
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {silver_table} (
                    trade_id STRING,
                    symbol STRING,
                    exchange STRING,
                    base_currency STRING,
                    quote_currency STRING,
                    price DOUBLE,
                    volume DOUBLE,
                    trade_value DOUBLE,
                    trade_datetime TIMESTAMP,
                    trade_date DATE,
                    timestamp_ms BIGINT,
                    hour_of_day INT,
                    day_of_week INT,
                    is_buy BOOLEAN,
                    is_sell BOOLEAN,
                    is_maker BOOLEAN,
                    is_taker BOOLEAN,
                    trade_side STRING,
                    conditions ARRAY<STRING>,
                    is_valid BOOLEAN,
                    validation_errors ARRAY<STRING>,
                    data_quality_score DOUBLE,
                    source_system STRING,
                    bronze_table STRING,
                    bronze_ingestion_timestamp TIMESTAMP,
                    silver_ingestion_timestamp TIMESTAMP,
                    silver_ingestion_date DATE,
                    transformation_version STRING
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
            )
            print(f"   ✅ Table {silver_table} created")

        # Read from Bronze table as STREAMING
        print(f"\n📡 Starting STREAMING from Bronze table: {bronze_table}")

        # Iceberg streaming reads require different approach
        # We'll use a query-based streaming source that reads incrementally
        # based on bronze_ingestion_timestamp

        # Get the latest timestamp from Silver table to determine starting point
        last_processed_timestamp = None
        try:
            # Try to get max bronze_ingestion_timestamp from Silver table
            result = spark.sql(
                f"SELECT MAX(bronze_ingestion_timestamp) as max_ts FROM {silver_table}"
            ).collect()
            if result and result[0]["max_ts"]:
                last_processed_timestamp = result[0]["max_ts"]
                print(
                    f"   📅 Last processed timestamp: {last_processed_timestamp}"
                )
            else:
                print("   📅 No previous data found, starting from beginning")
        except Exception as e:
            print(f"   ⚠️  Could not get last processed timestamp: {str(e)}")
            print("   📅 Starting from beginning")

        # Iceberg streaming reads don't work well with REST catalog directly
        # We'll use a workaround: rate source as trigger + read from table in foreachBatch
        # This is a common pattern for streaming from Iceberg tables

        # Use a rate source as a trigger mechanism
        # The actual data reading happens inside foreachBatch from the Bronze table
        # Rate source just provides periodic triggers (we ignore the data)
        print(
            "   🔧 Using rate source as trigger with incremental table reads..."
        )

        bronze_stream_df = (
            spark.readStream.format("rate")
            .option(
                "rowsPerSecond", 1
            )  # Very slow rate, just for triggering batches
            .option("numPartitions", 1)
            .load()
        )

        print("   ✅ Streaming trigger initialized")

        print("\n🚀 Starting streaming query...")
        print(
            "   This job will run continuously and process new records as they arrive in Bronze."
        )
        print("   Press Ctrl+C to stop.\n")

        # Use a mutable reference to track last processed timestamp
        last_timestamp_ref = [last_processed_timestamp]

        # Write stream using foreachBatch to handle transformations
        # The rate source triggers the batch, but we read from Bronze table inside foreachBatch
        query = (
            bronze_stream_df.writeStream.foreachBatch(
                lambda batch_df, epoch_id: transform_batch(
                    batch_df,
                    epoch_id,
                    spark,
                    silver_table,
                    bronze_table,
                    last_timestamp_ref,
                )
            )
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime=trigger_interval)
            .outputMode("update")
            .start()
        )

        print("✅ Streaming query started successfully!")
        print(f"   Checkpoint location: {checkpoint_location}")
        print(f"   Trigger interval: {trigger_interval}")
        print("\n" + "=" * 70)
        print("🔄 Streaming job is running...")
        print("   Processing records continuously from Bronze to Silver")
        print("   Press Ctrl+C to stop")
        print("=" * 70 + "\n")

        # Wait for termination
        query.awaitTermination()

        print("\n" + "=" * 70)
        print("✅ Streaming job stopped gracefully")
        print("=" * 70)

    except Exception as e:
        print(f"❌ Error occurred: {str(e)}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)

    finally:
        if "spark" in locals():
            spark.stop()


if __name__ == "__main__":
    main()
