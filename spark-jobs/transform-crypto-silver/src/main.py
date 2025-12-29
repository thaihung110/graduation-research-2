"""
Main entrypoint for Crypto Trades Bronze to Silver Transformation (STREAMING).

This job:
1. Reads from Bronze Iceberg table (STREAMING)
2. Transforms and enriches the data
3. Validates and scores data quality
4. Continuously loads data into Silver Iceberg table
"""

import logging
import sys

from config import load_job_config
from etl.extract import extract_from_bronze, get_last_processed_timestamp
from etl.load import ensure_silver_table_exists, load_to_silver
from etl.transform import transform_trades
from pyspark.sql.functions import col, max as spark_max
from spark_session import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_process_batch_function(spark, bronze_table, silver_table):
    """
    Create a process_batch function with closure over last_timestamp.
    
    Returns:
        Function that can be used in foreachBatch
    """
    last_timestamp = [None]

    def process_batch(batch_df, epoch_id):
        """Process each micro-batch in streaming."""
        logger.info(f"Processing batch {epoch_id}...")

        # Get last processed timestamp
        last_ts = get_last_processed_timestamp(spark, silver_table)
        if last_ts is None and last_timestamp[0] is not None:
            last_ts = last_timestamp[0]

        # Extract from Bronze table
        bronze_df = extract_from_bronze(
            spark=spark, bronze_table=bronze_table, filter_timestamp=last_ts
        )

        if bronze_df is None or bronze_df.count() == 0:
            logger.warning(f"No new records in batch {epoch_id}")
            return

        # Transform
        logger.info(f"Transforming records in batch {epoch_id}...")
        transformed_df = transform_trades(bronze_df)

        count = transformed_df.count()
        logger.info(f"Transformed {count} records in batch {epoch_id}")

        if count == 0:
            return

        # Load to Silver
        load_to_silver(transformed_df, silver_table, epoch_id)

        # Update last timestamp
        max_ts = bronze_df.agg(
            spark_max(col("bronze_ingestion_timestamp"))
        ).collect()[0][0]
        last_timestamp[0] = max_ts

    return process_batch


def main():
    """Main orchestration function."""
    try:
        # Load configuration
        job_config = load_job_config()

        # Parse command line arguments (override config if provided)
        bronze_table = (
            sys.argv[1]
            if len(sys.argv) > 1
            else job_config["bronze_table"]
        )
        silver_table = (
            sys.argv[2]
            if len(sys.argv) > 2
            else job_config["silver_table"]
        )

        logger.info("=" * 70)
        logger.info("Crypto Trades Bronze to Silver Transformation (STREAMING)")
        logger.info("=" * 70)
        logger.info(f"Source: {bronze_table}")
        logger.info(f"Target: {silver_table}")
        logger.info(f"Checkpoint Location: {job_config['checkpoint_location']}")
        logger.info(f"Trigger Interval: {job_config['trigger_interval']}")
        logger.info("=" * 70)

        # Initialize Spark
        spark = get_spark_session()

        # Ensure Silver table exists
        ensure_silver_table_exists(spark, silver_table)

        # Create process_batch function with closure
        process_batch_fn = create_process_batch_function(
            spark, bronze_table, silver_table
        )

        logger.info("🚀 Starting streaming query...")
        logger.info(
            "   This job will run continuously and process messages as they arrive."
        )
        logger.info("   Press Ctrl+C to stop.\n")

        # Rate source as trigger
        trigger_df = (
            spark.readStream.format("rate")
            .option("rowsPerSecond", 1)
            .option("numPartitions", 1)
            .load()
        )

        # Start streaming with foreachBatch
        query = (
            trigger_df.writeStream.foreachBatch(process_batch_fn)
            .option("checkpointLocation", job_config["checkpoint_location"])
            .trigger(processingTime=job_config["trigger_interval"])
            .outputMode("update")
            .start()
        )

        logger.info("✅ Streaming query started successfully!")
        logger.info(f"   Checkpoint location: {job_config['checkpoint_location']}")
        logger.info(f"   Trigger interval: {job_config['trigger_interval']}")
        logger.info("\n" + "=" * 70)
        logger.info("🔄 Streaming job is running...")
        logger.info("   Processing messages continuously from Bronze")
        logger.info("   Press Ctrl+C to stop")
        logger.info("=" * 70 + "\n")

        # Wait for termination
        query.awaitTermination()

        logger.info("\n" + "=" * 70)
        logger.info("✅ Streaming job stopped gracefully")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"❌ Error occurred: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if "spark" in locals():
            spark.stop()


if __name__ == "__main__":
    main()
