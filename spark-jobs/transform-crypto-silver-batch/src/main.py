"""
Main entrypoint for Crypto OHLCV Aggregation Job (BATCH).

This job:
1. Reads crypto trades from Bronze Iceberg table for specified date range
2. Aggregates trades into 1-hour OHLCV (Open-High-Low-Close-Volume) candles
3. Calculates additional statistics (VWAP, price changes)
4. Loads aggregated data into Silver Iceberg table
"""

import logging
import sys

from config import load_job_config
from etl.extract import extract_from_bronze
from etl.load import ensure_silver_table_exists, load_to_silver
from etl.transform import transform_to_ohlcv_1h
from spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Main orchestration function."""
    try:
        # Parse command line arguments
        if len(sys.argv) < 3:
            logger.error("Usage: python main.py <start_date> <end_date>")
            logger.error("Example: python main.py 2025-12-28 2025-12-29")
            sys.exit(1)

        start_date = sys.argv[1]
        end_date = sys.argv[2]

        # Load configuration
        job_config = load_job_config()
        bronze_table = job_config["bronze_table"]
        silver_table = job_config["silver_table"]

        # Parse silver table path (format: catalog.database.table)
        silver_parts = silver_table.split(".")
        if len(silver_parts) != 3:
            raise ValueError(
                f"Invalid silver table format: {silver_table}. Expected: catalog.database.table"
            )
        silver_database = silver_parts[1]
        silver_table_name = silver_parts[2]

        logger.info("=" * 70)
        logger.info("Crypto OHLCV Aggregation Job (BATCH)")
        logger.info("=" * 70)
        logger.info(f"Date Range: {start_date} to {end_date}")
        logger.info(f"Bronze Table: {bronze_table}")
        logger.info(f"Silver Table: {silver_table}")
        logger.info("=" * 70)

        # Initialize Spark
        logger.info("\n🚀 Initializing Spark session...")
        spark = get_spark_session()
        logger.info("✅ Spark session initialized")

        # Ensure Silver table exists
        logger.info("\n📋 Ensuring Silver table exists...")
        ensure_silver_table_exists(spark, silver_database, silver_table_name)

        # Extract from Bronze
        logger.info("\n📥 Extracting data from Bronze...")
        bronze_df = extract_from_bronze(
            spark, bronze_table, start_date, end_date
        )

        if bronze_df.count() == 0:
            logger.warning(
                f"⚠️  No data found in Bronze for date range {start_date} to {end_date}"
            )
            logger.info("Job completed with no data to process.")
            return

        # Transform to OHLCV
        logger.info("\n🔄 Transforming to hourly OHLCV aggregations...")
        ohlcv_df = transform_to_ohlcv_1h(bronze_df)

        # Load to Silver
        logger.info("\n📤 Loading aggregated data to Silver...")
        load_to_silver(ohlcv_df, silver_database, silver_table_name)

        logger.info("\n" + "=" * 70)
        logger.info("✅ Job completed successfully!")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"\n❌ Error occurred: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if "spark" in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
