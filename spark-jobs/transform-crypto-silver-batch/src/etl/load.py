"""Load transformed data into Silver Iceberg table."""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def ensure_silver_table_exists(
    spark: SparkSession, database: str, table: str
) -> None:
    """
    Ensure Silver table exists, create if it doesn't.

    Args:
        spark: SparkSession instance
        database: Database/namespace name
        table: Table name
    """
    from utils.schemas import get_create_table_sql

    def _create_namespace():
        logger.info(f"Ensuring namespace exists: silver.{database}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS silver.{database}")

    def _check_table():
        table_path = f"silver.{database}.{table}"
        spark.sql(f"DESCRIBE TABLE {table_path}").show()
        logger.info(f"✅ Table {table_path} already exists")
        return table_path

    def _create_table():
        table_path = f"silver.{database}.{table}"
        logger.info(f"Creating table {table_path}...")
        spark.sql(get_create_table_sql(database, table))
        logger.info(f"✅ Table {table_path} created")
        return table_path

    # Create namespace
    _create_namespace()

    # Check table exists, create if not
    table_path = f"silver.{database}.{table}"
    try:
        _check_table()
    except Exception:
        _create_table()


def load_to_silver(df: DataFrame, database: str, table: str) -> None:
    """
    Load aggregated DataFrame to Silver Iceberg table.

    Args:
        df: Aggregated DataFrame ready for Silver
        database: Database/namespace name
        table: Table name
    """
    table_path = f"silver.{database}.{table}"

    logger.info(f"Loading data to {table_path}...")
    record_count = df.count()
    logger.info(f"Writing {record_count} aggregated records...")

    # Write to Silver table (append mode)
    df.writeTo(table_path).append()

    logger.info(
        f"✅ Successfully loaded {record_count} records to {table_path}"
    )
