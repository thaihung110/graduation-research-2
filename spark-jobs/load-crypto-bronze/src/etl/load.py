"""Load transformed data into Iceberg Bronze table."""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def ensure_bronze_table_exists(
    spark: SparkSession, database: str, table: str
) -> None:
    """
    Ensure Bronze table exists, create if it doesn't.

    Args:
        spark: SparkSession instance
        database: Database/namespace name
        table: Table name
    """
    from utils.schemas import get_create_table_sql

    def _create_namespace():
        logger.info(f"Ensuring namespace exists: lakekeeper.{database}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakekeeper.{database}")

    def _check_table():
        table_path = f"lakekeeper.{database}.{table}"
        spark.sql(f"DESCRIBE TABLE {table_path}").show()
        logger.info(f"✅ Table {table_path} already exists")
        return table_path

    def _create_table():
        table_path = f"lakekeeper.{database}.{table}"
        logger.info(f"Creating table {table_path}...")
        spark.sql(get_create_table_sql(database, table))
        logger.info(f"✅ Table {table_path} created")
        return table_path

    # Create namespace
    _create_namespace()

    # Check table exists, create if not
    table_path = f"lakekeeper.{database}.{table}"
    try:
        _check_table()
    except Exception:
        _create_table()


def load_to_bronze(
    df: DataFrame, database: str, table: str, epoch_id: int
) -> None:
    """
    Load transformed DataFrame to Bronze Iceberg table.

    Args:
        df: Transformed DataFrame ready for Bronze
        database: Database/namespace name
        table: Table name
        epoch_id: Batch epoch ID for logging
    """
    table_path = f"lakekeeper.{database}.{table}"

    def _write_with_writeto():
        logger.info(f"Writing batch {epoch_id} to {table_path}...")
        df.writeTo(table_path).append()
        logger.info(f"✅ Batch {epoch_id} written successfully")

    def _write_with_insert():
        # Fallback to INSERT INTO if writeTo fails
        df.createOrReplaceTempView(f"batch_{epoch_id}_temp")
        spark = SparkSession.getActiveSession()
        if spark:
            spark.sql(
                f"INSERT INTO {table_path} SELECT * FROM batch_{epoch_id}_temp"
            )
            logger.info(f"✅ Batch {epoch_id} written using INSERT INTO")
        else:
            raise RuntimeError("No active SparkSession found")

    # Try writeTo
    try:
        _write_with_writeto()
    except Exception as e:
        logger.warning(f"writeTo failed, trying INSERT INTO: {e}")
        # Fallback to INSERT INTO
        _write_with_insert()
