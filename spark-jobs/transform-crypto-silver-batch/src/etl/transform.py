"""Transform Bronze data into Silver OHLCV aggregations."""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def transform_to_ohlcv_1h(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Bronze trades into 1-hour OHLCV aggregations.

    Args:
        bronze_df: DataFrame with Bronze trade data

    Returns:
        DataFrame with hourly OHLCV aggregations
    """
    logger.info("Transforming to 1-hour OHLCV aggregations...")

    # Create 1-hour time windows
    windowed_df = bronze_df.withColumn(
        "hour_window", F.window(F.col("trade_datetime"), "1 hour")
    )

    # Define window spec for first/last price within each hour window
    # Partitioned by symbol, exchange, and hour window, ordered by trade_datetime
    window_spec = Window.partitionBy(
        "symbol", "exchange", "hour_window"
    ).orderBy("trade_datetime")

    # Add row numbers for first and last trades
    windowed_df = windowed_df.withColumn(
        "row_num_asc", F.row_number().over(window_spec)
    )
    windowed_df = windowed_df.withColumn(
        "row_num_desc",
        F.row_number().over(window_spec.orderBy(F.desc("trade_datetime"))),
    )

    # Perform aggregations
    agg_df = windowed_df.groupBy(
        "symbol", "exchange", "base_currency", "quote_currency", "hour_window"
    ).agg(
        # OHLC prices
        F.first(F.when(F.col("row_num_asc") == 1, F.col("price"))).alias(
            "open_price"
        ),
        F.max("price").alias("high_price"),
        F.min("price").alias("low_price"),
        F.first(F.when(F.col("row_num_desc") == 1, F.col("price"))).alias(
            "close_price"
        ),
        # Volume and trade statistics
        F.sum("volume").alias("total_volume"),
        F.count("*").alias("trade_count"),
        F.avg("price").alias("avg_price"),
        # VWAP = sum(price * volume) / sum(volume)
        (F.sum(F.col("price") * F.col("volume")) / F.sum("volume")).alias(
            "vwap"
        ),
    )

    # Extract hour_start and hour_end from window
    final_df = agg_df.select(
        "symbol",
        "exchange",
        "base_currency",
        "quote_currency",
        F.col("hour_window.start").alias("hour_start"),
        F.col("hour_window.end").alias("hour_end"),
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "total_volume",
        "trade_count",
        "avg_price",
        "vwap",
        # Price changes
        (F.col("close_price") - F.col("open_price")).alias("price_change"),
        ((F.col("close_price") / F.col("open_price") - 1) * 100).alias(
            "price_change_pct"
        ),
        # Partitioning column
        F.to_date(F.col("hour_window.start")).alias("agg_date"),
        # Metadata
        F.current_timestamp().alias("silver_ingestion_timestamp"),
    )

    agg_count = final_df.count()
    logger.info(f"✅ Created {agg_count} hourly OHLCV aggregations")

    return final_df
