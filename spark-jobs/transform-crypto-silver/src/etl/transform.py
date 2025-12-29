"""Transform Bronze crypto trades to Silver schema."""

import os
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    array_contains,
    col,
    concat_ws,
    current_timestamp,
    dayofweek,
    hour,
    lit,
    md5,
    size,
    to_date,
    udf,
    upper,
    when,
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import ArrayType, DoubleType, StringType

from validation.quality_scoring import QualityScorer
from validation.trade_validator import TradeValidator

logger = logging.getLogger(__name__)


def transform_trades(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Bronze DataFrame to Silver schema.
    
    Args:
        bronze_df: DataFrame from Bronze table
        
    Returns:
        Transformed DataFrame ready for Silver table
    """
    logger.debug("Transforming trades from Bronze to Silver")

    # Create UDFs for validation and quality scoring
    validate_udf = udf(
        lambda price, volume, ts, sym, base, quote: TradeValidator.validate(
            price, volume, ts, sym, base, quote
        ),
        ArrayType(StringType()),
    )

    quality_score_udf = udf(
        lambda errors: QualityScorer.calculate(errors),
        DoubleType(),
    )

    # Core transformation
    transformed_df = bronze_df.select(
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
        # Primary fields
        col("symbol"),
        upper(col("exchange")).alias("exchange"),
        upper(col("base_currency")).alias("base_currency"),
        upper(col("quote_currency")).alias("quote_currency"),
        # Trade data (8 decimals)
        spark_round(col("price"), 8).alias("price"),
        spark_round(col("volume"), 8).alias("volume"),
        spark_round(col("price") * col("volume"), 8).alias("trade_value"),
        # Timestamps
        col("trade_datetime"),
        col("trade_date"),
        col("timestamp_ms"),
        hour(col("trade_datetime")).alias("hour_of_day"),
        dayofweek(col("trade_datetime")).alias("day_of_week"),
        # Trade conditions (using SQL expressions instead of UDF for performance)
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

    # Apply validation and quality scoring
    transformed_df = (
        transformed_df.withColumn(
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

    # Optional: filter invalid records
    if os.getenv("FILTER_INVALID_RECORDS", "false").lower() == "true":
        transformed_df = transformed_df.filter(col("is_valid") == True)

    # Select final columns in order
    final_df = transformed_df.select(
        "trade_id",
        "symbol",
        "exchange",
        "base_currency",
        "quote_currency",
        "price",
        "volume",
        "trade_value",
        "trade_datetime",
        "trade_date",
        "timestamp_ms",
        "hour_of_day",
        "day_of_week",
        "is_buy",
        "is_sell",
        "is_maker",
        "is_taker",
        "trade_side",
        "conditions",
        "is_valid",
        "validation_errors",
        "data_quality_score",
        "source_system",
        "bronze_table",
        "bronze_ingestion_timestamp",
        "silver_ingestion_timestamp",
        "silver_ingestion_date",
        "transformation_version",
    )

    return final_df

