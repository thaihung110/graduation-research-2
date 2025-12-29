import io
import json
import logging
from typing import Optional, Tuple

import avro.io
import avro.schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)


class AvroDecoder:
    """Decode Avro messages from Kafka."""

    def __init__(self, avro_schema_json: str):
        """Initialize decoder with Avro schema."""
        self.schema_obj = avro.schema.parse(avro_schema_json)

    def decode_row(self, row) -> Optional[Tuple[int, int, int, str]]:
        """Decode Avro binary message from Kafka row."""
        try:
            value_bytes = row.value
            if value_bytes is None:
                return None

            # Decode Avro binary to Python dict
            bytes_reader = io.BytesIO(value_bytes)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(self.schema_obj)
            decoded = reader.read(decoder)

            # Convert timestamp to milliseconds
            kafka_timestamp_ms = 0
            if row.timestamp is not None:
                if isinstance(row.timestamp, (int, float)):
                    kafka_timestamp_ms = int(row.timestamp)
                else:
                    import datetime

                    if isinstance(row.timestamp, datetime.datetime):
                        kafka_timestamp_ms = int(
                            row.timestamp.timestamp() * 1000
                        )
                    else:
                        kafka_timestamp_ms = (
                            int(float(row.timestamp) * 1000)
                            if row.timestamp
                            else 0
                        )

            return (
                int(row.partition),
                int(row.offset),
                kafka_timestamp_ms,
                json.dumps(decoded),
            )
        except Exception as e:
            logger.warning(
                f"Error decoding Avro at partition={row.partition}, "
                f"offset={row.offset}: {str(e)}"
            )
            return None

    def decode_batch(
        self,
        batch_df: DataFrame,
        spark: SparkSession,
        message_schema: StructType,
    ) -> DataFrame:
        """Decode Avro messages in a batch DataFrame."""
        # Convert to RDD, decode, filter None, convert back to DataFrame
        decoded_rdd = batch_df.rdd.map(self.decode_row).filter(
            lambda x: x is not None
        )

        decoded_schema = StructType(
            [
                StructField("kafka_partition", IntegerType(), False),
                StructField("kafka_offset", LongType(), False),
                StructField("kafka_timestamp", LongType(), False),
                StructField("message_json", StringType(), False),
            ]
        )

        decoded_df = spark.createDataFrame(decoded_rdd, decoded_schema)

        # Parse JSON and return DataFrame with parsed message
        parsed_df = decoded_df.select(
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            from_json(col("message_json"), message_schema).alias("message"),
        )

        return parsed_df
