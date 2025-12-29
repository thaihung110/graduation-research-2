"""Extract data from Kafka streaming source."""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def extract_from_kafka(
    spark: SparkSession,
    kafka_topic: str,
    kafka_bootstrap_servers: str,
    max_offsets_per_trigger: int,
) -> DataFrame:
    """
    Extract data from Kafka topic as streaming DataFrame.

    Args:
        spark: SparkSession instance
        kafka_topic: Kafka topic to read from
        kafka_bootstrap_servers: Kafka bootstrap servers
        max_offsets_per_trigger: Maximum offsets to process per trigger

    Returns:
        Streaming DataFrame from Kafka
    """
    logger.info(f"Extracting from Kafka topic: {kafka_topic}")

    kafka_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
        # SASL authentication configuration
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";',
        )
        .load()
    )

    logger.info(
        "✅ Kafka streaming source initialized with SASL authentication"
    )
    return kafka_stream_df
