"""Extract raw messages from a Kafka topic as a streaming DataFrame."""

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def extract_from_kafka(
    spark: SparkSession,
    kafka_topic: str,
    kafka_bootstrap_servers: str,
    max_offsets_per_trigger: int,
    kafka_sasl_username: Optional[str] = None,
    kafka_sasl_password: Optional[str] = None,
    kafka_group_id: Optional[str] = None,
    kafka_security_protocol: str = "SASL_SSL",
    kafka_sasl_mechanism: str = "SCRAM-SHA-512",
    kafka_ssl_truststore_location: Optional[str] = None,
    kafka_starting_offsets: str = "latest",
) -> DataFrame:
    """
    Read messages from a Kafka topic as a Spark Structured Streaming DataFrame.

    The raw Kafka value (bytes) is kept as-is and will be cast to STRING
    upstream in the transform step. This keeps the extract layer generic
    so the job can be pointed at any Kafka topic without schema changes.

    Args:
        spark: Active SparkSession.
        kafka_topic: Kafka topic name to subscribe to.
        kafka_bootstrap_servers: Comma-separated Kafka broker addresses.
        max_offsets_per_trigger: Maximum Kafka offsets to process each micro-batch.
        kafka_sasl_username: SASL/PLAIN username (Optional).
        kafka_sasl_password: SASL/PLAIN password (Optional).

    Returns:
        Streaming DataFrame with Kafka metadata columns:
        key, value, topic, partition, offset, timestamp, timestampType.
    """
    logger.info("[KAFKA] Connecting to brokers=[%s]", kafka_bootstrap_servers)
    logger.info("[KAFKA] topic=[%s] startingOffsets=[%s] maxOffsetsPerTrigger=[%s]",
                kafka_topic, kafka_starting_offsets, max_offsets_per_trigger)

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", kafka_starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
    )

    if kafka_group_id:
        reader = reader.option("kafka.group.id", kafka_group_id)
        logger.info("[KAFKA] Consumer group ID: %s", kafka_group_id)

    if kafka_sasl_username and kafka_sasl_password:
        if kafka_sasl_mechanism == "SCRAM-SHA-512":
            login_module = "org.apache.kafka.common.security.scram.ScramLoginModule"
        else:
            login_module = "org.apache.kafka.common.security.plain.PlainLoginModule"

        jaas_config = (
            f"{login_module} required "
            f'username="{kafka_sasl_username}" '
            f'password="{kafka_sasl_password}";'
        )
        reader = (
            reader.option("kafka.security.protocol", kafka_security_protocol)
            .option("kafka.sasl.mechanism", kafka_sasl_mechanism)
            .option("kafka.sasl.jaas.config", jaas_config)
        )
        if kafka_security_protocol == "SASL_SSL":
            reader = reader.option(
                "kafka.ssl.endpoint.identification.algorithm", ""
            )
            if kafka_ssl_truststore_location:
                reader = (
                    reader.option("kafka.ssl.truststore.type", "PEM")
                    .option(
                        "kafka.ssl.truststore.location",
                        kafka_ssl_truststore_location,
                    )
                )
            logger.info("[KAFKA] SSL truststore: type=PEM location=%s", kafka_ssl_truststore_location)
        logger.info(
            "[KAFKA] Auth configured: protocol=%s mechanism=%s user=%s",
            kafka_security_protocol,
            kafka_sasl_mechanism,
            kafka_sasl_username,
        )

    stream_df = reader.load()

    logger.info("[KAFKA] Streaming source initialized successfully.")
    return stream_df
