"""
Main entrypoint for the Streaming Test job.

Pipeline
--------
Kafka topic  →  (raw bytes)  →  flat Bronze table in Iceberg / Lakehouse

Purpose
-------
This is a **test** job used to validate the end-to-end streaming pipeline
on the data platform:

    Kafka → Spark Structured Streaming → Iceberg REST (Gravitino/Lakekeeper)
            → MinIO (S3-compatible object store) via Credential Vending

The job intentionally keeps the transform step minimal: the raw Kafka value
is stored as a UTF-8 string alongside the standard Kafka metadata columns.
This makes the job topic-agnostic, so it can be pointed at any Kafka topic
without code changes.
"""

import logging
import sys

from config import load_job_config
from etl.extract import extract_from_kafka
from etl.load import ensure_table_exists, load_to_bronze
from etl.transform import transform_raw_message
from spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Batch processor
# ---------------------------------------------------------------------------


def process_batch(
    batch_df, epoch_id, catalog: str, schema: str, table: str
) -> None:
    """
    Process a single micro-batch.

    Called by Spark's `foreachBatch` API for each triggered micro-batch.
    Errors are re-raised so Spark's checkpoint mechanism can handle retries.
    """
    logger.info("[BATCH-%d] ── Micro-batch started ──────────────────────", epoch_id)
    try:
        load_to_bronze(batch_df, catalog, schema, table, epoch_id)
        logger.info("[BATCH-%d] ── Micro-batch completed ─────────────────────", epoch_id)
    except Exception as exc:
        logger.error("[BATCH-%d] FAILED: %s", epoch_id, exc, exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point — initialise Spark, set up the streaming query, and wait."""
    job_cfg = load_job_config()

    # Allow CLI arguments to override env-var defaults
    kafka_topic = sys.argv[1] if len(sys.argv) > 1 else job_cfg["kafka_topic"]
    catalog = sys.argv[2] if len(sys.argv) > 2 else job_cfg["catalog"]
    schema = sys.argv[3] if len(sys.argv) > 3 else job_cfg["schema"]
    table = sys.argv[4] if len(sys.argv) > 4 else job_cfg["table"]

    logger.info("=" * 70)
    logger.info("[JOB] Streaming Test Job  —  Kafka → Lakehouse (Bronze)")
    logger.info("=" * 70)
    logger.info("[JOB]   Kafka topic         : %s", kafka_topic)
    logger.info("[JOB]   Bootstrap servers   : %s", job_cfg["kafka_bootstrap_servers"])
    logger.info("[JOB]   Starting offsets    : %s", job_cfg["kafka_starting_offsets"])
    logger.info("[JOB]   Target table        : %s.%s.%s", catalog, schema, table)
    logger.info("[JOB]   Checkpoint location : %s", job_cfg["checkpoint_location"])
    logger.info("[JOB]   Trigger interval    : %s", job_cfg["trigger_interval"])
    logger.info("[JOB]   Max offsets/trigger : %s", job_cfg["max_offsets_per_trigger"])
    logger.info("=" * 70)

    spark = get_spark_session()

    try:
        # Ensure the target Iceberg table exists before streaming starts
        ensure_table_exists(spark, catalog, schema, table)

        # Extract: read raw Kafka messages as a streaming DataFrame
        kafka_stream_df = extract_from_kafka(
            spark=spark,
            kafka_topic=kafka_topic,
            kafka_bootstrap_servers=job_cfg["kafka_bootstrap_servers"],
            max_offsets_per_trigger=job_cfg["max_offsets_per_trigger"],
            kafka_sasl_username=job_cfg["kafka_sasl_username"],
            kafka_sasl_password=job_cfg["kafka_sasl_password"],
            kafka_security_protocol=job_cfg["kafka_security_protocol"],
            kafka_sasl_mechanism=job_cfg["kafka_sasl_mechanism"],
            kafka_ssl_truststore_location=job_cfg["kafka_ssl_truststore_location"],
            kafka_starting_offsets=job_cfg["kafka_starting_offsets"],
            kafka_group_id=job_cfg["kafka_group_id"],
        )

        # Transform: flatten to Bronze schema (raw value + Kafka metadata)
        transformed_df = transform_raw_message(kafka_stream_df)

        # Load: write each micro-batch to Iceberg via foreachBatch
        query = (
            transformed_df.writeStream.foreachBatch(
                lambda batch_df, epoch_id: process_batch(
                    batch_df, epoch_id, catalog, schema, table
                )
            )
            .option("checkpointLocation", job_cfg["checkpoint_location"])
            .trigger(processingTime=job_cfg["trigger_interval"])
            .outputMode("append")
            .start()
        )

        logger.info("[JOB] Streaming query started. Waiting for termination...")
        query.awaitTermination()

    except Exception as exc:
        logger.error("[JOB] Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("[JOB] SparkSession stopped.")


if __name__ == "__main__":
    main()
