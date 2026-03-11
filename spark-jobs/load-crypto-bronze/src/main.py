"""
Main entrypoint for Crypto Trades Bronze Ingestion Job (STREAMING).

This job:
1. Reads Avro-encoded messages from Kafka topic (STREAMING)
2. Decodes Avro messages to structured data
3. Transforms and enriches the data
4. Continuously loads data into Iceberg table in bronze warehouse
"""

import base64
import json
import logging
import sys
from urllib import parse, request

from config import load_job_config, load_oauth_debug_config
from etl.extract import extract_from_kafka
from etl.load import ensure_bronze_table_exists, load_to_bronze
from etl.transform import transform_trades
from spark_session import get_spark_session
from utils.avro_decoder import AvroDecoder
from utils.schemas import get_avro_schema, get_avro_schema_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _decode_jwt_payload(token: str) -> dict:
    """
    Decode JWT payload (without verifying signature) for debugging purposes.

    This is only used for logging to help debug OAuth configuration issues.
    """
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}

        payload_b64 = parts[1]
        # Add padding if necessary
        padding = "=" * (-len(payload_b64) % 4)
        decoded_bytes = base64.urlsafe_b64decode(payload_b64 + padding)
        return json.loads(decoded_bytes.decode("utf-8"))
    except Exception as e:
        logger.warning("Failed to decode JWT payload for debug: %s", e, exc_info=True)
        return {}


def log_oauth_access_token_debug() -> None:
    """
    Fetch and decode an access token from Keycloak using the same client
    credentials Spark/Iceberg REST use, then log the JWT payload.
    """
    try:
        oauth_conf = load_oauth_debug_config()

        logger.info(
            "OAuth debug: requesting access token from %s with client_id=%s, scope=%s",
            oauth_conf["token_endpoint"],
            oauth_conf["client_id"],
            oauth_conf["scope"],
        )

        data = parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": oauth_conf["client_id"],
                "client_secret": oauth_conf["client_secret"],
                "scope": oauth_conf["scope"],
            }
        ).encode("utf-8")

        req = request.Request(oauth_conf["token_endpoint"], data=data)
        req.add_header(
            "Content-Type", "application/x-www-form-urlencoded"
        )

        with request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")

        token_response = json.loads(body)
        access_token = token_response.get("access_token")

        if not access_token:
            logger.warning(
                "OAuth debug: no access_token in response. Raw response: %s", body
            )
            return

        payload = _decode_jwt_payload(access_token)

        # Do not log full token to avoid leaking secrets; only log length and payload.
        logger.info(
            "OAuth debug: received access token (length=%d). Decoded JWT payload:\n%s",
            len(access_token),
            json.dumps(payload, indent=2, sort_keys=True),
        )
    except Exception as e:
        logger.error(
            "OAuth debug: failed to fetch/decode access token: %s",
            e,
            exc_info=True,
        )


def process_batch(
    batch_df, epoch_id, spark, database, table, avro_decoder, message_schema
):
    """
    Process each micro-batch in streaming.

    This function is called by Spark's foreachBatch for each batch.
    """
    try:
        logger.info(f"Processing batch {epoch_id}...")

        batch_count = batch_df.count()
        if batch_count == 0:
            logger.warning(f"No messages in batch {epoch_id}")
            return

        logger.info(f"Processing {batch_count} messages in batch {epoch_id}")

        # Decode Avro messages
        parsed_df = avro_decoder.decode_batch(batch_df, spark, message_schema)

        decoded_count = parsed_df.count()
        logger.info(
            f"Decoded {decoded_count} messages from {batch_count} total"
        )

        if decoded_count == 0:
            logger.warning(f"No valid messages decoded in batch {epoch_id}")
            return

        # Transform trades
        transformed_df = transform_trades(parsed_df)

        processed_count = transformed_df.count()
        logger.info(
            f"Processed {processed_count} trade records in batch {epoch_id}"
        )

        if processed_count == 0:
            logger.warning(f"No records to write in batch {epoch_id}")
            return

        # Load to Bronze
        load_to_bronze(transformed_df, database, table, epoch_id)

    except Exception as e:
        # Log error but don't fail the entire streaming query
        logger.error(f"Error processing batch {epoch_id}: {e}", exc_info=True)
        # Re-raise to let Spark handle it (will retry based on checkpoint)
        raise


def main():
    """Main orchestration function."""
    try:
        # Load configuration
        job_config = load_job_config()

        # Parse command line arguments (override config if provided)
        kafka_topic = (
            sys.argv[1] if len(sys.argv) > 1 else job_config["kafka_topic"]
        )
        database = sys.argv[2] if len(sys.argv) > 2 else job_config["database"]
        table = sys.argv[3] if len(sys.argv) > 3 else job_config["table"]

        # Format checkpoint location
        checkpoint_location = job_config["checkpoint_location"]
        if (
            "{database}" in checkpoint_location
            or "{table}" in checkpoint_location
        ):
            checkpoint_location = checkpoint_location.format(
                database=database, table=table
            )
        elif checkpoint_location == "/tmp/checkpoints/crypto-bronze":
            checkpoint_location = (
                f"/tmp/checkpoints/crypto-bronze-{database}-{table}"
            )

        logger.info("=" * 70)
        logger.info("Crypto Trades Bronze Ingestion Job (STREAMING)")
        logger.info("=" * 70)
        logger.info(f"Kafka Topic: {kafka_topic}")
        logger.info(f"Target: {database}.{table}")
        logger.info(f"Checkpoint Location: {checkpoint_location}")
        logger.info(f"Trigger Interval: {job_config['trigger_interval']}")
        logger.info("=" * 70)

        # Debug: fetch and decode OAuth access token used by Iceberg REST client
        # to help diagnose authentication / JWKS issues.
        logger.info("OAuth debug: starting access token fetch and decode...")
        log_oauth_access_token_debug()

        # Initialize Spark
        spark = get_spark_session()

        # Ensure Bronze table exists
        ensure_bronze_table_exists(spark, database, table)

        # Initialize Avro decoder
        avro_decoder = AvroDecoder(get_avro_schema_json())
        message_schema = get_avro_schema()

        # Extract from Kafka
        kafka_stream_df = extract_from_kafka(
            spark=spark,
            kafka_topic=kafka_topic,
            kafka_bootstrap_servers=job_config["kafka_bootstrap_servers"],
            max_offsets_per_trigger=job_config["max_offsets_per_trigger"],
        )

        logger.info("🚀 Starting streaming query...")
        logger.info(
            "   This job will run continuously and process messages as they arrive."
        )
        logger.info("   Press Ctrl+C to stop.\n")

        # Start streaming with foreachBatch
        query = (
            kafka_stream_df.writeStream.foreachBatch(
                lambda batch_df, epoch_id: process_batch(
                    batch_df,
                    epoch_id,
                    spark,
                    database,
                    table,
                    avro_decoder,
                    message_schema,
                )
            )
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime=job_config["trigger_interval"])
            .outputMode("update")
            .start()
        )

        logger.info("✅ Streaming query started successfully!")
        logger.info(f"   Checkpoint location: {checkpoint_location}")
        logger.info(f"   Trigger interval: {job_config['trigger_interval']}")
        logger.info("\n" + "=" * 70)
        logger.info("🔄 Streaming job is running...")
        logger.info("   Processing messages continuously from Kafka")
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
