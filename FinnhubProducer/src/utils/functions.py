import io
import json

import avro.io
import avro.schema
import finnhub
from kafka import KafkaProducer


# setting up Finnhub settings
def load_config(config_file):
    with open(config_file, "r") as f:
        config = json.load(f)
    return config


# setting up Finnhub client connection to test if tickers specified in config exist
def load_client(token):
    return finnhub.Client(api_key=token)


# look up ticker in Finnhub
def lookup_ticker(finnhub_client, ticker):
    return finnhub_client.symbol_lookup(ticker)


# validate if ticker exists
def ticker_validator(finnhub_client, ticker):
    """Validate ticker exists. Skip validation for crypto symbols."""
    # Crypto symbols format: EXCHANGE:PAIR (e.g., BINANCE:BTCUSDT)
    # Finnhub symbol_lookup API doesn't work well with crypto
    if ":" in ticker and any(
        exchange in ticker
        for exchange in ["BINANCE", "COINBASE", "KRAKEN", "BITFINEX"]
    ):
        print(f"   ℹ️  Crypto symbol detected, skipping validation")
        return True

    try:
        results = lookup_ticker(finnhub_client, ticker)
        if not results or "result" not in results:
            return False

        for stock in results["result"]:
            if stock.get("symbol") == ticker:
                return True
        return False
    except Exception as e:
        print(f"   ⚠️  Validation error: {e}, proceeding anyway")
        return False  # Return False to let caller decide


# setting up a Kafka connection
def load_producer(kafka_server, sasl_username=None, sasl_password=None):
    """
    Create a Kafka producer with optional SASL authentication.

    Args:
        kafka_server: Kafka bootstrap server address
        sasl_username: Optional SASL username for authentication
        sasl_password: Optional SASL password for authentication

    Returns:
        KafkaProducer instance
    """
    config = {
        "bootstrap_servers": kafka_server,
        # Retry configuration
        "retries": 3,
        "max_in_flight_requests_per_connection": 1,
        # Timeout configuration
        "request_timeout_ms": 30000,
        # Compression for better network efficiency
        "compression_type": "snappy",
        # Reliability - wait for all replicas
        "acks": "all",
        # Buffer settings
        "buffer_memory": 33554432,  # 32MB
        "batch_size": 16384,  # 16KB
        "linger_ms": 10,  # Wait 10ms to batch messages
    }

    # Add SASL authentication if credentials provided
    if sasl_username and sasl_password:
        config.update(
            {
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": sasl_username,
                "sasl_plain_password": sasl_password,
            }
        )
        print(f"✅ SASL authentication enabled for user: {sasl_username}")
    else:
        print(
            "ℹ️  No SASL credentials provided, connecting without authentication"
        )

    return KafkaProducer(**config)


# parse Avro schema
def load_avro_schema(schema_path):
    return avro.schema.parse(open(schema_path).read())


# encode message into avro format
def avro_encode(data, schema):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(data, encoder)
    return bytes_writer.getvalue()
