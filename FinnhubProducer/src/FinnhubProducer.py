# Main file for Finnhub API & Kafka integration
import ast
import json
import os
import re
import sys
import time
import traceback

import websocket
from utils.functions import *
from utils.symbol_parser import SymbolParser


# proper class that ingests upcoming messages from Finnhub websocket into Kafka
class FinnhubProducer:
    def __init__(self):
        print("=" * 50)
        print("FinnhubProducer Starting...")
        print("=" * 50)

        print("\nEnvironment Variables:")
        for k, v in os.environ.items():
            if "TOKEN" in k or "PASSWORD" in k:
                print(f"{k}=***")
            else:
                print(f"{k}={v}")

        print("\nInitializing components...")
        self.finnhub_client = load_client(os.environ["FINNHUB_API_TOKEN"])
        print("✅ Finnhub client initialized")

        kafka_server = (
            f"{os.environ['KAFKA_SERVER']}:{os.environ['KAFKA_PORT']}"
        )
        print(f"Connecting to Kafka: {kafka_server}")

        # Get SASL credentials (optional)
        sasl_username = os.environ.get("KAFKA_SASL_USERNAME")
        sasl_password = os.environ.get("KAFKA_SASL_PASSWORD")

        self.producer = load_producer(
            kafka_server, sasl_username, sasl_password
        )
        print("✅ Kafka producer initialized")

        self.avro_schema = load_avro_schema("src/schemas/crypto_trades.avsc")
        print("✅ Avro schema loaded (crypto_trades.avsc)")

        self.tickers = ast.literal_eval(os.environ["FINNHUB_STOCKS_TICKERS"])
        self.validate = os.environ["FINNHUB_VALIDATE_TICKERS"]
        print(f"✅ Tickers configured: {self.tickers}")
        print(f"✅ Validation enabled: {self.validate}")

        # Reconnection logic with rate limit handling
        self.reconnect_count = 0
        self.max_reconnect = 3  # Reduced to avoid rate limits
        self.is_connected = False
        self.rate_limited = False
        self.rate_limit_reset_time = None

        self.connect_websocket()

    def connect_websocket(self):
        print("\nConnecting to Finnhub WebSocket...")
        websocket.enableTrace(False)  # Disable verbose tracing for cleaner logs
        self.ws = websocket.WebSocketApp(
            f'wss://ws.finnhub.io?token={os.environ["FINNHUB_API_TOKEN"]}',
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        print("Starting WebSocket connection...")
        try:
            self.ws.run_forever()
        except KeyboardInterrupt:
            print("\n👋 Shutting down FinnhubProducer gracefully...")
            self.ws.close()
            sys.exit(0)
        except Exception as e:
            print(f"\n❌ Fatal error in WebSocket connection: {e}")
            traceback.print_exc()
            sys.exit(1)

    def on_message(self, ws, message):
        try:
            message_data = json.loads(message)
            msg_type = message_data.get("type", "unknown")

            print(f"\n📨 Received message type: {msg_type}")
            print(
                f"   Full message: {json.dumps(message_data, indent=2)}"
            )  # Log full message for debugging

            # Check for error messages from Finnhub
            if msg_type == "error":
                print(
                    f"❌ Error from Finnhub: {message_data.get('msg', 'Unknown error')}"
                )
                return

            # Skip ping messages (keep-alive)
            if msg_type == "ping":
                print("⏭️  Skipping ping message")
                return

            # Skip messages without a 'data' field
            if "data" not in message_data:
                print(f"⏭️  Skipping message without data field: {msg_type}")
                return

            # Only process trade messages with data field
            if message_data["data"]:
                trade_count = len(message_data["data"])
                print(f"📊 Processing {trade_count} trade(s)")

                # Show first trade for debugging
                if trade_count > 0:
                    first_trade = message_data["data"][0]
                    print(
                        f"   Sample: {first_trade.get('s')} @ ${first_trade.get('p')} vol:{first_trade.get('v')}"
                    )

                # Transform raw trades to enriched format matching crypto_trades.avsc
                ingestion_timestamp_ms = int(time.time() * 1000)
                enriched_trades = []

                for raw_trade in message_data["data"]:
                    symbol = raw_trade.get("s", "")
                    parsed = SymbolParser.parse_symbol(symbol)

                    # Build enriched trade record
                    enriched_trade = {
                        "symbol": symbol,
                        "exchange": parsed.get("exchange") or "",
                        "base_currency": parsed.get("base_currency") or "",
                        "quote_currency": parsed.get("quote_currency") or "",
                        "price": raw_trade.get("p", 0.0),
                        "volume": raw_trade.get("v", 0.0),
                        "timestamp_ms": raw_trade.get("t", 0),
                        "conditions": raw_trade.get(
                            "c"
                        ),  # Can be None or array
                        "ingestion_timestamp_ms": ingestion_timestamp_ms,
                    }
                    enriched_trades.append(enriched_trade)

                # Build message matching crypto_trades.avsc schema
                topic = os.environ["KAFKA_TOPIC_NAME"]
                avro_message_data = {
                    "message_type": message_data["type"],
                    "trades": enriched_trades,
                    "producer_metadata": {
                        "producer_id": os.getenv(
                            "HOSTNAME", "finnhub-producer"
                        ),
                        "kafka_topic": topic,
                        "schema_version": "1.0",
                    },
                }

                avro_message = avro_encode(avro_message_data, self.avro_schema)

                future = self.producer.send(topic, avro_message)

                # Wait for send to complete and check result
                try:
                    record_metadata = future.get(timeout=10)
                    print(
                        f"✅ Sent {trade_count} trade(s) to Kafka topic '{topic}'"
                    )
                    print(
                        f"   Partition: {record_metadata.partition}, Offset: {record_metadata.offset}"
                    )
                except Exception as send_error:
                    print(f"❌ Error sending to Kafka: {send_error}")
            else:
                print(f"⚠️  Received message with empty data: {msg_type}")

        except KeyError as e:
            print(f"❌ Error: Missing field in message - {e}")
            print(f"   Message received: {message}")
            traceback.print_exc()
        except json.JSONDecodeError as e:
            print(f"❌ Error: Failed to parse JSON message - {e}")
            print(f"   Raw message: {message}")
            traceback.print_exc()
        except Exception as e:
            print(f"❌ Error processing message: {e}")
            print(f"   Message: {message}")
            traceback.print_exc()

    def on_error(self, ws, error):
        print(f"\n❌ WebSocket error: {error}")
        error_str = str(error)

        # Check for rate limit error (429)
        if (
            "429" in error_str
            or "Too Many Requests" in error_str
            or "API limit reached" in error_str
        ):
            self.rate_limited = True

            print("\n" + "=" * 70)
            print("⚠️  RATE LIMIT EXCEEDED - Finnhub API Limit Reached")
            print("=" * 70)
            print("\n📊 Finnhub Free Tier Limits:")
            print("   - WebSocket connections: 1")
            print("   - Connection attempts per minute: 5")
            print("   - Rate limit window: 60 seconds")
            print("\n🔍 Possible causes:")
            print("   1. Multiple producer instances running")
            print("   2. Pod crash-loop with rapid restarts")
            print("   3. Previous connections not fully closed")

            # Extract rate limit reset time
            reset_time = None
            match = re.search(r"'x-ratelimit-reset': '(\d+)'", error_str)
            if match:
                reset_time = int(match.group(1))
                self.rate_limit_reset_time = reset_time
                current_time = int(time.time())
                wait_seconds = max(
                    reset_time - current_time + 10, 60
                )  # Add 10s buffer

                print(f"\n⏰ Rate limit info:")
                print(f"   - Current time: {time.ctime(current_time)}")
                print(f"   - Reset time: {time.ctime(reset_time)}")
                print(
                    f"   - Wait duration: {wait_seconds} seconds ({wait_seconds/60:.1f} minutes)"
                )
            else:
                # Default wait if we can't extract reset time
                wait_seconds = 300  # 5 minutes
                print(
                    f"\n⏳ Could not extract reset time, using default: {wait_seconds}s"
                )

            print(f"\n💡 Recovery actions:")
            print(
                f"   1. Waiting {wait_seconds} seconds for rate limit to reset"
            )
            print(f"   2. Will attempt to reconnect after wait period")
            print(
                f"   3. If issue persists, scale deployment to 0 and wait 5 minutes"
            )
            print("\n💎 To avoid this issue:")
            print("   - Ensure only ONE producer instance is running")
            print("   - Use the restart script: restart_finnhub_producer.sh")
            print("   - Consider upgrading to Finnhub Premium tier")
            print("=" * 70 + "\n")

            # Wait for rate limit to reset
            print(f"⏳ Sleeping for {wait_seconds} seconds...")
            time.sleep(wait_seconds)
            print("✅ Wait period completed, rate limit should be reset")
            self.rate_limited = False
            return
        else:
            # Other errors
            traceback.print_exc()

        self.is_connected = False

    def on_close(self, ws, close_status_code, close_msg):
        print(f"\n🔌 WebSocket connection closed")
        print(f"   Status code: {close_status_code}")
        print(f"   Message: {close_msg}")
        self.is_connected = False

        # If we were rate limited, don't attempt immediate reconnection
        if self.rate_limited:
            print(
                "⚠️  Connection closed due to rate limit. Already handled in on_error."
            )
            return

        # Attempt to reconnect with exponential backoff
        if self.reconnect_count < self.max_reconnect:
            self.reconnect_count += 1
            # Exponential backoff: 60s, 120s, 240s (max 5 min to avoid rate limits)
            wait_time = min(60 * (2 ** (self.reconnect_count - 1)), 300)

            print(
                f"\n🔄 Reconnection attempt {self.reconnect_count}/{self.max_reconnect}"
            )
            print(f"⏳ Waiting {wait_time} seconds before reconnecting...")
            print(f"💡 Using exponential backoff to avoid rate limits")

            time.sleep(wait_time)
            print(f"🚀 Attempting reconnection...")
            self.connect_websocket()  # Re-initiate connection
        else:
            print(
                f"\n❌ Max reconnect attempts ({self.max_reconnect}) reached."
            )
            print(f"💡 Kubernetes will restart the pod with backoff policy.")
            print(f"   This helps avoid rate limit issues.")
            sys.exit(1)

    def on_open(self, ws):
        self.is_connected = True
        self.reconnect_count = (
            0  # Reset reconnect counter on successful connection
        )
        self.rate_limited = False  # Reset rate limit flag

        print("\n🔌 WebSocket connection opened!")
        print("=" * 50)
        print("Subscribing to tickers...")

        for ticker in self.tickers:
            print(f"\n📡 Processing ticker: {ticker}")

            if self.validate == "1":
                print(f"   Validating ticker...")
                try:
                    if ticker_validator(self.finnhub_client, ticker):
                        subscribe_msg = json.dumps(
                            {"type": "subscribe", "symbol": ticker}
                        )
                        print(
                            f"   ✅ Ticker validated, sending subscription: {subscribe_msg}"
                        )
                        self.ws.send(subscribe_msg)
                        print(f"   ✅ Subscription request sent for {ticker}")
                    else:
                        print(
                            f"   ❌ Subscription for {ticker} failed - ticker not found"
                        )
                except Exception as e:
                    print(f"   ❌ Error validating ticker {ticker}: {e}")
                    traceback.print_exc()
            else:
                subscribe_msg = json.dumps(
                    {"type": "subscribe", "symbol": ticker}
                )
                print(
                    f"   Sending subscription (validation skipped): {subscribe_msg}"
                )
                self.ws.send(subscribe_msg)
                print(f"   ✅ Subscription request sent for {ticker}")

        print("\n✅ All subscription requests sent")
        print("⏳ Waiting for trade data from Finnhub...")
        print(
            "💡 Note: Crypto trades may be infrequent. You will see trades when they occur."
        )
        print("=" * 50 + "\n")


if __name__ == "__main__":
    try:
        FinnhubProducer()
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)
