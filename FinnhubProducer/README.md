# FinnhubProducer

Real-time crypto trading data producer that streams data from Finnhub WebSocket API to Kafka.

## Overview

FinnhubProducer connects to Finnhub's WebSocket API to receive real-time crypto trade data and publishes it to a Kafka topic in Avro format.

**Data Flow**:

```
Finnhub WebSocket API → FinnhubProducer → Kafka Topic (Avro)
```

## Features

- ✅ Real-time WebSocket connection to Finnhub API
- ✅ Crypto trade data streaming (BTC, ETH, etc.)
- ✅ Avro message encoding
- ✅ Kafka producer with SASL authentication
- ✅ Automatic reconnection on failures
- ✅ Environment variable configuration
- ✅ Docker containerization

## Directory Structure

```
FinnhubProducer/
├── src/
│   ├── FinnhubProducer.py          # Main producer application
│   ├── config/
│   │   └── avro_schema.avsc        # Avro schema for trade messages
│   └── utils/
│       └── functions.py            # Helper functions (Kafka, Avro)
├── Dockerfile                      # Docker image definition
├── build_and_push.sh              # Build and push script
├── requirements.txt               # Python dependencies
└── README.md
```

## Environment Variables

### Required

| Variable            | Description           | Example                                    |
| ------------------- | --------------------- | ------------------------------------------ |
| `FINNHUB_API_TOKEN` | Finnhub API token     | `your-api-token`                           |
| `KAFKA_SERVER`      | Kafka broker hostname | `openhouse-kafka`                          |
| `KAFKA_PORT`        | Kafka broker port     | `9092`                                     |
| `KAFKA_TOPIC_NAME`  | Target Kafka topic    | `market-data.finnhub.crypto-trades.bronze` |

### Optional

| Variable                   | Description                    | Default                           |
| -------------------------- | ------------------------------ | --------------------------------- |
| `FINNHUB_STOCKS_TICKERS`   | Comma-separated crypto symbols | `BINANCE:BTCUSDT,BINANCE:ETHUSDT` |
| `FINNHUB_VALIDATE_TICKERS` | Validate ticker symbols        | `false`                           |
| `KAFKA_SASL_USERNAME`      | SASL username for Kafka        | -                                 |
| `KAFKA_SASL_PASSWORD`      | SASL password for Kafka        | -                                 |

## Avro Schema

Trade messages are encoded using the following Avro schema:

```json
{
  "type": "record",
  "name": "Trade",
  "namespace": "com.finnhub.trade",
  "fields": [
    { "name": "symbol", "type": "string" },
    { "name": "price", "type": "double" },
    { "name": "volume", "type": "double" },
    { "name": "timestamp", "type": "long" },
    { "name": "conditions", "type": { "type": "array", "items": "string" } },
    { "name": "exchange", "type": "string" }
  ]
}
```

## Local Development

### Prerequisites

- Python 3.9+
- Kafka cluster (local or remote)
- Finnhub API token ([Get free token](https://finnhub.io/register))

### Setup

1. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

2. **Set environment variables**:

   ```bash
   export FINNHUB_API_TOKEN="your-token"
   export KAFKA_SERVER="localhost"
   export KAFKA_PORT="9092"
   export KAFKA_TOPIC_NAME="crypto-trades"
   export FINNHUB_STOCKS_TICKERS="BINANCE:BTCUSDT,BINANCE:ETHUSDT"
   ```

3. **Run producer**:
   ```bash
   python src/FinnhubProducer.py
   ```

### Expected Output

```
Connected to Finnhub WebSocket
Subscribed to: BINANCE:BTCUSDT
Subscribed to: BINANCE:ETHUSDT
Message sent to Kafka: {'symbol': 'BINANCE:BTCUSDT', 'price': 42000.5, ...}
Message sent to Kafka: {'symbol': 'BINANCE:ETHUSDT', 'price': 2250.3, ...}
```

## Docker Deployment

### Build Image

```bash
# Build locally
docker build -t finnhub-producer:latest .

# Or use build script
chmod +x build_and_push.sh
./build_and_push.sh
```

The build script will:

1. Build Docker image
2. Tag as `hungvt0110/finnhub-producer:latest`
3. Push to Docker Hub

### Run Container

```bash
docker run -d \
  --name finnhub-producer \
  -e FINNHUB_API_TOKEN="your-token" \
  -e KAFKA_SERVER="kafka-host" \
  -e KAFKA_PORT="9092" \
  -e KAFKA_TOPIC_NAME="crypto-trades" \
  -e FINNHUB_STOCKS_TICKERS="BINANCE:BTCUSDT" \
  -e KAFKA_SASL_USERNAME="admin" \
  -e KAFKA_SASL_PASSWORD="admin" \
  hungvt0110/finnhub-producer:latest
```

### View Logs

```bash
docker logs -f finnhub-producer
```

## Kubernetes Deployment

See [infra/k8s/ingestion/README.md](../infra/k8s/ingestion/README.md) for Kubernetes deployment instructions.

**Quick deploy**:

```bash
cd infra/k8s/ingestion/scripts
./deploy_finnhub_producer.sh
```

## Configuration Examples

### Multiple Crypto Pairs

```bash
export FINNHUB_STOCKS_TICKERS="BINANCE:BTCUSDT,BINANCE:ETHUSDT,BINANCE:SOLUSDT,BINANCE:ADAUSDT"
```

### With SASL Authentication

```bash
export KAFKA_SASL_USERNAME="admin"
export KAFKA_SASL_PASSWORD="admin"
```

### Custom Kafka Topic

```bash
export KAFKA_TOPIC_NAME="market-data.crypto.trades.raw"
```

## Monitoring

### Check Kafka Messages

```bash
# Using kafka-console-consumer
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic market-data.finnhub.crypto-trades.bronze \
  --from-beginning \
  --max-messages 10
```

### Verify Avro Encoding

Messages are Avro-encoded. To decode:

```python
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer

consumer = AvroConsumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-consumer',
    'schema.registry.url': 'http://localhost:8081'
})

consumer.subscribe(['crypto-trades'])
msg = consumer.poll(1.0)
print(msg.value())  # Decoded Avro message
```

## Troubleshooting

### WebSocket Connection Failed

**Error**: `Failed to connect to Finnhub WebSocket`

**Solutions**:

- Verify API token is valid
- Check internet connectivity
- Ensure Finnhub API is not rate-limited

### Kafka Connection Refused

**Error**: `KafkaError: Connection refused`

**Solutions**:

- Verify Kafka broker is running
- Check `KAFKA_SERVER` and `KAFKA_PORT` are correct
- Test connectivity: `telnet kafka-host 9092`

### SASL Authentication Failed

**Error**: `SASL authentication failed`

**Solutions**:

- Verify `KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD`
- Check Kafka broker SASL configuration
- Ensure SASL mechanism is `PLAIN`

### No Messages in Kafka

**Check**:

1. Producer logs show "Message sent to Kafka"
2. Kafka topic exists:
   ```bash
   kafka-topics.sh --list --bootstrap-server localhost:9092
   ```
3. Consumer can read from topic (see Monitoring section)

## Dependencies

```
websocket-client==1.6.1
kafka-python==2.0.2
avro-python3==1.10.2
```

## API Rate Limits

Finnhub free tier limits:

- **60 API calls/minute**
- **30 WebSocket connections**

For production, consider upgrading to a paid plan.

## References

- [Finnhub API Documentation](https://finnhub.io/docs/api)
- [Finnhub WebSocket Trades](https://finnhub.io/docs/api/websocket-trades)
- [Kafka Python Client](https://kafka-python.readthedocs.io/)
- [Apache Avro](https://avro.apache.org/docs/current/)
