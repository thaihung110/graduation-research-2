# Crypto Trades Bronze Ingestion Job (Streaming)

PySpark **streaming** application to continuously load crypto trades from Kafka into Iceberg bronze table.

## Overview

This is a **continuous streaming job** that:

1. **Continuously reads** Avro-encoded messages from Kafka topic `market-data.finnhub.crypto-trades.bronze`
2. **Decodes** Avro binary messages to structured data in real-time
3. **Transforms and enriches** the data (extracts exchange, base/quote currency)
4. **Continuously loads** data into Iceberg table `bronze.crypto_trades_raw` on lakekeeper

**Key Feature**: This job runs **continuously** and processes messages as they arrive in Kafka, providing near real-time data ingestion.

## Architecture

### Streaming Data Flow

```
Finnhub Producer (WebSocket)
    ↓ (Real-time trades)
Kafka Topic (market-data.finnhub.crypto-trades.bronze)
    ↓ (Avro binary messages - continuously)
Spark Streaming Job (readStream)
    ↓ (Micro-batches every 5 seconds)
foreachBatch: Decode Avro → Transform → Enrich
    ↓ (Structured DataFrame per batch)
Iceberg Table (bronze.crypto_trades_raw)
    ↓ (Append mode - continuously)
Bronze Layer (Ready for Silver transformation)
```

### Streaming Components

#### 1. **Kafka Streaming Source** (`readStream`)

```python
kafka_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")  # Start from latest offset
    .option("failOnDataLoss", "false")     # Continue on data loss
    .option("maxOffsetsPerTrigger", "10000")  # Max 10k messages per batch
    .load()
)
```

**What it does:**

- Creates a **streaming DataFrame** that continuously reads from Kafka
- Uses **micro-batch processing**: processes data in small batches at regular intervals
- Tracks Kafka offsets automatically to ensure no data loss
- Starts from `latest` offset (only new messages) or `earliest` (all messages)

**Key Options:**

- `startingOffsets`: `"latest"` (new messages only) or `"earliest"` (all messages)
- `maxOffsetsPerTrigger`: Maximum messages per batch (prevents memory overflow)
- `failOnDataLoss`: Whether to fail if data is lost (set to `false` for resilience)

#### 2. **Checkpoint Location**

```python
checkpoint_location = "/tmp/checkpoints/crypto-bronze-{database}-{table}"
```

**What it does:**

- Stores **streaming state** and **Kafka offsets** processed so far
- Enables **fault tolerance**: if job crashes, it resumes from last checkpoint
- Tracks **progress** across restarts
- **Critical**: Must be persistent storage in production (not `/tmp`)

**Checkpoint contains:**

- Kafka offsets for each partition
- Streaming query metadata
- Schema information
- Processing state

**Important**:

- Never delete checkpoint while job is running
- Use persistent volume in Kubernetes
- Each streaming query needs unique checkpoint location

#### 3. **Trigger Configuration**

```python
.trigger(processingTime="5 seconds")
```

**What it does:**

- Defines **how often** to process new data
- `processingTime`: Process data every N seconds (e.g., "5 seconds")
- Alternative: `once=True` (process once and stop) or `continuous` (low-latency)

**Trigger Types:**

- **Processing Time**: `trigger(processingTime="5 seconds")` - Process every 5s
- **Once**: `trigger(once=True)` - Process once and stop
- **Continuous**: `trigger(continuous="1 second")` - Ultra-low latency (experimental)

**For this job**: Uses `processingTime` for balanced throughput and latency.

#### 4. **foreachBatch Processing**

```python
.foreachBatch(lambda batch_df, epoch_id: process_batch(...))
```

**What it does:**

- Processes each **micro-batch** independently
- Allows **custom logic** that can't be done with standard Spark SQL
- Enables **Avro decoding** using RDD operations (not available in pure SQL)

**Why foreachBatch?**

- Avro decoding requires RDD operations (`rdd.map()`)
- Standard Spark SQL streaming doesn't support RDD operations
- `foreachBatch` bridges streaming and batch processing

**Process Flow in foreachBatch:**

1. Receive micro-batch DataFrame from Kafka
2. Convert to RDD for Avro decoding
3. Decode each Avro binary message
4. Convert back to DataFrame
5. Parse JSON and explode trades array
6. Transform and enrich data
7. Write to Iceberg table

#### 5. **Avro Decoding in Streaming**

```python
def decode_avro_row(row):
    """Decode Avro binary message from Kafka row."""
    value_bytes = row.value
    bytes_reader = io.BytesIO(value_bytes)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    decoded = reader.read(decoder)
    return (partition, offset, timestamp, json.dumps(decoded))
```

**What it does:**

- Converts **binary Avro** messages from Kafka to **JSON strings**
- Handles errors gracefully (skips invalid messages)
- Preserves Kafka metadata (partition, offset, timestamp)

**Why RDD?**

- Avro decoding requires Python-level operations
- DataFrame operations don't support binary decoding directly
- RDD provides low-level access to process each row

#### 6. **Data Transformation Pipeline**

Each micro-batch goes through:

1. **Avro Decoding**: Binary → JSON string
2. **JSON Parsing**: JSON string → Structured DataFrame
3. **Explode Trades**: Array of trades → Individual trade rows
4. **Enrichment**: Extract exchange, base_currency, quote_currency from symbol
5. **Timestamp Conversion**: Milliseconds → Timestamp, Date
6. **Metadata Addition**: Add Kafka partition, offset, ingestion timestamp

#### 7. **Iceberg Write (Append Mode)**

```python
final_df.writeTo(table_path).append()
```

**What it does:**

- Appends new records to Iceberg table
- Uses Iceberg's **append** operation (more efficient than INSERT INTO)
- Maintains table schema and partitioning
- Supports **ACID transactions** (all-or-nothing per batch)

**Write Modes:**

- `append()`: Add new records (used in this job)
- `overwrite()`: Replace all data
- `createOrReplace()`: Create or replace table

**Fallback Strategy:**

- If `writeTo().append()` fails, falls back to `INSERT INTO`
- Ensures data is written even if Iceberg API has issues

## Files

- `load_crypto_bronze.py` - Main PySpark streaming application
- `Dockerfile` - Docker image definition
- `build-image.sh` - Script to build and push Docker image to Docker Hub

**Docker Image**: `hungvt0110/load-crypto-bronze:latest`

## Input Schema (Kafka Avro Messages)

Messages in Kafka are Avro-encoded with this schema:

```json
{
  "type": "record",
  "name": "CryptoTradeMessage",
  "namespace": "com.finnhub.crypto",
  "fields": [
    {
      "name": "message_type",
      "type": "string"
    },
    {
      "name": "trades",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Trade",
          "fields": [
            { "name": "symbol", "type": "string" },
            { "name": "exchange", "type": "string" },
            { "name": "base_currency", "type": "string" },
            { "name": "quote_currency", "type": "string" },
            { "name": "price", "type": "double" },
            { "name": "volume", "type": "double" },
            { "name": "timestamp_ms", "type": "long" },
            {
              "name": "conditions",
              "type": ["null", { "type": "array", "items": "string" }],
              "default": null
            },
            { "name": "ingestion_timestamp_ms", "type": "long" }
          ]
        }
      }
    },
    {
      "name": "producer_metadata",
      "type": {
        "type": "record",
        "name": "ProducerMetadata",
        "fields": [
          { "name": "producer_id", "type": "string" },
          { "name": "kafka_topic", "type": "string" },
          { "name": "schema_version", "type": "string" }
        ]
      }
    }
  ]
}
```

## Output Schema (Iceberg Table)

| Column                       | Type          | Description                              |
| ---------------------------- | ------------- | ---------------------------------------- |
| `symbol`                     | string        | Full symbol (e.g., BINANCE:BTCUSDT)      |
| `exchange`                   | string        | Exchange name (e.g., BINANCE)            |
| `base_currency`              | string        | Base currency (e.g., BTC)                |
| `quote_currency`             | string        | Quote currency (e.g., USDT)              |
| `price`                      | double        | Trade price                              |
| `volume`                     | double        | Trade volume                             |
| `trade_datetime`             | timestamp     | Trade timestamp (converted from ms)      |
| `trade_date`                 | date          | Trade date (partition column)            |
| `timestamp_ms`               | bigint        | Original timestamp in milliseconds       |
| `conditions`                 | array<string> | Trade conditions (optional)              |
| `ingestion_timestamp_ms`     | bigint        | Timestamp when producer received message |
| `message_type`               | string        | Message type (always "trade")            |
| `producer_id`                | string        | Producer instance identifier             |
| `kafka_topic`                | string        | Kafka topic name                         |
| `schema_version`             | string        | Schema version                           |
| `kafka_partition`            | int           | Kafka partition number                   |
| `kafka_offset`               | bigint        | Kafka offset (for deduplication)         |
| `bronze_ingestion_timestamp` | timestamp     | When record was ingested to Bronze layer |

**Partitioning**: `trade_date`, `exchange`

**Table Properties:**

- `format-version`: `2` (Iceberg format version)
- Partitioned by date and exchange for efficient queries

## Configuration

### Environment Variables

| Variable                  | Default                                                                     | Description                                            |
| ------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------ |
| `SPARK_MINOR_VERSION`     | `3.5`                                                                       | Spark version                                          |
| `ICEBERG_VERSION`         | `1.5.2`                                                                     | Iceberg version                                        |
| `CATALOG_URL`             | `http://openhouse-lakekeeper:8181/catalog`                                  | Lakekeeper catalog URL                                 |
| `CLIENT_ID`               | `spark`                                                                     | OAuth2 client ID for lakekeeper                        |
| `CLIENT_SECRET`           | `7PwnKrR0cRhh9PKaQRPRx2KGQBgmOUxa`                                          | OAuth2 client secret                                   |
| `WAREHOUSE`               | `bronze`                                                                    | Warehouse name in lakekeeper                           |
| `KEYCLOAK_TOKEN_ENDPOINT` | `http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token` | Keycloak OAuth2 endpoint                               |
| `KAFKA_BOOTSTRAP_SERVERS` | `openhouse-kafka:9092`                                                      | Kafka bootstrap servers                                |
| `KAFKA_TOPIC`             | `market-data.finnhub.crypto-trades.bronze`                                  | Kafka topic name                                       |
| `CHECKPOINT_LOCATION`     | `/tmp/checkpoints/crypto-bronze-{database}-{table}`                         | **Streaming checkpoint location** (must be persistent) |
| `TRIGGER_INTERVAL`        | `5 seconds`                                                                 | **How often to process batches**                       |

### Command Line Arguments

```bash
spark-submit load_crypto_bronze.py [kafka_topic] [database] [table]
```

- `kafka_topic` (optional): Kafka topic name (default: from env `KAFKA_TOPIC`)
- `database` (optional): Target database/namespace (default: `bronze`)
- `table` (optional): Target table name (default: `crypto_trades_raw`)

### Streaming-Specific Configuration

#### Checkpoint Location

**Critical for production:**

- Must be on **persistent storage** (not `/tmp` which is ephemeral)
- Use Kubernetes PersistentVolume or S3/MinIO path
- Format: `s3a://bucket/checkpoints/crypto-bronze` or `/mnt/persistent/checkpoints`

**Example:**

```bash
export CHECKPOINT_LOCATION="s3a://data-platform/checkpoints/crypto-bronze-bronze-crypto_trades_raw"
```

#### Trigger Interval

Controls how often to process new data:

```bash
# Process every 10 seconds (lower latency, more overhead)
export TRIGGER_INTERVAL="10 seconds"

# Process every 60 seconds (higher latency, less overhead)
export TRIGGER_INTERVAL="60 seconds"
```

**Trade-offs:**

- **Shorter interval**: Lower latency, but more overhead (more frequent commits)
- **Longer interval**: Higher latency, but less overhead (fewer commits)

#### Max Offsets Per Trigger

Limits messages processed per batch (prevents memory issues):

```python
.option("maxOffsetsPerTrigger", "10000")  # Default: 10,000
```

**Adjust based on:**

- Message size
- Available memory
- Processing speed

## Build and Push Docker Image

### Prerequisites

1. **Docker installed** and running
2. **Docker Hub account** (username: `hungvt0110`)
3. **Logged in to Docker Hub:**
   ```bash
   docker login -u hungvt0110
   ```

### Build and Push

```bash
cd spark-jobs/load-crypto-bronze
./build-image.sh
```

This will:

- Build image as `hungvt0110/load-crypto-bronze:latest`
- Push to Docker Hub automatically

### Custom Options

```bash
# Custom tag
IMAGE_TAG=v1.0.0 ./build-image.sh

# Build without pushing
PUSH_TO_DOCKERHUB=false ./build-image.sh

# Different username
DOCKERHUB_USERNAME=your-username ./build-image.sh
```

## Kubernetes Deployment

### Important: Streaming Job Considerations

**This is a long-running job** that:

- Runs **continuously** (doesn't exit on completion)
- Requires **persistent checkpoint storage**
- Should **not restart** on completion (unlike batch jobs)

### Step 1: Create Persistent Volume for Checkpoint

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: crypto-bronze-checkpoint
  namespace: default
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: standard # Adjust to your cluster
```

Apply:

```bash
kubectl apply -f crypto-bronze-checkpoint-pvc.yaml
```

### Step 2: Build and Push Image

```bash
cd spark-jobs/load-crypto-bronze
./build-image.sh
```

### Step 3: Create SparkApplication Manifest

The manifest should be configured for **long-running streaming job**:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: load-crypto-bronze
  namespace: default
  labels:
    app: load-crypto-bronze
    component: etl
    layer: bronze
spec:
  type: Python
  mode: cluster
  pythonVersion: "3"
  sparkVersion: "3.5.0"

  # Spark image with Python support and application code
  image: hungvt0110/load-crypto-bronze:latest
  imagePullPolicy: Always

  # Main Python application file (inside the image)
  mainApplicationFile: local:///app/load_crypto_bronze.py

  # Arguments to pass to the Python script
  arguments:
    - "market-data.finnhub.crypto-trades.bronze" # Kafka topic
    - "bronze" # Database/namespace
    - "crypto_trades_raw" # Table name

  # Spark configuration
  sparkConf:
    # JARs packages - must be set before SparkSession creation
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4"
    # Ivy cache to /tmp (always writable in pods)
    "spark.jars.ivy": "/tmp/.ivy2"
    # Kafka consumer config
    "spark.sql.streaming.kafka.useDeprecatedOffsetFetching": "false"

  # Hadoop configuration for MinIO (S3-compatible)
  hadoopConf:
    "fs.s3a.endpoint": "http://openhouse-minio:9000"
    "fs.s3a.path.style.access": "true"
    "fs.s3a.connection.ssl.enabled": "false"
    "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

  # Driver configuration
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "2g"
    memoryOverhead: "512m"
    labels:
      version: 3.5.0
    serviceAccount: openhouse-spark-operator-spark
    env:
      - name: HOME
        value: "/tmp"
      - name: SPARK_MINOR_VERSION
        value: "3.5"
      - name: ICEBERG_VERSION
        value: "1.5.2"
      - name: CATALOG_URL
        value: "http://openhouse-lakekeeper:8181/catalog"
      - name: CLIENT_ID
        value: "spark"
      - name: CLIENT_SECRET
        value: "7PwnKrR0cRhh9PKaQRPRx2KGQBgmOUxa"
      - name: WAREHOUSE
        value: "bronze"
      - name: KEYCLOAK_TOKEN_ENDPOINT
        value: "http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token"
      - name: AWS_ACCESS_KEY_ID
        value: "admin"
      - name: AWS_SECRET_ACCESS_KEY
        value: "admin123"
      - name: KAFKA_BOOTSTRAP_SERVERS
        value: "openhouse-kafka:9092"
      - name: KAFKA_TOPIC
        value: "market-data.finnhub.crypto-trades.bronze"
      # Streaming-specific configs
      - name: CHECKPOINT_LOCATION
        value: "s3a://data-platform/checkpoints/crypto-bronze-bronze-crypto_trades_raw"
        # Or use PVC: value: "/mnt/checkpoint" (mount PVC)
      - name: TRIGGER_INTERVAL
        value: "5 seconds"

  # Executor configuration
  executor:
    cores: 2
    instances: 2
    memory: "4g"
    memoryOverhead: "1g"
    labels:
      version: 3.5.0
    env:
      - name: HOME
        value: "/tmp"
      - name: SPARK_MINOR_VERSION
        value: "3.5"
      - name: ICEBERG_VERSION
        value: "1.5.2"
      - name: CATALOG_URL
        value: "http://openhouse-lakekeeper:8181/catalog"
      - name: CLIENT_ID
        value: "spark"
      - name: CLIENT_SECRET
        value: "7PwnKrR0cRhh9PKaQRPRx2KGQBgmOUxa"
      - name: WAREHOUSE
        value: "bronze"
      - name: KEYCLOAK_TOKEN_ENDPOINT
        value: "http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token"
      - name: AWS_ACCESS_KEY_ID
        value: "admin"
      - name: AWS_SECRET_ACCESS_KEY
        value: "admin123"

  # Restart policy for streaming job
  restartPolicy:
    type: Always # Restart if job fails (important for streaming)
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 3
    onSubmissionFailureRetryInterval: 20

  # Time to live (how long to keep completed pods)
  timeToLiveSeconds: 3600
```

**Key differences from batch jobs:**

- `restartPolicy.type: Always` - Restart on failure (streaming should be resilient)
- `CHECKPOINT_LOCATION` - Must be persistent (S3 or PVC)
- Job runs continuously (doesn't complete)

### Step 4: Deploy

```bash
kubectl apply -f infra/k8s/compute/applications/spark/bronze-layer/jobs/load-crypto-bronze.yaml
```

### Step 5: Monitor

```bash
# Check status (should show RUNNING)
kubectl get sparkapplication load-crypto-bronze -n default

# View logs (streaming continuously)
kubectl logs -l spark-role=driver -n default -f

# Check checkpoint location
kubectl exec -it <driver-pod> -- ls -la /tmp/checkpoints/

# Describe for details
kubectl describe sparkapplication load-crypto-bronze -n default
```

**Expected behavior:**

- Job status: `RUNNING` (not `COMPLETED`)
- Logs show: "🔄 Streaming job is running..."
- Logs show periodic batch processing: "🔄 Processing batch X..."
- Checkpoint files are created/updated

## How Streaming Works

### Micro-Batch Processing

1. **Every 5 seconds** (or configured interval):

   - Spark reads new messages from Kafka (up to `maxOffsetsPerTrigger`)
   - Creates a micro-batch DataFrame

2. **foreachBatch callback**:

   - Receives the micro-batch
   - Decodes Avro messages
   - Transforms and enriches data
   - Writes to Iceberg table

3. **Checkpoint update**:

   - Saves Kafka offsets processed
   - Updates streaming state
   - Enables recovery on restart

4. **Repeat**: Process continues indefinitely

### Fault Tolerance

**If job crashes:**

1. Spark Operator restarts the job (if `restartPolicy: Always`)
2. Job reads checkpoint location
3. Resumes from last processed Kafka offset
4. No data loss (at-least-once semantics)

**Checkpoint contains:**

- Kafka offsets for each partition
- Last processed batch ID
- Schema information

### At-Least-Once Semantics

- **Guarantee**: Each message is processed **at least once**
- **Possible**: Messages may be processed **multiple times** (on restart)
- **Solution**: Use `kafka_partition` + `kafka_offset` for deduplication in downstream jobs
