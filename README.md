# Crypto Data Lakehouse Platform

Real-time crypto trading data platform built on Kubernetes with Apache Iceberg, Spark, and Kafka.

## Overview

End-to-end data platform for ingesting, processing, and analyzing crypto trading data using modern data lakehouse architecture.

**Data Flow**:

```
Finnhub API → FinnhubProducer → Kafka → Spark Streaming → Bronze (Iceberg)
                                                              ↓
                                                       Spark Batch → Silver (Iceberg)
```

## Architecture

### Components

| Component         | Purpose                          | Technology                           |
| ----------------- | -------------------------------- | ------------------------------------ |
| **Ingestion**     | Real-time crypto data collection | FinnhubProducer (Python + WebSocket) |
| **Messaging**     | Event streaming                  | Apache Kafka                         |
| **Processing**    | Stream & batch processing        | Apache Spark on Kubernetes           |
| **Storage**       | Data lakehouse                   | Apache Iceberg + MinIO (S3)          |
| **Catalog**       | Metadata management              | Lakekeeper (Iceberg REST Catalog)    |
| **Orchestration** | Workflow scheduling              | Apache Airflow                       |
| **Auth**          | Identity & access                | Keycloak (OAuth2)                    |

### Data Layers

1. **Bronze Layer**: Raw crypto trades from Kafka (streaming ingestion)
2. **Silver Layer**: Cleaned and aggregated OHLCV data (batch processing)

## Project Structure

```
.
├── FinnhubProducer/                    # Real-time crypto data producer
│   ├── src/
│   │   ├── FinnhubProducer.py         # Main WebSocket producer
│   │   ├── schemas/                    # Avro schemas
│   │   └── utils/                      # Helper functions
│   ├── Dockerfile
│   └── README.md
│
├── spark-jobs/                         # Spark applications
│   ├── load-crypto-bronze/            # Streaming: Kafka → Bronze Iceberg
│   │   ├── src/
│   │   │   ├── main.py
│   │   │   ├── config.py
│   │   │   ├── etl/                   # Extract, Transform, Load
│   │   │   └── utils/                 # Avro decoder, schemas
│   │   ├── Dockerfile
│   │   └── README.md
│   │
│   └── transform-crypto-silver-batch/ # Batch: Bronze → Silver OHLCV
│       ├── src/
│       │   ├── main.py
│       │   ├── config.py              # Dual catalog config
│       │   ├── etl/                   # OHLCV aggregation logic
│       │   └── utils/                 # Silver table schemas
│       ├── Dockerfile
│       └── README.md
│
├── airflow-dags-deployment/           # Airflow DAGs
│   ├── dags/
│   │   └── crypto_ohlcv_silver_batch_dag.py  # Daily OHLCV aggregation
│   └── README.md
│
└── infra/k8s/                         # Kubernetes infrastructure
    ├── storage/                        # Storage layer (MinIO, Lakekeeper, Keycloak)
    │   ├── config/
    │   ├── scripts/
    │   └── README.md
    │
    ├── orchestration/                  # Kafka, NiFi, Airflow
    │   ├── config/
    │   ├── scripts/
    │   └── README.md
    │
    ├── compute/                        # Spark Operator
    │   ├── applications/spark/
    │   │   ├── bronze-layer/jobs/
    │   │   └── silver-layer/jobs/
    │   ├── scripts/
    │   └── README.md
    │
    └── ingestion/                      # FinnhubProducer deployment
        ├── application/
        ├── scripts/
        └── README.md
```

## Quick Start

### Prerequisites

- Kubernetes cluster (local or cloud)
- kubectl configured
- Helm 3+
- Docker

### 1. Deploy Storage Layer

```bash
cd infra/k8s/storage/scripts

# Install MinIO (S3-compatible storage)
./install_minio.sh

# Install Lakekeeper (Iceberg catalog)
./install_lakekeeper.sh

# Install Keycloak (OAuth2 provider)
./install_keycloak.sh
```

### 2. Deploy Orchestration Layer

```bash
cd infra/k8s/orchestration/scripts

# Install Kafka
kubectl apply -f ../config/kafka.yaml

# Install Airflow
./install_airflow.sh
```

### 3. Deploy Compute Layer

```bash
cd infra/k8s/compute/scripts

# Install Spark Operator
./install_spark_operators.sh
```

### 4. Deploy Ingestion

```bash
cd infra/k8s/ingestion/scripts

# Deploy FinnhubProducer
./deploy_finnhub_producer.sh
```

### 5. Start Spark Jobs

```bash
cd infra/k8s/compute/scripts

# Start Bronze streaming job (Kafka → Iceberg)
./start_load_crypto_bronze.sh

# Silver batch job runs via Airflow (daily at 2 AM)
# Or manually:
./start_transform_crypto_silver_batch.sh
```

## Data Pipeline

### Bronze Layer (Streaming)

**Job**: `load-crypto-bronze`

- **Source**: Kafka topic `market-data.finnhub.crypto-trades.bronze`
- **Processing**: Avro decoding, schema validation, timestamp conversion
- **Target**: Iceberg table `bronze.crypto_trades_raw`
- **Partitioning**: By `trade_date` and `exchange`
- **Trigger**: 10-second micro-batches

### Silver Layer (Batch)

**Job**: `transform-crypto-silver-batch`

- **Source**: Bronze Iceberg table
- **Processing**: 1-hour OHLCV aggregation, VWAP calculation
- **Target**: Iceberg table `silver.crypto_ohlcv_1h`
- **Partitioning**: By `agg_date` and `exchange`
- **Schedule**: Daily at 2 AM via Airflow

## Key Features

### Real-time Ingestion

- WebSocket connection to Finnhub API
- Avro message encoding
- Kafka SASL authentication
- Auto-reconnection

### Streaming Processing

- Spark Structured Streaming
- Checkpoint-based recovery
- Exactly-once semantics (Kafka offsets)
- Schema evolution support

### Batch Processing

- Dual Iceberg catalog (Bronze + Silver)
- Window-based OHLCV aggregation
- Date range parameterization
- Airflow orchestration

### Data Lakehouse

- Apache Iceberg tables
- Time travel queries
- Schema evolution
- ACID transactions
- S3-compatible storage (MinIO)

### Security

- OAuth2 authentication (Keycloak)
- SASL authentication (Kafka)
- Kubernetes secrets management
- TLS encryption

## Monitoring

### Check Data Ingestion

```bash
# FinnhubProducer logs
kubectl logs -l app=finnhub-producer -f

# Kafka messages
kubectl exec -it kafka-0 -- kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic market-data.finnhub.crypto-trades.bronze \
  --max-messages 10
```

### Check Spark Jobs

```bash
# List SparkApplications
kubectl get sparkapplications

# Bronze job logs
kubectl logs -l spark-role=driver,spark-app-name=load-crypto-bronze -f

# Silver job logs
kubectl logs -l spark-role=driver,spark-app-name=transform-crypto-silver-batch -f
```

### Query Iceberg Tables

```bash
# Connect to Spark shell
kubectl run spark-shell -it --rm --image=apache/spark:3.5.0-python3 \
  -- /opt/spark/bin/pyspark

# Query Bronze
spark.sql("SELECT COUNT(*) FROM bronze.bronze.crypto_trades_raw").show()
spark.sql("SELECT * FROM bronze.bronze.crypto_trades_raw LIMIT 10").show()

# Query Silver
spark.sql("SELECT COUNT(*) FROM silver.silver.crypto_ohlcv_1h").show()
spark.sql("SELECT * FROM silver.silver.crypto_ohlcv_1h ORDER BY hour_start DESC LIMIT 10").show()
```

## Documentation

- [FinnhubProducer](FinnhubProducer/README.md) - Real-time data producer
- [Bronze Spark Job](spark-jobs/load-crypto-bronze/README.md) - Streaming ingestion
- [Silver Spark Job](spark-jobs/transform-crypto-silver-batch/README.md) - OHLCV aggregation
- [Airflow DAGs](airflow-dags-deployment/README.md) - Workflow orchestration
- [Infrastructure](infra/k8s/README.md) - Kubernetes setup

## Technology Stack

- **Languages**: Python 3.9+
- **Data Processing**: Apache Spark 3.5.0
- **Streaming**: Apache Kafka, Structured Streaming
- **Storage**: Apache Iceberg, MinIO (S3)
- **Catalog**: Lakekeeper (Iceberg REST)
- **Orchestration**: Apache Airflow, Spark Operator
- **Container**: Docker, Kubernetes
- **Auth**: Keycloak (OAuth2)

## License

This project is for educational purposes (graduation research).
