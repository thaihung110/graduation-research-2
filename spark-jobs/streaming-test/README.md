# Streaming Test Spark Job

A **test** job for validating the end-to-end Kafka → Spark Structured Streaming → Iceberg (Lakehouse) pipeline on the data platform.

## What it does

```
Kafka topic  →  Spark Structured Streaming  →  Iceberg table (Bronze)
                    (foreachBatch)               lakekeeper.<database>.<table>
```

The job intentionally keeps transformation minimal:

| Column                | Description                                      |
| --------------------- | ------------------------------------------------ |
| `kafka_topic`         | Source Kafka topic name                          |
| `kafka_partition`     | Kafka partition                                  |
| `kafka_offset`        | Kafka offset                                     |
| `kafka_timestamp`     | Kafka message timestamp (event time)             |
| `kafka_key`           | Message key (may be null)                        |
| `raw_value`           | Raw message value as UTF-8 string                |
| `raw_value_bytes_len` | Byte length of the raw value                     |
| `bronze_ingestion_ts` | Wall-clock time when the record landed in Bronze |
| `ingestion_date`      | Date partition key                               |

## Directory structure

```
streaming-test/
├── Dockerfile          # Self-contained image with pre-baked JARs
├── build-image.sh      # Build & push script
├── README.md
└── src/
    ├── main.py         # Entry point
    ├── config.py       # Env-var based configuration
    ├── spark_session.py
    └── etl/
        ├── extract.py  # Kafka → Streaming DataFrame
        ├── transform.py # minimal transform (raw bytes → string + metadata)
        └── load.py     # Iceberg DDL + append (writeTo / INSERT INTO fallback)
```

## Build & push

```bash
cd spark-jobs/streaming-test
./build-image.sh v1.0
```

## Environment variables

| Variable                  | Default                                                                     | Description                       |
| ------------------------- | --------------------------------------------------------------------------- | --------------------------------- |
| `KAFKA_TOPIC`             | `streaming-test`                                                            | Kafka topic to subscribe to       |
| `KAFKA_BOOTSTRAP_SERVERS` | `openhouse-kafka:9092`                                                      | Kafka brokers                     |
| `KAFKA_SASL_USERNAME`     | `admin`                                                                     | SASL/PLAIN username               |
| `KAFKA_SASL_PASSWORD`     | `admin`                                                                     | SASL/PLAIN password               |
| `DATABASE`                | `bronze`                                                                    | Iceberg namespace                 |
| `TABLE`                   | `streaming_test_raw`                                                        | Iceberg table name                |
| `CHECKPOINT_LOCATION`     | `s3a://bronze/checkpoints/streaming-test`                                   | Spark checkpoint path             |
| `TRIGGER_INTERVAL`        | `30 seconds`                                                                | Micro-batch trigger interval      |
| `MAX_OFFSETS_PER_TRIGGER` | `1000`                                                                      | Max Kafka offsets per micro-batch |
| `CATALOG_URL`             | `http://openhouse-gravitino:9001/iceberg`                                   | Iceberg REST catalog URL          |
| `CLIENT_ID`               | `spark`                                                                     | OAuth2 client ID                  |
| `CLIENT_SECRET`           | `YeG2U2zPQqnLoIfD3Bc3c55pfIUnDNFd`                                          | OAuth2 client secret              |
| `WAREHOUSE`               | `bronze`                                                                    | Iceberg warehouse                 |
| `KEYCLOAK_TOKEN_ENDPOINT` | `http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token` | Keycloak token URL                |

## Running via Airflow

Trigger the `streaming-test` DAG from the Airflow UI. All parameters are
configurable via the DAG's **Trigger with config** dialog.

## Running manually (kubectl)

```bash
kubectl apply -f infra/k8s/compute/applications/spark/bronze-layer/jobs/streaming-test.yaml
```
