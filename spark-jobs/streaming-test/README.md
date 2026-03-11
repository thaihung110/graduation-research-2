# Streaming Test Spark Job

A **test** job for validating the end-to-end Kafka → Spark Structured Streaming → Iceberg (Lakehouse) pipeline on the data platform.

## What it does

```
Kafka topic  →  Spark Structured Streaming  →  Iceberg table
                    (foreachBatch)               <catalog>.<database>.<table>
```

The job intentionally keeps transformation minimal:

| Column                | Description                                                     |
| --------------------- | --------------------------------------------------------------- |
| `kafka_topic`         | Source Kafka topic name                                         |
| `kafka_partition`     | Kafka partition                                                 |
| `kafka_offset`        | Kafka offset                                                    |
| `kafka_timestamp`     | Kafka message timestamp (event time)                            |
| `kafka_key`           | Message key (may be null)                                       |
| `raw_value`           | Raw message value as UTF-8 string                               |
| `raw_value_bytes_len` | Byte length of the raw value                                    |
| `bronze_ingestion_ts` | Wall-clock time when the record landed in destination warehouse |
| `ingestion_date`      | Date partition key                                              |

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

## Runtime configuration

The job is configured entirely via environment variables and Spark/Hadoop
config, and is usually launched from Airflow using the
`spark-streaming-job-template` DAG.

### Core environment variables (as read by `config.py`)

| Variable                              | Default in code                                                             | Description                                 |
| ------------------------------------- | --------------------------------------------------------------------------- | ------------------------------------------- |
| `KAFKA_TOPIC`                         | `streaming-test`                                                            | Kafka topic to subscribe to                 |
| `KAFKA_BOOTSTRAP_SERVERS`             | `openhouse-kafka:9092`                                                      | Kafka brokers                               |
| `KAFKA_STARTING_OFFSETS`              | `latest`                                                                    | Kafka starting offsets                      |
| `KAFKA_SECURITY_PROTOCOL`             | `SASL_SSL`                                                                  | Kafka security protocol                     |
| `KAFKA_SASL_MECHANISM`                | `SCRAM-SHA-512`                                                             | Kafka SASL mechanism                        |
| `KAFKA_SSL_TRUSTSTORE_LOCATION`       | `/etc/kafka/certs/ca/ca.crt`                                                | Kafka SSL truststore path                   |
| `KAFKA_SASL_USERNAME`                 | _none_                                                                      | Kafka SASL username                         |
| `KAFKA_SASL_PASSWORD`                 | _none_                                                                      | Kafka SASL password                         |
| `KAFKA_GROUP_ID`                      | _none_                                                                      | Kafka consumer group id                     |
| `MAX_OFFSETS_PER_TRIGGER`             | `1000`                                                                      | Max offsets per micro-batch                 |
| `CATALOG_NAME`                        | `lakekeeper`                                                                | Iceberg catalog name                        |
| `CATALOG_URL`                         | `http://openhouse-gravitino:9001/iceberg`                                   | Iceberg REST catalog URL                    |
| `CATALOG_SCOPE`                       | `email`                                                                     | OAuth / Gravitino scope                     |
| `WAREHOUSE`                           | `bronze`                                                                    | Warehouse / bucket prefix                   |
| `KEYCLOAK_TOKEN_ENDPOINT`             | `http://openhouse-keycloak:80/realms/iceberg/protocol/openid-connect/token` | Keycloak token URL                          |
| `CLIENT_ID`                           | `spark`                                                                     | OAuth2 client ID                            |
| `CLIENT_SECRET`                       | `YeG2U2zPQqnLoIfD3Bc3c55pfIUnDNFd`                                          | OAuth2 client secret (redacted in Spark UI) |
| `CATALOG_S3_ENDPOINT`                 | `http://storage-minio:9000`                                                 | S3 endpoint used by Iceberg’s S3FileIO      |
| `CATALOG`                             | `lakekeeper`                                                                | Target Iceberg catalog (logical name)       |
| `SCHEMA`                              | `bronze`                                                                    | Target Iceberg schema / namespace           |
| `TABLE`                               | `streaming_test_raw`                                                        | Target Iceberg table name                   |
| `CHECKPOINT_LOCATION`                 | `s3a://bronze/checkpoints/streaming-test`                                   | Spark checkpoint path                       |
| `TRIGGER_INTERVAL`                    | `30 seconds`                                                                | Micro-batch trigger interval                |
| `SPARK_NO_DATA_MICRO_BATCHES_ENABLED` | `true`                                                                      | Whether to run no-data micro-batches        |

The job also sets:

- `spark.sql.catalog.<CATALOG_NAME>.*` to configure Iceberg REST + credential vending
- `spark.sql.streaming.noDataMicroBatches.enabled` from `SPARK_NO_DATA_MICRO_BATCHES_ENABLED`
- `spark.redaction.regex` so that secrets (including `CLIENT_SECRET`) are masked in Spark UI

### Example Airflow trigger config (`airflow_conf.txt`)

This is the concrete configuration currently used when triggering the job via
Airflow:

```yaml
job_name_prefix: streaming-test
image_repo: hub.vtcc.vn:8989/hungvt0110/streaming-test
image_tag: v0.6
main_file_path: local:///app/src/main.py

user_env_vars:
  CATALOG_NAME: gravitino_test
  CATALOG_URL: http://gravitino:9001/iceberg
  KEYCLOAK_TOKEN_ENDPOINT: http://security-keycloak/realms/iceberg/protocol/openid-connect/token
  CLIENT_ID: trino
  CLIENT_SECRET: <client_secret>
  CATALOG_SCOPE: email
  WAREHOUSE: iceberg_s3
  CATALOG_S3_ENDPOINT: http://storage-minio:9000

  KAFKA_BOOTSTRAP_SERVERS: lakehouse-kafka-kafka-bootstrap.dmp-lakehouse-demo.svc:9094
  KAFKA_TOPIC: raw-ingress
  KAFKA_STARTING_OFFSETS: latest
  KAFKA_SECURITY_PROTOCOL: SASL_SSL
  KAFKA_SASL_MECHANISM: SCRAM-SHA-512
  KAFKA_SSL_TRUSTSTORE_LOCATION: /etc/kafka/certs/ca/ca.crt
  KAFKA_SASL_USERNAME: raw-consumer
  KAFKA_SASL_PASSWORD: <kafka_password>
  KAFKA_GROUP_ID: raw-cg-test
  MAX_OFFSETS_PER_TRIGGER: "1000"

  SCHEMA: test_schema
  TABLE: hungvt
  CHECKPOINT_LOCATION: s3a://gravitino1/checkpoints/streaming-test
  TRIGGER_INTERVAL: 2 seconds
  SPARK_NO_DATA_MICRO_BATCHES_ENABLED: "true"

spark_conf:
  spark.eventLog.dir: s3a://spark-events/logs

hadoop_conf:
  fs.s3a.endpoint: http://storage-minio:9000
  fs.s3a.bucket.spark-events.endpoint: http://storage-minio:9000
  fs.s3a.bucket.spark-events.access.key: root
  fs.s3a.bucket.spark-events.secret.key: <minio_secret>
  fs.s3a.bucket.gravitino1.endpoint: http://storage-minio:9000
  fs.s3a.bucket.gravitino1.access.key: root
  fs.s3a.bucket.gravitino1.secret.key: <minio_secret>
```

### Running via Airflow

- DAG: `spark-streaming-job-template`
- Trigger manually và paste cấu hình giống YAML ở trên vào dialog
  **Trigger run with config**.
