# Dockerfile for Streaming Test Spark Job
#
# Builds a self-contained Spark image that:
#   - Pre-downloads all required JARs (Iceberg, Kafka, Hadoop-AWS) at build
#     time so the job never needs network access at runtime for JAR resolution.
#   - Packages the Python application code under /app/src.
#
# Build & push:
#   cd spark-jobs/streaming-test && ./build-image.sh v1.0

FROM apache/spark:3.5.0-python3

USER root

# ── Python dependencies ────────────────────────────────────────────────────
# (none required beyond PySpark which is already in the base image)
# If you need extra Python libs, add them here:
# RUN pip install --no-cache-dir <package>

# ── JAR dependencies ───────────────────────────────────────────────────────
# Pre-bake all JARs into /opt/spark/jars so Spark can find them without
# calling out to Maven Central at runtime.
ARG ICEBERG_VERSION=1.10.1
ARG SPARK_VERSION=3.5.0
ARG SCALA_VERSION=2.12
ARG HADOOP_VERSION=3.3.4
ARG PACKAGES="\
org.apache.iceberg:iceberg-spark-runtime-3.5_${SCALA_VERSION}:${ICEBERG_VERSION},\
org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION},\
org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION},\
org.apache.hadoop:hadoop-aws:${HADOOP_VERSION}"

RUN echo "print('Downloading JARs...')" > /tmp/download.py \
    && /opt/spark/bin/spark-submit --packages $PACKAGES /tmp/download.py \
    && find /root/.ivy2 -type f -name "*.jar" -exec cp {} /opt/spark/jars/ \; \
    && rm -rf /root/.ivy2 /tmp/download.py

# ── Kafka TLS certificate ──────────────────────────────────────────────────
# Bake CA cert into the image so both driver and executor pods can verify
# Kafka broker TLS without needing an external secret mount.
COPY ca.crt /etc/kafka/certs/ca/ca.crt
RUN chmod 644 /etc/kafka/certs/ca/ca.crt

# ── Application code ───────────────────────────────────────────────────────
WORKDIR /app
COPY src/ /app/src/

RUN chmod +x /app/src/main.py \
    && chmod 777 /app \
    && chown -R 185:185 /app

# Switch to default Spark user (uid 185)
USER 185

ENV PYTHONPATH=/app/src:$PYTHONPATH

CMD ["python", "/app/src/main.py"]
