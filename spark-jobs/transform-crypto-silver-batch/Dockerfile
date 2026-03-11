# Dockerfile for Spark job to transform crypto trades from Bronze to Silver
# Use official Apache Spark image with Python support
FROM apache/spark:3.5.0-python3

# Switch to root for any setup needed
USER root

# ==============================================================================
# Pre-download required JAR dependencies to avoid network downloads at runtime
# ==============================================================================
ARG ICEBERG_VERSION=1.10.1
ARG SCALA_VERSION=2.12
ARG HADOOP_VERSION=3.3.4
ARG PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_${SCALA_VERSION}:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION},org.apache.hadoop:hadoop-aws:${HADOOP_VERSION}"

# Run an empty pyspark script to let Ivy download the packages and their dependencies,
# then copy them directly into the standard Spark jars directory.
# We use an explicit repository to avoid connection issues.
RUN set -e && \
    echo "print('Downloading packages...')" > /tmp/download.py && \
    /opt/spark/bin/spark-submit \
        --packages $PACKAGES \
        --repositories https://repo1.maven.org/maven2/ \
        /tmp/download.py && \
    JAR_COUNT=$(find /root/.ivy2 -type f -name "*.jar" | wc -l) && \
    echo "Downloaded $JAR_COUNT jars." && \
    if [ "$JAR_COUNT" -eq 0 ]; then echo "Error: No jars downloaded!" && exit 1; fi && \
    find /root/.ivy2 -type f -name "*.jar" -exec cp {} /opt/spark/jars/ \; && \
    rm -rf /root/.ivy2 /tmp/download.py

# Set working directory
WORKDIR /app

# Copy Python application code (src directory)
COPY src/ /app/src/

# Make script executable and set ownership/permissions
RUN chmod +x /app/src/main.py && \
    chmod 777 /app && \
    chown -R 185:185 /app

# Switch back to default spark user (uid 185)
USER 185

# Set PYTHONPATH to include src directory
ENV PYTHONPATH=/app/src

# Default command
CMD ["python", "/app/src/main.py"]


