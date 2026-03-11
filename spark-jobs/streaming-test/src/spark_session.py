"""SparkSession factory for Streaming Test job."""

from config import load_spark_config
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """Create and configure SparkSession with Iceberg/Lakekeeper and Kafka support."""
    conf_dict = load_spark_config()

    spark_conf = SparkConf().setAppName("Streaming Test — Kafka → Lakehouse")
    for key, value in conf_dict.items():
        spark_conf = spark_conf.set(key, value)

    return SparkSession.builder.config(conf=spark_conf).getOrCreate()
