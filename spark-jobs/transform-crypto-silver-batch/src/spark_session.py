from config import load_spark_config
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """Create and configure Spark session with Iceberg and dual catalogs."""
    conf = load_spark_config()

    spark_conf = SparkConf().setAppName(
        "Crypto OHLCV Aggregation - Bronze to Silver (Batch)"
    )
    for key, value in conf.items():
        spark_conf = spark_conf.set(key, value)

    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    return spark
