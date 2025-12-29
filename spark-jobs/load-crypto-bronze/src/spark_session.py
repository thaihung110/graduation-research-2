from config import load_spark_config
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """Create and configure Spark session with Iceberg, Kafka, and lakekeeper catalog."""
    conf = load_spark_config()

    spark_conf = SparkConf().setAppName("Crypto Trades Bronze Ingestion")
    for key, value in conf.items():
        spark_conf = spark_conf.set(key, value)

    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    return spark
