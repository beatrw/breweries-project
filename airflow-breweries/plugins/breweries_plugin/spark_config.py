import json
from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    with open('/opt/airflow/config/airflow_config.json') as f:
        spark_config = json.load(f)

    spark = SparkSession.builder.appName(app_name)

    for key, value in spark_config.items():
        spark = spark.config(key, value)

    spark = spark.getOrCreate()

    return spark
