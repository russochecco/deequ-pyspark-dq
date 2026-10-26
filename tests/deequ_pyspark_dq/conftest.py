import os
import pytest
from pyspark.sql import SparkSession

os.environ["SPARK_VERSION"] = "4.1"


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("DeequPySparkDQTest")
        .config("spark.sparkContext.logLevel", "ERROR")
        .getOrCreate()
    )

    yield spark

    spark.stop()
