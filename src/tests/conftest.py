import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.config("spark.master", "local")
        .appName("test")
        .getOrCreate()
    )
