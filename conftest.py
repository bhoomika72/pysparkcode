import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("PySpark Testing") \
        .master("local[*]") \
        .getOrCreate()
