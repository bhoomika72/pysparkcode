import pytest
import sys
import os

# Add the source directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Set environment variables for PySpark
os.environ['SPARK_HOME'] = 'C:\\spark-3.5.4-bin-hadoop3'
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\admin\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'

# Import SparkSession
from pyspark.sql import SparkSession
from src.data_check import get_count, get_column_names, filter_by_column_value

def test_spark_session_creation():
    """
    Smoke test to ensure the Spark session is created successfully.
    """
    spark = SparkSession.builder \
        .appName("Smoke Test") \
        .master("local[*]") \
        .getOrCreate()

    assert spark is not None
    assert spark.version is not None
    spark.stop()


def test_csv_file_loading(spark):
    """
    Smoke test to ensure the CSV file is loaded successfully into a DataFrame.
    """
    # Replace with the correct path to your CSV file
    file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\sample_data.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    assert df is not None
    assert df.count() > 0  # Ensures data exists in the file


def test_basic_get_count(spark):
    """
    Smoke test to verify the `get_count` function works with a small dataset.
    """
    # Create a small in-memory DataFrame
    data = [("Test", 1), ("Data", 2)]
    df = spark.createDataFrame(data, ["name", "value"])

    # Test the `get_count` function
    result = get_count(df)

    assert result == 2  # Ensure the row count matches the data


def test_basic_get_column_names(spark):
    """
    Smoke test to verify the `get_column_names` function works with a small dataset.
    """
    # Create a small in-memory DataFrame
    data = [("Test", 1), ("Data", 2)]
    df = spark.createDataFrame(data, ["name", "value"])

    # Test the `get_column_names` function
    result = get_column_names(df)

    assert result == ["name", "value"]  # Ensure the column names match


def test_basic_filter_by_column_value(spark):
    """
    Smoke test to verify the `filter_by_column_value` function works with a small dataset.
    """
    # Create a small in-memory DataFrame
    data = [("Alice", "New York"), ("Bob", "Los Angeles"), ("Charlie", "New York")]
    df = spark.createDataFrame(data, ["name", "city"])

    # Test the `filter_by_column_value` function
    filtered_df = filter_by_column_value(df, "city", "New York")

    assert filtered_df.count() == 2  # Ensure only rows with "New York" are filtered
