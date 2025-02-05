import sys
import os
import pytest
from pyspark.sql import functions as F

# Add the source directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.data_check import get_count, get_column_names, filter_by_column_value

# File path for current data
current_data_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\cicd\\curr_data.csv"

### Unit Test Cases ###
@pytest.mark.unit
def test_get_column_names(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    result = get_column_names(df)
    assert result == ["Id", "Name", "Age", "City"]

@pytest.mark.unit
def test_filter_by_column_value(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    filtered_df = filter_by_column_value(df, "City", "New York")
    filtered_count = get_count(filtered_df)
    assert filtered_count > 0

@pytest.mark.unit
def test_empty_dataframe(spark):
    df = spark.createDataFrame([], schema="Id INT, Name STRING, Age INT, City STRING")
    assert df.count() == 0

@pytest.mark.unit
def test_get_count(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    assert get_count(df) > 0

@pytest.mark.unit
def test_column_existence(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    assert "Age" in get_column_names(df)


