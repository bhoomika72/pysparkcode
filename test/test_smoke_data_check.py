import sys
import os
import pytest
from pyspark.sql import functions as F

# Add the source directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.data_check import get_column_names, check_non_null_columns, validate_column_types

# File path for current data
current_data_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\cicd\\curr_data.csv"
### Smoke Test Cases ###
@pytest.mark.smoke
def test_basic_data_reading(spark):
    try:
        current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
        assert current_df.count() > 0
    except Exception as e:
        pytest.fail(f"Failed to read data: {str(e)}")

@pytest.mark.smoke
def test_non_null_columns(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    non_null_columns = ["Id", "Name"]
    result = check_non_null_columns(df, non_null_columns)
    assert result is True

@pytest.mark.smoke
def test_column_names_and_types(spark):
    current_df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    expected_schema = {"Id": "int", "Name": "string", "Age": "int", "City": "string"}
    result = validate_column_types(current_df, expected_schema)
    assert result is True

@pytest.mark.smoke
def test_column_case_sensitivity(spark):
    df = spark.read.csv(current_data_file_path, header=True, inferSchema=True)
    columns = get_column_names(df)
    assert "name" not in columns and "Name1" in columns

@pytest.mark.smoke
def test_reading_invalid_file(spark):
    invalid_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\cicd\\invalid.csv"
    try:
        spark.read.csv(invalid_file_path, header=True, inferSchema=True)
        pytest.fail("Expected an error due to missing file, but none occurred.")
    except Exception as e:
        assert "Path does not exist" in str(e)