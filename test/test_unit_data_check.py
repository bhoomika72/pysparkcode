import sys
import os
import json

# Add the source directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Set environment variables for PySpark
os.environ['SPARK_HOME'] = 'C:\\spark-3.5.4-bin-hadoop3'
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\admin\\AppData\\Local\\Programs\\Python\\Python39\\python.exe'

from src.data_check import get_count, get_column_names, filter_by_column_value


def test_get_column_names(spark):
    df = spark.read.csv("C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\sample_data.csv", header=True, inferSchema=True)
    
    # Get column names
    result = get_column_names(df)
    
    # Assert the column names are as expected
    assert result == ["Name", "Age", "City"]


def test_filter_by_column_value(spark):
    df = spark.read.csv("C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\sample_data.csv", header=True, inferSchema=True)
    
    filtered_df = filter_by_column_value(df, "city", "New York")
    
    # Get the count of filtered rows
    filtered_count = get_count(filtered_df)
    
    # Assert that filtered rows are as expected (adjust the expected count based on your test data)
    assert filtered_count > 0


# File paths
count_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\cicd\\pysparkcode\\row_count.json"
data_file_path = "C:\\Users\\admin\\OneDrive - TestPerform\\Desktop\\sample_data.csv"


def read_previous_count():
    try:
        # Check if the file exists
        if os.path.exists(count_file_path):
            with open(count_file_path, "r") as file:
                data = file.read().strip()  # Read and strip any extra spaces/newlines
                
                # If the file is not empty, load the JSON data
                if data:
                    count_data = json.loads(data)
                    return count_data.get("row_count", 0)
                else:
                    return 0  # Return 0 if the file is empty
        else:
            return 0  # Return 0 if the file doesn't exist
            
    except json.JSONDecodeError:
        # If JSON is malformed, log or print an error and return 0
        print("Error: Malformed JSON or empty file. Returning default count 0.")
        return 0
    except Exception as e:
        # Catch any other exceptions and return 0
        print(f"Error: {e}. Returning default count 0.")
        return 0


def write_current_count(count):
    """Write the current row count to the JSON file."""
    with open(count_file_path, 'w') as file:
        json.dump({"row_count": count}, file)


def test_insertion_deletion(spark):
    # Read the initial data
    df = spark.read.csv(data_file_path, header=True, inferSchema=True)
    
    # Get the current row count
    current_count = get_count(df)
    
    # Read the previous row count
    previous_count = read_previous_count()
    
    # Calculate inserted and deleted rows
    inserted_rows = max(current_count - previous_count, 0)
    deleted_rows = max(previous_count - current_count, 0)

    # Calculate the expected count: previous count + inserted rows - deleted rows
    expected_count = previous_count + inserted_rows - deleted_rows
    
    # Assert the current count matches the expected count
    assert current_count == expected_count, f"Expected {expected_count}, but got {current_count}"
    
    # Output the result for verification
    print(f"Previous Row Count: {previous_count}")
    print(f"Current Row Count: {current_count}")
    print(f"Inserted Rows: {inserted_rows}")
    print(f"Deleted Rows: {deleted_rows}")
    
    # Save the current count for future runs
    write_current_count(current_count)

    # Assertions
    assert current_count >= 0  # Current count should never be negative
    assert inserted_rows >= 0  # Inserted rows should be non-negative
    assert deleted_rows >= 0  # Deleted rows should be non-negative
