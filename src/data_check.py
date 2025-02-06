from pyspark.sql import DataFrame

def get_count(df: DataFrame) -> int:
    """Returns the number of rows in the DataFrame."""
    return df.count()

def get_column_names(df: DataFrame) -> list:
    """Returns the list of column names from the DataFrame."""
    return df.columns

def filter_by_column_value(df: DataFrame, column_name: str, value) -> DataFrame:
    """Filters the DataFrame by a specific column value."""
    return df.filter(df[column_name] == value)

def check_non_null_columns(df: DataFrame, columns: list) -> bool:
    """Checks if the given columns contain any null values."""
    for col in columns:
        if df.filter(df[col].isNull()).count() > 0:
            return False
    return True

def validate_column_types(df: DataFrame, expected_schema: dict) -> bool:
    """Validates if the DataFrame's schema matches the expected data types."""
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    for col, dtype in expected_schema.items():
        if col not in actual_schema or actual_schema[col] != dtype:
            return False
    return True


def test_print() -> None:
    """Prints a test statement."""
    print("This is a test print statement.")

def cicd_demo() -> None:
    print("New code deployed using Jenkins pipeline")
