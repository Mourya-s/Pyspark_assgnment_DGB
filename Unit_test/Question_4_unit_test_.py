import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer, posexplode

# ---------------------------
# Spark Session
# ---------------------------
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("unit-test") \
        .getOrCreate()

# ---------------------------
# Sample JSON Data
# ---------------------------
@pytest.fixture
def input_df(spark):

    data = [
        {
            "id": 1,
            "properties": {"name": "ABC", "storeSize": "L"},
            "employees": [
                {"empId": 101, "empName": "John"},
                {"empId": 102, "empName": "Jane"}
            ]
        },
        {
            "id": 2,
            "properties": {"name": "XYZ", "storeSize": "M"},
            "employees": [
                {"empId": 103, "empName": "Mike"}
            ]
        },
        {
            "id": 3,
            "properties": {"name": "DEF", "storeSize": "S"},
            "employees": None   # important for explode_outer test
        }
    ]

    return spark.createDataFrame(data)

# ---------------------------
# Function to Flatten
# ---------------------------
def flatten_df(df):
    return df.select(
        col("id"),
        col("properties.name").alias("company_name"),
        col("properties.storeSize").alias("store_size"),
        explode("employees").alias("employee")
    ).select(
        "id", "company_name", "store_size",
        col("employee.empId").alias("employee_id"),
        col("employee.empName").alias("employee_name")
    )

# ---------------------------
# Test Cases
# ---------------------------

def test_flatten_count_difference(input_df):
    df2 = flatten_df(input_df)

    original_count = input_df.count()
    flattened_count = df2.count()

    assert flattened_count > original_count   # explode increases rows


def test_flatten_data_correctness(spark, input_df):
    df2 = flatten_df(input_df)
    

    expected_data = [
        (1, "ABC", "L", 101, "John"),
        (1, "ABC", "L", 102, "Jane"),
        (2, "XYZ", "M", 103, "Mike")
    ]

    expected_df = spark.createDataFrame(
        expected_data,
        ["id", "company_name", "store_size", "employee_id", "employee_name"]
    )

    assert sorted(df2.collect()) == sorted(expected_df.collect())


def test_explode_vs_explode_outer(input_df):
    explode_count = input_df.select(explode("employees")).count()

    explode_outer_count = input_df.select(explode_outer("employees")).count()

    # explode ignores null, explode_outer keeps it
    assert explode_outer_count >= explode_count


def test_posexplode(input_df):
    df_pos = input_df.select("id", posexplode("employees").alias("pos", "employee"))

    # Check if position column exists
    assert "pos" in df_pos.columns

    # Ensure rows are generated
    assert df_pos.count() > 0