import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

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
# Input Data
# ---------------------------
@pytest.fixture
def employee_df(spark):
    data = [
        (1, "IT", 1000),
        (2, "IT", 2000),
        (3, "HR", 1500),
        (4, "HR", 2500),
        (5, "Sales", 3000)
    ]
    return spark.createDataFrame(data, ["emp_id", "department", "salary"])

# ---------------------------
# Function to Test
# ---------------------------
def get_avg_salary(df):
    return df.groupBy("department") \
        .agg(avg("salary").alias("avg_salary"))

# ---------------------------
# Test Cases
# ---------------------------

def test_avg_salary_values(spark, employee_df):
    result_df = get_avg_salary(employee_df)

    expected_data = [
        ("IT", 1500.0),
        ("HR", 2000.0),
        ("Sales", 3000.0)
    ]

    expected_df = spark.createDataFrame(expected_data, ["department", "avg_salary"])

    assert sorted(result_df.collect()) == sorted(expected_df.collect())


def test_schema(employee_df):
    result_df = get_avg_salary(employee_df)

    assert result_df.columns == ["department", "avg_salary"]


def test_row_count(employee_df):
    result_df = get_avg_salary(employee_df)

    # 3 departments → 3 rows
    assert result_df.count() == 3