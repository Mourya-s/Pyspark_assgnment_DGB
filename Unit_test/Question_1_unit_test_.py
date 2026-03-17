import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ---------------------------
# Spark Session (fixture)
# ---------------------------
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("unit-test") \
        .getOrCreate()

# ---------------------------
# Test Data
# ---------------------------
@pytest.fixture
def input_data(spark):

    purchase_data = [
        (1, "iphone13"), 
        (1, "dell i5 core"), 
        (2, "iphone13"), 
        (2, "dell i5 core"), 
        (3, "iphone13"), 
        (3, "dell i5 core"), 
        (1, "dell i3 core"), 
        (1, "hp i5 core"), 
        (1, "iphone14"), 
        (3, "iphone14"), 
        (4, "iphone13")
    ]

    schema = StructType([
        StructField("customer", IntegerType(), True),
        StructField("product_model", StringType(), True)
    ])

    return spark.createDataFrame(purchase_data, schema)


# ---------------------------
# Function to Test
# ---------------------------
def get_only_iphone13(df):
    return df.groupBy("customer") \
        .agg(countDistinct("product_model").alias("distinct_count"),
             collect_set("product_model").alias("products")) \
        .filter((col("distinct_count") == 1) & (col("products")[0] == "iphone13")) \
        .select("customer")


def get_upgraded(df):
    return df.groupBy("customer") \
        .agg(collect_set("product_model").alias("products")) \
        .filter(array_contains(col("products"), "iphone13") &
                array_contains(col("products"), "iphone14")) \
        .select("customer")


# ---------------------------
# Test Cases
# ---------------------------

def test_only_iphone13(spark, input_data):
    result_df = get_only_iphone13(input_data)

    expected_data = [(4,)]
    expected_df = spark.createDataFrame(expected_data, ["customer"])

    assert sorted(result_df.collect()) == sorted(expected_df.collect())


def test_upgraded(spark, input_data):
    result_df = get_upgraded(input_data)

    expected_data = [(1,), (3,)]
    expected_df = spark.createDataFrame(expected_data, ["customer"])

    assert sorted(result_df.collect()) == sorted(expected_df.collect())