## Create DataFrame as purchase_data_df,  product_data_df with custom schema with the below data 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, countDistinct, collect_set, size, lag
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("PurchaseAnalysis").getOrCreate()

# Purchase Data
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

# Product Data
product_data = [
    ("iphone13",), 
    ("dell i5 core",), 
    ("dell i3 core",), 
    ("hp i5 core",), 
    ("iphone14",)
]

# Schemas
purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])

product_schema = StructType([
    StructField("product_model", StringType(), True)
])

# Create DataFrames
purchase_data_df = spark.createDataFrame(purchase_data, schema=purchase_schema)
product_data_df = spark.createDataFrame(product_data, schema=product_schema)

purchase_data_df.show()
product_data_df.show()

## 2.Find the customers who have bought only iphone13 
from pyspark.sql.functions import countDistinct

# Group by customer and count distinct products
only_iphone13_df = purchase_data_df.groupBy("customer") \
    .agg(countDistinct("product_model").alias("distinct_count"),
         collect_set("product_model").alias("products")) \
    .filter((col("distinct_count") == 1) & (col("products")[0] == "iphone13")) \
    .select("customer")

only_iphone13_df.show()

## 3.Find customers who upgraded from product iphone13 to product iphone14 
upgraded_df = purchase_data_df.groupBy("customer") \
    .agg(collect_set("product_model").alias("products")) \
    .filter(array_contains(col("products"), "iphone13") & array_contains(col("products"), "iphone14")) \
    .select("customer")

upgraded_df.show()

## 4.Find customers who have bought all models in the new Product Data 
all_products_df = purchase_data_df.groupBy("customer") \
    .agg(collect_set("product_model").alias("products")) \
    .filter(size(col("products")) == product_data_df.count()) \
    .select("customer")

all_products_df.show()
 