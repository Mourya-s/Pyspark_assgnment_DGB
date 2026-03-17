# ## 1. Create a Data Frame with custom schema creation by using Struct Type and Struct Field 

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

spark = SparkSession.builder.appName("xyz").getOrCreate()

data = [
    (1, 101, 'login', '2023-09-05 08:30:00'),
    (2, 102, 'click', '2023-09-06 12:45:00'),
    (3, 101, 'click', '2023-09-07 14:15:00'),
    (4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2023-09-09 17:30:00'),
    (6, 101, 'click', '2023-09-10 11:20:00'),
    (7, 103, 'click', '2023-09-11 10:15:00'),
    (8, 102, 'click', '2023-09-12 13:10:00')
]

schema = StructType([
    StructField("log_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_activity", StringType(), True),
    StructField("time_stamp", StringType(), True)  # Keep string first, can convert to timestamp later
])

df = spark.createDataFrame(data, schema)
df.show()


# 2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function 
old_cols = df.columns
new_cols = ["log_id", "user_id", "user_activity", "time_stamp"]

for old, new in zip(old_cols, new_cols):
    df = df.withColumnRenamed(old, new)


# 3. Write a query to calculate the number of actions performed by each user in the last 7 days 
from pyspark.sql.functions import col, current_date, to_date, datediff

# Convert time_stamp to date type
df = df.withColumn("time_stamp", to_date(col("time_stamp"), "yyyy-MM-dd"))

# Filter last 7 days
df_last7 = df.filter(datediff(current_date(), col("time_stamp")) <= 7)

# Count actions per user
df_last7.groupBy("user_id").count().show()



# # 4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type 
x=df.withcolumn("login_date",todate("login_date"))
df1=df.witcolumn("login_date",dateformat("login_date",yyyy-MM-DD))



# # 5. Write the data frame as a CSV file with different write options except (merge condition) 
df.write.mode("overwrite").option("header", True).csv("/path/data/new_db")


# 6. Write it as a managed table with the Database name as user and table name as login_details with overwrite mode. 
# Ensure database exists
spark.sql("CREATE DATABASE IF NOT EXISTS user")

# Write as managed table
df.write.mode("overwrite").saveAsTable("user.login_details")