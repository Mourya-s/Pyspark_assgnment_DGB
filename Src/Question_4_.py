# 1. Read JSON file provided in the attachment using the dynamic function 
df = spark.read.json("path/....")

# 2. flatten the data frame which is a custom schema 
from pyspark.sql.functions import col, explode

df2 = df.select(
    col("id"),
    col("properties.name").alias("company_name"),
    col("properties.storeSize").alias("store_size"),
    explode("employees").alias("employee")
).select(
    "id", "company_name", "store_size",
    col("employee.empId").alias("employee_id"),
    col("employee.empName").alias("employee_name")
)

# 3. find out the record count when flattened and 
# when it's not flattened(find out the difference why you are getting more count) 
print(df.count())
print(df2.count()) # this will have more number of rows

# 4. Differentiate the difference using explode, explode outer, posexplode functions 
df.select("id", posexplode("employees").alias("pos", "employee")).show()


# 5. Filter the id which is equal to 0001  
df2.filter(col("id") == 1001).show()


# 6. convert the column names from camel case to snake case 
df2 = df2.withColumnRenamed("employeeName", "employee_name") \
         .withColumnRenamed("storeSize", "store_size")


# 7. Add a new column named load_date with the current date 
from pyspark.sql.functions import current_date

df2 = df2.withColumn("load_date", current_date())


# 8. create 3 new columns as year, month, and day from the load_date column 
from pyspark.sql.functions import year, month, dayofmonth

df2 = df2.withColumn("year", year("load_date")) \
         .withColumn("month", month("load_date")) \
         .withColumn("day", dayofmonth("load_date"))


# 9. write data frame to a table with the Database name as employee and
#  table name as employee_details with overwrite mode, format as JSON and 
#  partition based on (year, month, day) using replacing where condition on year, month, day 


 # (to replaceWhere only specific partiotion)
df2.write \
   .format("json") \
   .mode("overwrite") \
   .partitionBy("year","month","day") \
   .option("replaceWhere", "year=2026 AND month=3 AND day=13") \
   .saveAsTable("employee.employee_details")