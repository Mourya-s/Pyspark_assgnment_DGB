# 1. create all 3 data frames as employee_df, department_df, country_df
# with custom schema defined in dynamic way 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("EmployeeExample").getOrCreate()

# Employee schema
employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("state", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("age", IntegerType(), True)
])

employee_data = [
    (11, "james", "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott", "D103", "ca", 8000, 36),
    (15, "jen", "D102", "ny", 9500, 38),
    (16, "jeff", "D103", "uk", 9100, 35),
    (17, "maria", "D101", "ny", 7900, 40)
]

employee_df = spark.createDataFrame(employee_data, schema=employee_schema)

# Department schema
department_schema = StructType([
    StructField("dept_id", StringType(), True),
    StructField("dept_name", StringType(), True)
])

department_data = [
    ("D101","sales"),
    ("D102","finance"),
    ("D103","marketing"),
    ("D104","hr"),
    ("D105","support")
]

department_df = spark.createDataFrame(department_data, schema=department_schema)

# Country schema
country_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True)
])

country_data = [
    ("ny","newyork"),
    ("ca","California"),
    ("uk","Russia")
]

country_df = spark.createDataFrame(country_data, schema=country_schema)


# 2. Find avg salary of each department 
from pyspark.sql.functions import avg

avg_salary_df = employee_df.groupBy("department").agg(avg("salary").alias("avg_salary"))
avg_salary_df.show()



# 3. Find the employee’s name and department name whose name starts with ‘m’  
from pyspark.sql.functions import col

employee_m_df = employee_df.filter(col("employee_name").startswith("m")) \
    .join(department_df, employee_df.department == department_df.dept_id, "inner") \
    .select("employee_name", "dept_name")

employee_m_df.show()



# 4. Create another new column in  employee_df as a bonus by multiplying employee salary *2 
from pyspark.sql.functions import col

employee_df = employee_df.withColumn("bonus", col("salary") * 2)
employee_df.show()



# 5. Reorder the column names of employee_df columns 
# as (employee_id,employee_name,salary,State,Age,department) 
employee_df = employee_df.select("employee_id", "employee_name", "salary", "state", "age", "department", "bonus")
employee_df.show()

# 6. Give the result of an inner join, left join, and right join
# when joining employee_df with department_df in a dynamic way 
# Inner Join
inner_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "inner")
inner_df.show()

# Left Join
left_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "left")
left_df.show()

# Right Join
right_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "right")
right_df.show()


# 7. Derive a new data frame with country_name instead of State in employee_df  
# Eg(11,“james”,”D101”,”newyork”,8900,32) 
country_joined_df = employee_df.join(country_df, employee_df.state == country_df.country_code, "left") \
    .drop("state") \
    .withColumnRenamed("country_name", "state")  # replace 'state' column
country_joined_df.show()




# 8. convert all the column names into lowercase from the result
#  of question 7in a dynamic way, add the load_date column with the current date 
from pyspark.sql.functions import current_date

# Lowercase column names
lowercase_df = country_joined_df.toDF(*[c.lower() for c in country_joined_df.columns]) \
    .withColumn("load_date", current_date())
lowercase_df.show()


# 9. create 2 external tables with parquet, CSV format with the
#  same name database name, and 2 different table names as CSV and parquet format. 
# Paths for external tables
parquet_path = "/path/to/external/parquet_table"
csv_path = "/path/to/external/csv_table"

# Write Parquet
lowercase_df.write.mode("overwrite").parquet(parquet_path)

# Write CSV
lowercase_df.write.mode("overwrite").option("header", True).csv(csv_path)

# Optionally, register as external table in Spark SQL
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS db_1.parquet_table
USING PARQUET
LOCATION '{parquet_path}'
""")

spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS db_1.csv_table
USING CSV
OPTIONS (header "true")
LOCATION '{csv_path}'
""")
 