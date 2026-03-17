## 1.Create a Dataframe as credit_card_df with different read methods 
credit_card_df=spark.createdatframe([("1234567891234567",), 
("5678912345671234",), 
("9123456712345678",), 
("1234567812341122",), 
("1234567812341342",) ],["card_number"])

## 2. print number of partitions 
num_partitions = credit_card_df.rdd.getNumPartitions()
print("Number of partitions:", num_partitions)

##3. Increase the partition size to 5 
df1 = credit_card_df.repartition(5)
print("Partitions after increase:", df1.rdd.getNumPartitions())

## 4. Decrease the partition size back to its original partition size 
df2 = df1.coalesce(num_partitions)  # back to original partitions
print("Partitions after decreasing:", df2.rdd.getNumPartitions())

##5.Create a UDF to print only the last 4 digits marking the remaining digits as *
# Eg: ************4567  
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def mask_card(card_number):
    return '*' * (len(card_number) - 4) + card_number[-4:]

mask_udf = udf(mask_card, StringType())


## 6.output should have 2 columns as card_number, masked_card_number(with output of question 2) 
df.withcolumn("Masked_card_number",demo_udf("card_number"))