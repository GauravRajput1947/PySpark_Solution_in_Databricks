# Databricks notebook source
# MAGIC %md
# MAGIC  **Read CSV file**
# MAGIC

# COMMAND ----------

spark

# COMMAND ----------

flight_df = spark.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "false") \
    .option("mode", "failfast") \
    .load("/Volumes/workspace/default/my_volume/flight_data.csv")

flight_df.show(5)

# COMMAND ----------

flight_df_Header=spark.read.format("csv") \
    .option("header", "True") \
    .option("inferSchema", "false") \
    .option("mode", "failfast") \
    .load("/Volumes/workspace/default/my_volume/flight_data.csv")
flight_df_Header.show(5)

# COMMAND ----------

flight_df_Header.printSchema()

# COMMAND ----------

flight_df_Header_schema=spark.read.format("csv") \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .option("mode", "failfast") \
    .load("/Volumes/workspace/default/my_volume/flight_data.csv")
flight_df_Header.show(5) 

# COMMAND ----------

flight_df_Header_schema.printSchema()

# COMMAND ----------

# MAGIC %md   **Create Schema**

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType


# COMMAND ----------

my_schema=StructType([StructField("DEST_COUNTRY_NAME",StringType(),True),
                       StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
                       StructField("count",IntegerType(),True)])


# COMMAND ----------

flight_df = spark.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "false") \
    .schema(my_schema) \
    .option("mode", "permisive") \
    .load("/Volumes/workspace/default/my_volume/flight_data.csv")

flight_df.show(5)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/default/my_volume/flight_data.csv

# COMMAND ----------

flight_df = spark.read.format("csv") \
    .option("header", "false") \
    .option("SkipRows", "1")\
    .option("inferSchema", "false") \
    .schema(my_schema) \
    .option("mode", "permisive") \
    .load("/Volumes/workspace/default/my_volume/flight_data.csv")

flight_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Handling Corrupted Data**
# MAGIC

# COMMAND ----------

employee_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("mode", "permisive") \
    .load("/Volumes/workspace/default/my_volume1/employee.csv")

employee_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Code run in failfast mode (failed execution when corrupt record found)**

# COMMAND ----------

employee_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("mode", "Failfast") \
    .load("/Volumes/workspace/default/my_volume1/employee.csv")

employee_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## How can we print bad Record
# MAGIC ### ANS:Create Schema

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# COMMAND ----------

emp_schema=StructType([StructField("id",IntegerType(),True),
                       StructField("name",StringType(),True),
                       StructField("age",IntegerType(),True),
                       StructField("salary",IntegerType(),True),
                       StructField("address",StringType(),True),
                       StructField("nominee",StringType(),True),
                       StructField("_corrupted_record",StringType(),True)])


# COMMAND ----------

employee_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(emp_schema) \
    .option("mode", "permissive") \
    .load("/Volumes/workspace/default/my_volume1/employee.csv")

employee_df.show(truncate=False)




# COMMAND ----------

# MAGIC %md
# MAGIC ## **How to we store corrupted records and how can we access it later**

# COMMAND ----------

employee_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .schema(emp_schema) \
    .option("badRecordsPath","/Volumes/workspace/default/my_volume1/bad_records")\
    .load("/Volumes/workspace/default/my_volume1/employee.csv")
    

employee_df.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/default/my_volume1

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/default/my_volume1/bad_records/20260412T185306/bad_records/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Above data in jSON format so i want to read and print so i need to cread new data frame Df

# COMMAND ----------

bad_data_df=spark.read.format("json").load ("/Volumes/workspace/default/my_volume1/bad_records/20260412T185306/bad_records/")
bad_data_df.show()