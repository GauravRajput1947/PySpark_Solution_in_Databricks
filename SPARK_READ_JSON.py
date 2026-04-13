# Databricks notebook source
# MAGIC %md
# MAGIC ##  Read JSON file in Spark

# COMMAND ----------

spark.read.format("json")\
     .option("inferschema","true")\
     .option("mode","PERMISSIVE")\
     .load("/Volumes/workspace/default/read_json/line_delimited_json.json").show()
       


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extra Field JSON

# COMMAND ----------

spark.read.format("json")\
     .option("inferschema","true")\
     .option("mode","PERMISSIVE")\
     .load("/Volumes/workspace/default/read_json/line_delimited_json_extrafield.json").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### MultiLine Read

# COMMAND ----------

spark.read.format("json")\
     .option("inferschema","true")\
     .option("mode","PERMISSIVE")\
     .load("/Volumes/workspace/default/read_json/Multi_line_correct.json").show()


# COMMAND ----------

spark.read.format("json")\
     .option("inferschema","true")\
     .option("mode","PERMISSIVE")\
     .option("multiline","true")\
     .load("/Volumes/workspace/default/read_json/Multi_line_correct.json").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Incorrect JSON

# COMMAND ----------

spark.read.format("json")\
     .option("inferschema","true")\
     .option("mode","PERMISSIVE")\
     .option("multiline","true")\
     .load("/Volumes/workspace/default/read_json/Multi_line_incorrect.json").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Corrupted Records

# COMMAND ----------

spark.read.format("json")\
     .option("inferschema","true")\
     .option("mode","PERMISSIVE")\
     .load("/Volumes/workspace/default/read_json/corrupted_json.json").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Zomoto Data

# COMMAND ----------

spark.read.format("json")\
     .option("inferschema","true")\
     .option("mode","PERMISSIVE")\
     .load("/Volumes/workspace/default/read_json/file5.json").show()

# COMMAND ----------

spark.read.format("json")\
     .option("inferschema","true")\
     .option("mode","PERMISSIVE")\
     .load("/Volumes/workspace/default/read_json/file5.json").printSchema()