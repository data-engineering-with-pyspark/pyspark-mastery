# Databricks notebook source
# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Reading from CSV Files")
    .master("local[*]")
    .getOrCreate()
)

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read a csv file into dataframe

# COMMAND ----------



df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("dbfs:/FileStore/ease_with_data/datasets/emp.csv")


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading with Schema

# COMMAND ----------

# Reading with Schema
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"

df_schema = spark.read.format("csv").option("header",True).schema(_schema).load("dbfs:/FileStore/ease_with_data/datasets/emp.csv")


# COMMAND ----------

df_schema.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PERMISSIVE (Default mode) - Handle BAD records
# MAGIC
# MAGIC

# COMMAND ----------

# Handle BAD records - PERMISSIVE (Default mode)

_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date, bad_record string"

df_p = spark.read.format("csv").schema(_schema).option("columnNameOfCorruptRecord", "bad_record").option("header", True).load("dbfs:/FileStore/ease_with_data/datasets/emp_new.csv")


# COMMAND ----------

df_p.printSchema()

# COMMAND ----------

df_p.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROPMALFORMED - Handle BAD records

# COMMAND ----------

# Handle BAD records - DROPMALFORMED
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"

df_m = spark.read.format("csv").option("header", True).option("mode", "DROPMALFORMED").schema(_schema).load("dbfs:/FileStore/ease_with_data/datasets/emp_new.csv")


# COMMAND ----------

df_m.printSchema()

# COMMAND ----------

df_m.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAILFAST - Handle BAD records
# MAGIC

# COMMAND ----------

# Handle BAD records - FAILFAST

_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"

df_m = spark.read.format("csv").option("header", True).option("mode", "FAILFAST").schema(_schema).load("dbfs:/FileStore/ease_with_data/datasets/emp_new.csv")



# COMMAND ----------

df_m.printSchema()

# COMMAND ----------

df_m.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multiple options

# COMMAND ----------

# BONUS TIP
# Multiple options

_options = {
    "header" : "true",
    "inferSchema" : "true",
    "mode" : "PERMISSIVE"
}

df = (spark.read.format("csv").options(**_options).load("dbfs:/FileStore/ease_with_data/datasets/emp.csv"))


# COMMAND ----------

df.show()

# COMMAND ----------

spark.stop()

# COMMAND ----------


