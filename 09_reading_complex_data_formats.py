# Databricks notebook source
# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Reading Complex Data Formats")
    .master("local[*]")
    .getOrCreate()
)

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Parquet Sales data
# MAGIC

# COMMAND ----------

# Read Parquet Sales data

df_parquet = spark.read.format("parquet").load("data/input/sales_total_parquet/*.parquet")

# COMMAND ----------

df_parquet.printSchema()

# COMMAND ----------

df_parquet.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read ORC Sales data
# MAGIC

# COMMAND ----------

# Read ORC Sales data

df_orc = spark.read.format("orc").load("data/input/sales_total_orc/*.orc")

# COMMAND ----------

df_orc.printSchema()

# COMMAND ----------

df_orc.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benefits of Columnar Storage
# MAGIC
# MAGIC

# COMMAND ----------

# Benefits of Columnar Storage

# Lets create a simple Python decorator - {get_time} to get the execution timings
# If you dont know about Python decorators - check out : https://www.geeksforgeeks.org/decorators-in-python/
import time

def get_time(func):
    def inner_get_time() -> str:
        start_time = time.time()
        func()
        end_time = time.time()
        return (f"Execution time: {(end_time - start_time)*1000} ms")
    print(inner_get_time())

# COMMAND ----------

@get_time
def x():
    df = spark.read.format("parquet").load("data/input/sales_data.parquet")
    df.count()

# COMMAND ----------

@get_time
def x():
    df = spark.read.format("parquet").load("data/input/sales_data.parquet")
    df.select("trx_id").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RECURSIVE READ

# COMMAND ----------

# BONUS TIP
# RECURSIVE READ

sales_recursive
|__ sales_1\1.parquet
|__ sales_1\sales_2\2.parquet



# COMMAND ----------

df_1 = spark.read.format("parquet").load("data/input/sales_recursive/sales_1/1.parquet")
df_1.show()

# COMMAND ----------

df_1 = spark.read.format("parquet").load("data/input/sales_recursive/sales_1/sales_2/2.parquet")
df_1.show()

# COMMAND ----------

df_1 = spark.read.format("parquet").option("recursiveFileLookup", True).load("data/input/sales_recursive/")
df_1.show()
