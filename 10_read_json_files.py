# Databricks notebook source
# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Reading and Parsing JSON Files/Data")
    .getOrCreate()
)

spark

# COMMAND ----------

# Read Single line JSON file

df_single = spark.read.format("json").load("dbfs:/FileStore/ease_with_data/datasets/order_singleline-1.json")

# COMMAND ----------

df_single.printSchema()

# COMMAND ----------

df_single.show()

# COMMAND ----------

# Read Multiline JSON file

df_multi = spark.read.format("json").option("multiLine", True).load("dbfs:/FileStore/ease_with_data/datasets/order_multiline-1.json")

# COMMAND ----------

df_multi.printSchema()

# COMMAND ----------

df_multi.show()

# COMMAND ----------

df = spark.read.format("text").load("dbfs:/FileStore/ease_with_data/datasets/order_singleline-1.json")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

# With Schema

_schema = "customer_id string, order_id string, contact array<long>"

df_schema = spark.read.format("json").schema(_schema).load("dbfs:/FileStore/ease_with_data/datasets/order_singleline-1.json")

# COMMAND ----------

df_schema.show()

# COMMAND ----------

df_schema.printSchema()

# COMMAND ----------

_schema = "contact array<string>, customer_id string, order_id string, order_line_items array<struct<amount double, item_id string, qty long>>"

# COMMAND ----------

df_schema_new = spark.read.format("json").schema(_schema).load("dbfs:/FileStore/ease_with_data/datasets/order_singleline-1.json")

# COMMAND ----------

df_schema_new.printSchema()

# COMMAND ----------

df_schema_new.show()

# COMMAND ----------

# Function from_json to read from a column

_schema = "contact array<string>, customer_id string, order_id string, order_line_items array<struct<amount double, item_id string, qty long>>"

from pyspark.sql.functions import from_json

df_expanded = df.withColumn("parsed", from_json(df.value, _schema))


# COMMAND ----------

df_expanded.printSchema()

# COMMAND ----------

display(df_expanded)

# COMMAND ----------

# Function to_json to parse a JSON string
from pyspark.sql.functions import to_json

df_unparsed = df_expanded.withColumn("unparsed", to_json(df_expanded.parsed))

# COMMAND ----------

df_unparsed.printSchema()

# COMMAND ----------

df_unparsed.select("unparsed").show(truncate=False)

# COMMAND ----------

# Get values from Parsed JSON

df_1 = df_expanded.select("parsed.*")

# COMMAND ----------

from pyspark.sql.functions import explode

df_2 = df_1.withColumn("expanded_line_items", explode("order_line_items"))

# COMMAND ----------

df_2.show()

# COMMAND ----------

df_3 = df_2.select("contact", "customer_id", "order_id", "expanded_line_items.*")

# COMMAND ----------

df_3.show()

# COMMAND ----------

# Explode Array fields
df_final = df_3.withColumn("contact_expanded", explode("contact"))


# COMMAND ----------

df_final.printSchema()

# COMMAND ----------

df_final.drop("contact").show()
