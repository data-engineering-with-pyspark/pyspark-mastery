# Databricks notebook source
# MAGIC %md
# MAGIC ### SparkSession

# COMMAND ----------

from pyspark.sql import SparkSession

spark = (SparkSession
        .builder
        .appName('spark session')
        .master('local[*]')
        .getOrCreate()
        )

# COMMAND ----------

spark

# COMMAND ----------

emp_data = [
    ["001","101","Joh Doe","30","Male","50000","2015-01-01"],
    ["002","101","Jae Smith","25","Female","45000","2016-02-15"],
    ["003","102","Bob Brow","35","Male","55000","2014-05-01"],
    ["004","102","Alice Lee","28","Female","48000","2017-09-30"],
    ["005","103","Jack Cha","40","Male","60000","2013-04-01"],
    ["006","103","Jill Wog","32","Female","52000","2018-07-01"],
    ["007","101","James Johso","42","Male","70000","2012-03-15"],
    ["008","102","Kate Kim","29","Female","51000","2019-10-01"],
    ["009","103","Tom Ta","33","Male","58000","2016-06-01"],
    ["010","104","Lisa Lee","27","Female","47000","2018-08-01"],
    ["011","104","David Park","38","Male","65000","2015-11-01"],
    ["012","105","Susa Che","31","Female","54000","2017-02-15"],
    ["013","106","Bria Kim","45","Male","75000","2011-07-01"],
    ["014","107","Emily Lee","26","Female","46000","2019-01-01"],
    ["015","106","Michael Lee","37","Male","63000","2014-09-30"],
    ["016","107","Kelly Zhag","30","Female","49000","2018-04-01"],
    ["017","105","George Wag","34","Male","57000","2016-03-15"],
    ["018","104","acy Liu","29","Female","50000","2017-06-01"],
    ["019","103","Steve Che","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]
	
emp_schema = "employee_id string, departmet_id string, ame string, age string, gender string, salary string, hire_date string"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create dataframe

# COMMAND ----------

emp = spark.createDataFrame(data=emp_data, schema=emp_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check num. of partitions

# COMMAND ----------

emp.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show() (ACTION!!!)

# COMMAND ----------

emp.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation ( Sal > 50000)

# COMMAND ----------

emp_final = emp.where('salary > 50000')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate the num of parttion 

# COMMAND ----------

emp_final.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ### write df as CSV (ACTION!!!)

# COMMAND ----------

emp_final.write.format('csv').mode('overwrite').save('dbfs:/FileStore/ease_with_data/emp.csv')
