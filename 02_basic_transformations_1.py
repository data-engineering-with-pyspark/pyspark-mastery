# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('basic_transformations_1').master('local[*]').getOrCreate()

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
	
emp_schema = "employee_id string, departmet_id string, name string, age string, gender string, salary string, hire_date string"


# COMMAND ----------

emp = spark.createDataFrame(data=emp_data, schema=emp_schema)
emp.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Struct schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_pyspark = StringType([
    StructField('name', StringType(), True ),
    StructField('department_id', StringType(), True),
    StructField('name', StringType(), True ),
    StructField('age', StringType(), True ),
    StructField('gender', StringType(), True ),
    StructField('hire_date', StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Columns and expression (col & expr)
# MAGIC

# COMMAND ----------

# SELECT columns\n",
# select employee_id, name, age, salary from emp\n

from pyspark.sql import functions as F

emp_filter = emp.select(F.col('employee_id'), F.expr('name'), emp.age, emp.salary)
emp_filter.show()

# COMMAND ----------

# Using expr for select\n",
# select employee_id as emp_id, name, cast(age as int) as age, salary from emp_filtered

emp_filter_1 = emp_filter.select(F.expr('employee_id as emp_id'), emp_filter.name, F.expr('CAST(age as INT) as age'), emp_filter.salary )
emp_filter_1.show()
emp_filter_1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SelectExpr

# COMMAND ----------

emp_filter_2 = emp_filter.selectExpr('employee_id as emp_id', 'name', 'CAST(age as INT) as age', 'salary')
emp_filter_2.show()
emp_filter_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Where

# COMMAND ----------

# Filter emp based on Age > 30\n",
# select emp_id, name, age, salary from emp_casted where age > 30

emp_final = emp_filter_2.select('emp_id', 'name', 'age', 'salary').where('age > 30')
emp_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### write DF to csv
# MAGIC

# COMMAND ----------

emp_final.write.format('csv').mode('overwrite').save('dbfs:/FileStore/ease_with_data/emp.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### parse string schema to struct schema

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

str_schema = 'name string, age int'
struct_schema = _parse_datatype_string(str_schema)
struct_schema
