# Databricks notebook source
# MAGIC %md
# MAGIC ### Create dataframe

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

emp = spark.createDataFrame(data=emp_data, schema=emp_schema)
emp.show()
emp.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Casting columns

# COMMAND ----------

# select employee_id, name, age, cast(salary as double) as salary from emp
from pyspark.sql import functions as F

emp_cast = emp.select('employee_id', 'name', 'age', F.col('salary').cast('double'))
emp_cast.show()
emp_cast.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Columns

# COMMAND ----------

# select employee_id, name, age, salary, (salary * 0.2) as tax 
emp_tax = emp_cast.withColumn('tax', F.col('salary')*0.2)
emp_tax.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Literals

# COMMAND ----------

# select employee_id, name, age, salary, tax, 1 as col_one, 'two' as col_two 
emp_new_cols = emp_tax.withColumn('col_one', F.lit(1)).withColumn('col_two', F.lit('two'))
emp_new_cols.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming Columns

# COMMAND ----------

# select employee_id as emp_id, name, age, salary, tax, columnOne, columnTwo 

emp_rn = emp_new_cols.withColumnRenamed('employee_id', 'emp_id')
emp_rn.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Remove Column

# COMMAND ----------

emp_drop = emp_rn.drop('col_one', 'col_two')
emp_drop.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter data

# COMMAND ----------

# select employee_id as emp_id, name, age, salary, tax, columnOne from emp_col_dropped where tax > 10000
emp_filter = emp_drop.where('tax > 10000')
emp_filter.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMIT data

# COMMAND ----------

# select employee_id as emp_id, name, age, salary, tax, columnOne from emp_filtered limit 5
emp_limit = emp_filter.limit(5)
emp_limit.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add multiple columns

# COMMAND ----------

columns = {
    'tax': F.col('salary') * 0.2,
    'col_one': F.lit(1),
    'col_two': F.lit('two') 
}
df_final = emp.withColumns(columns)
df_final.show()
