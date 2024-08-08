# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Dataframe

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
    ["018","104","Nancy Liu","29","","50000","2017-06-01"],
    ["019","103","Steve Che","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]
	
emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

emp = spark.createDataFrame(data=emp_data, schema=emp_schema)
emp.show()
emp.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unique data 

# COMMAND ----------

# Get unique data,
# select distinct emp.* from emp
emp.distinct().show()

# COMMAND ----------

# Unique of department_ids\n",
# select distinct department_id from emp\n",
emp.select('department_id').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WINDOW functions

# COMMAND ----------

# select *, max(salary) over(partition by department_id order by salary desc) as max_salary from emp_unique

from pyspark.sql.window import Window
from pyspark.sql import functions as F  

window_spec =  Window.partitionBy(F.col('department_id')).orderBy(F.col('salary').desc())

emp_1 = emp.withColumn('dept_max_salary', F.max(F.col('salary')).over(window_spec))
emp_1.show()

# COMMAND ----------

# 2nd highest salary of each department\n",
# select *, row_number() over(partition by department_id order by salary desc) as rn from emp_unique where rn = 2"

window_spec = Window.partitionBy(F.col('department_id')).orderBy(F.col('salary').desc())
emp_2 = emp.withColumn('rn', F.row_number().over(window_spec)).where('rn = 2')
emp_2.show()

# COMMAND ----------

# Window function using expr\n",
# select *, row_number() over(partition by department_id order by salary desc) as rn from emp_unique where rn = 2\n",

emp3 = emp.withColumn('rn', F.expr('row_number() over(PARTITION BY department_id ORDER BY salary DESC)')).where('rn = 2')
emp3.show()
