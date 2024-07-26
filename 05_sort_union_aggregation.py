# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Dataframe

# COMMAND ----------

emp_data_1 = [
		["001","101","John Doe","30","Male","50000","2015-01-01"],
		["002","101","Jane Smith","25","Female","45000","2016-02-15"],
		["003","102","Bob Brown","35","Male","55000","2014-05-01"],
		["004","102","Alice Lee","28","Female","48000","2017-09-30"],
		["005","103","Jack Chan","40","Male","60000","2013-04-01"],
		["006","103","Jill Wong","32","Female","52000","2018-07-01"],
		["007","101","James Johnson","42","Male","70000","2012-03-15"],
		["008","102","Kate Kim","29","Female","51000","2019-10-01"],
		["009","103","Tom Tan","33","Male","58000","2016-06-01"],
		["010","104","Lisa Lee","27","Female","47000","2018-08-01"],
]
emp_data_2 = [
		["011","104","David Park","38","Male","65000","2015-11-01"],
		["012","105","Susan Chen","31","Female","54000","2017-02-15"],
		["013","106","Brian Kim","45","Male","75000","2011-07-01"],
		["014","107","Emily Lee","26","Female","46000","2019-01-01"],
		["015","106","Michael Lee","37","Male","63000","2014-09-30"],
		["016","107","Kelly Zhang","30","Female","49000","2018-04-01"],
		["017","105","George Wang","34","Male","57000","2016-03-15"],
		["018","104","Nancy Liu","29","","50000","2017-06-01"],
		["019","103","Steven Chen","36","Male","62000","2015-08-01"],
		["020","102","Grace Kim","32","Female","53000","2018-11-01"],
]

emp_schema = "employee_id string, departmet_id string, name string, age string, gender string, salary string, hire_date string"

emp_data_1 = spark.createDataFrame(data=emp_data_1, schema=emp_schema)
emp_data_1.show()
emp_data_2 = spark.createDataFrame(data=emp_data_2, schema=emp_schema)
emp_data_2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION, UNION ALL & UNION BY NAME

# COMMAND ----------

# UNION :- Removes duplicate rows
emp_un = emp_data_1.union(emp_data_2)
emp_un.show()

# UNION ALL :- Retains duplicate rows
emp_un_all = emp_data_1.unionAll(emp_data_2)
emp_un_all.show()

# UNION BY NAME :- merges 2 dataframes based on column names
emp_un_name = emp_data_1.unionByName(emp_data_2)
emp_un_name.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SORT

# COMMAND ----------

# Sort the emp data based on desc Salary\n",
# select * from emp order by salary desc\
from pyspark.sql import functions as F

emp_sort = emp_un.orderBy(F.col('salary').desc())
emp_sort.show()
emp_sort_a = emp_un.orderBy(F.col('salary').asc())
emp_sort_a.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregation

# COMMAND ----------

# COUNT
# select dept_id, count(employee_id) as total_dept_count from emp_sort group by dept_id 

dept_cnt = emp_sort.groupBy('departmet_id').agg(F.count('employee_id').alias('total_dept_count'))
dept_cnt.show()

# COMMAND ----------

# SUM
# select dept_id, sum(salary) as total_dept_salary from emp_sorted group by dept_id

dept_sal = emp_sort.groupBy('departmet_id').agg(F.sum('salary').alias('total_dept_salary'))
dept_sal.show()

# COMMAND ----------

# HAVING
# select dept_id, avg(salary) as avg_dept_salary from emp_sort group by dept_id having avg(salary) > 50000

dept_avg = emp_sort.groupBy('departmet_id').agg(F.avg('salary').alias('avg_dept_salary')).where('avg_dept_salary > 50000')
dept_avg.show()
