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
    ["018","104","Nancy Liu","29","","50000","2017-06-01"],
    ["019","103","Steve Che","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]
	
emp_schema = "employee_id string, departmet_id string, name string, age string, gender string, salary string, hire_date string"

emp = spark.createDataFrame(data=emp_data, schema=emp_schema)
emp.show()
emp.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### CASE, WHEN

# COMMAND ----------

# select employee_id, name, age, salary, gender,
# case when gender = 'Male' then 'M' when gender = 'Female' then 'F' else null end as new_gender, hire_date from emp
from pyspark.sql import functions as F

emp_gender = emp.withColumn('new_gender', F.when(F.col('gender')=='Male', F.lit('M'))\
                                            .when(F.col('gender')=='Female', F.lit('F'))\
                                            .otherwise(None))

emp_gender.show()

# COMMAND ----------

# using expr

emp_gender_1 = emp.withColumn('new_gender', F.expr('''CASE WHEN gender="Male" THEN "M" 
                                                        WHEN gender="Female" THEN "F" 
                                                        ELSE null
                                                    END '''))
emp_gender_1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replace in Strings

# COMMAND ----------

# select employee_id, name, replace(name, 'J', 'Z') as new_name, age, salary, gender, new_gender, hire_date

emp_new_name = emp_gender.withColumn('new_name', F.regexp_replace(F.col('name'), 'J', 'Z') )
emp_new_name.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert Date

# COMMAND ----------

#select *,  to_date(hire_date, 'YYYY-MM-DD') as hire_date from emp_new_name

emp_date_fix = emp_new_name.withColumn('hire_date', F.to_date(F.col('hire_date'), 'yyyy-MM-dd'))
emp_date_fix.show()
emp_date_fix.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Current Date & Time Columns

# COMMAND ----------

# Add current_date, current_timestamp, extract year 

emp_ts = emp_date_fix.withColumn('date_now', F.current_date())\
                      .withColumn('ts_now', F.current_timestamp())

emp_ts.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Null records

# COMMAND ----------

emp_1 = emp_ts.na.drop()
emp_1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix Null values

# COMMAND ----------

# select *, nvl('new_gender', 'O') as new_gender from emp

emp_null = emp_ts.withColumn('new_gender', F.coalesce(F.col('new_gender'), F.lit('O')))
emp_null.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop old columns and Fix new column names

# COMMAND ----------

emp_final = emp_null.drop('name', 'gender').withColumnRenamed('new_name', 'name').withColumnRenamed('new_gender', 'gender')
emp_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert date into String and extract date information

# COMMAND ----------

emp_fix = emp_final.withColumn('year', F.date_format(F.col('ts_now'), 'yyyy'))\
                    .withColumn('month', F.date_format(F.col('ts_now'), 'MMM'))\
                    .withColumn('data_dt', F.concat(F.date_format(F.col('ts_now'), 'yyyy'), F.date_format(F.col('ts_now'), 'MM'), F.date_format(F.col('ts_now'), 'dd')) )
emp_fix.show()
