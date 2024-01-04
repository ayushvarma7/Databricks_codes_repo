# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists hr_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC use hr_bronze

# COMMAND ----------

input_path="s3://ayush-varma/databricks_project/input"
output_path="s3://ayush-varma/databricks_project/output/"


# COMMAND ----------

print(input_path)
display(dbutils.fs.ls(input_path))

# COMMAND ----------

# (spark.readStream
# .format("cloudFiles")
# .option("cloudFiles.format","csv")
# .option("cloudFiles.schemaLocation", f"{output_path}/bronze/countries/schemalocation")
# .option("cloudFiles.inferColumnTypes", True)
# .option("cloudFiles.schemaEvolutionMode", "rescue") # we have now used rescue mode in schemaEvolutionMode to not make it stop
# .load(f"{input_path}")
# .writeStream
# .option("checkpointLocation", f"{output_path}/bronze/countries/checkpoint")
# .option("path", f"{output_path}/bronze/countries/output")  
# .table("hr_bronze.countries_raw")
# )

# COMMAND ----------

# %sql 
# select * from hive_metastore.hr_bronze.countries_raw

# COMMAND ----------

countries_df=spark.read.csv(path=f"{input_path}/COUNTRIES.csv", header=True, inferSchema=True)

departments_df=spark.read.csv(path=f"{input_path}/DEPARTMENTS.csv", header=True, inferSchema=True)

employees_df=spark.read.csv(path=f"{input_path}/EMPLOYEES.csv", header=True, inferSchema=True)

job_history_df=spark.read.csv(path=f"{input_path}/JOB_HISTORY.csv", header=True, inferSchema=True)

jobs_df=spark.read.csv(path=f"{input_path}/JOBS.csv", header=True, inferSchema=True)

locations_df=spark.read.csv(path=f"{input_path}/LOCATIONS.csv", header=True, inferSchema=True)

regions_df=spark.read.csv(path=f"{input_path}/REGIONS.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing dataframes into delta tables

# COMMAND ----------

countries_df.write.saveAsTable("hr_bronze.countries_raw")
departments_df.write.saveAsTable("hr_bronze.departments_raw")
employees_df.write.saveAsTable("hr_bronze.employees_raw")
job_history_df.write.saveAsTable("hr_bronze.job_history_raw")
jobs_df.write.saveAsTable("hr_bronze.jobs_raw")
locations_df.write.saveAsTable("hr_bronze.locations_raw")
regions_df.write.saveAsTable("hr_bronze.regions_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hr_bronze.jobs_raw
