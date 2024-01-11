# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

input_path="s3://ayush-varma/databricks_project/input"
output_path="s3://ayush-varma/databricks_project/output/"

# COMMAND ----------

@dlt.table(
    table_properties={
        "myPipeline.quality":"bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)

def countries():
   countries_df=(spark.read.csv(path=f"{input_path}/COUNTRIES.csv", header=True, inferSchema=True))

   return countries_df

# COMMAND ----------

@dlt.table(
    table_properties={
         "myPipeline.quality":"bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)

def departments():
    departments_df=spark.read.csv(path=f"{input_path}/DEPARTMENTS.csv", header=True, inferSchema=True)

    return departments_df

# COMMAND ----------

@dlt.table(
    table_properties={
         "myPipeline.quality":"bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)

def employees():
    employees_df=spark.read.csv(path=f"{input_path}/EMPLOYEES.csv", header=True, inferSchema=True)

    return employees_df

# COMMAND ----------

@dlt.table(
    table_properties={
         "myPipeline.quality":"bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)

def job_history():
    job_history_df=spark.read.csv(path=f"{input_path}/JOB_HISTORY.csv", header=True, inferSchema=True)

    return job_history_df

# COMMAND ----------

@dlt.table(
    table_properties={
         "myPipeline.quality":"bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)

def jobs():
    jobs_df=spark.read.csv(path=f"{input_path}/JOBS.csv", header=True, inferSchema=True)

    return jobs_df

# COMMAND ----------

@dlt.table(
    table_properties={
         "myPipeline.quality":"bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)

def locations():
    locations_df=spark.read.csv(path=f"{input_path}/LOCATIONS.csv", header=True, inferSchema=True)

    return locations_df

# COMMAND ----------

@dlt.table(
    table_properties={
         "myPipeline.quality":"bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)

def regions():
    regions_df=spark.read.csv(path=f"{input_path}/REGIONS.csv", header=True, inferSchema=True)

    return regions_df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating cleaned data in silver layer

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a table consisting of full address, joined on locations, countries and regions table

# COMMAND ----------

@dlt.table(
    table_properties={
        "myPipeline.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

def full_address():
    location_df=dlt.read("locations").dropDuplicates()

    # replacing '-'  with 'null' values
    location_df=location_df.withColumn("STATE_PROVINCE", when(location_df["STATE_PROVINCE"] == " - ", None).otherwise(location_df["STATE_PROVINCE"]))

    location_df=location_df.withColumn("POSTAL_CODE", when(location_df["POSTAL_CODE"] == " - ", None).otherwise(location_df["POSTAL_CODE"]))
    
    locations_cleaned_df=location_df.filter(
        (col("STATE_PROVINCE").isNotNull()) &
        (col("POSTAL_CODE").isNotNull()) &
        (col("LOCATION_ID").isNotNull()) &
        (col("CITY").isNotNull()) &
        (col("POSTAL_CODE").isNotNull()) 
    )

    regions_df=dlt.read("regions").dropDuplicates()

    regions_cleaned_df=regions_df.filter(
        (col("REGION_ID").isNotNull()) &
        (col("REGION_NAME").isNotNull())
    )

    countries_df=dlt.read("countries").dropDuplicates()

    countries_cleaned_df=countries_df.filter(
        (col("COUNTRY_ID").isNotNull()) &
        (col("COUNTRY_NAME").isNotNull()) &
        ((col("REGION_ID").isNotNull()))
    )

    # cleaning done, now performing joining of these 3 dfs

    # joining countries and regions on REGION_ID
    full_address_df=countries_cleaned_df.join(regions_cleaned_df, ["REGION_ID"], "left")

    # joining this df with locations

    full_address_df=locations_cleaned_df.join(full_address_df, ["COUNTRY_ID"], "right")


    return full_address_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Job details table

# COMMAND ----------

@dlt.table(
    table_properties={
        "myPipeline.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

def job_details():
    job_history_df=dlt.read("job_history").dropDuplicates()

    job_history_df=job_history_df.filter(
        (col("EMPLOYEE_ID").isNotNull()) &
        (col("START_DATE").isNotNull()) &
        (col("END_DATE").isNotNull()) &
        (col("JOB_ID").isNotNull()) &
        (col("DEPARTMENT_ID").isNotNull()) 
    )

    jobs_df=dlt.read("jobs").dropDuplicates()

    jobs_df=jobs_df.filter(
         (col("JOB_ID").isNotNull()) &
        (col("JOB_TITLE").isNotNull()) &
        (col("MIN_SALARY").isNotNull()) &
        (col("MAX_SALARY").isNotNull()) 
    
    )
    

    departments_df=dlt.read("departments").dropDuplicates()

    departments_df=departments_df.filter(
         (col("DEPARTMENT_ID").isNotNull()) &
        (col("DEPARTMENT_NAME").isNotNull()) &
        (col("MANAGER_ID").isNotNull()) &
        (col("LOCATION_ID").isNotNull()) 
    )


    job_details_df= jobs_df.join(job_history_df, ["JOB_ID"], "left")

    job_details_df= job_details_df.join(departments_df, ["DEPARTMENT_ID"], "inner")

    return job_details_df

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a clean employees table

# COMMAND ----------

@dlt.table(
    table_properties={
        "myPipeline.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)


def employees_clean():
    employees_df=dlt.read("employees").dropDuplicates()

    # wherever the commission is defined null, replacing that value with 0
    employees_df=employees_df.withColumn("COMMISSION_PCT", when(employees_df["COMMISSION_PCT"]==' - ', 0).otherwise(employees_df["COMMISSION_PCT"]))

    employees_df=employees_df.withColumn("MANAGER_ID", when(employees_df["MANAGER_ID"] ==' - ', None).otherwise(employees_df["MANAGER_ID"]))

    employees_clean_df=employees_df.filter(
        (col("EMPLOYEE_ID").isNotNull()) &
        (col("FIRST_NAME").isNotNull()) &
        (col("LAST_NAME").isNotNull()) &
        (col("EMAIL").isNotNull()) &
        (col("PHONE_NUMBER").isNotNull()) &
        (col("HIRE_DATE").isNotNull()) &
        (col("JOB_ID").isNotNull()) &
        (col("SALARY") > 0 ) &
        (col("COMMISSION_PCT") >= 0) &
        (col("MANAGER_ID").isNotNull()) &
        (col("DEPARTMENT_ID").isNotNull())  
    )



    return employees_clean_df
