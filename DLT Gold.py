# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.view(
  name="employeeByCountry",
  comment="Group employees by Country")


def employeesByCountry():
    empCountry= dlt.read("employees_clean")

    groupedEmp= empCountry.groupBy('COUNTRY_NAME')

    return groupedEmp

    

# COMMAND ----------

@dlt.table(
    table_properties={
        "myPipeline.quality":"gold",
        "pipelines.autoOptimize.managed": "true"
    }
)

def employeesByCountry():
    empCountry= dlt.read("employees_clean")

    groupedEmp= empCountry.groupBy('COUNTRY_NAME')

    return groupedEmp.show()

# COMMAND ----------


