# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

@dlt.create_view
def customers():
  return (
    spark.read.csv('s3://tamanna-kumari/input_files/customers.csv', header=True)
  )
  
@dlt.create_table(
  table_properties={
    "myCompanyPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def raw():
  return (
    spark.readStream.format("cloudFiles") \
      .option("cloudFiles.schemaLocation", "s3://tamanna-kumari/output_path/dlt") \
      .option("cloudFiles.format", "csv") \
      .option("cloudFiles.inferColumnTypes", "true") \
      .load("s3://tamanna-kumari/input_files/")
  )
  
@dlt.create_table(
  partition_cols=["name"],
  table_properties={
    "myCompanyPipeline.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def cleaned():
  df = dlt.read_stream("raw").join(dlt.read("customers"), ["name"], "left")
  df = df.select("id","name","customers.country")
  return df

@dlt.create_table(
  table_properties={
    "myCompanyPipeline.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def US():
  df = dlt.read_stream("cleaned").where("country == 'US'") 
  df = df.select(df.country, df.id,df.name)
  return df

@dlt.create_table(
  table_properties={
    "myCompanyPipeline.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def india():
  df = dlt.read_stream("cleaned").where("country == 'india'") 
  df = df.select(df.country, df.id,df.name)
  return df
