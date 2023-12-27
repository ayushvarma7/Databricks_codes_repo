# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.types import *
import dlt
from delta.tables import DeltaTable


# COMMAND ----------

from pyspark.sql import SparkSession

csv_path="s3://ayush-varma/Input/StreamingData/"
delta_table_path="s3://ayush-varma/deltatable/"

spark = SparkSession.builder \
    .appName("delta-example") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .config("spark.databricks.delta.files", delta_table_path) \
    .getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("firstName", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("profession", StringType(), True),
    StructField("salary", IntegerType(), True),
])


# Read the CSV file as a Spark DataFrame
df = spark.read.option("header", "true").schema(schema).csv(csv_path)

# Assuming the dataframe columns are already appropriate, write the table as Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path)

# Create a Delta Table object for querying and further manipulation
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Register the Delta table as a Delta Live Table
delta_table.toDF().createOrReplaceLiveData(tableName="my_delta_live_table", isRegistered=True)

# Confirm that the Delta Live Table has been created successfully
spark.sql("SHOW LIVE TABLES").show(truncate=False)

# COMMAND ----------

delta_table.toDF().printSchema()

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY delta.`{}`".format(delta_table_path)).show(truncate=False))

# COMMAND ----------

display(df)

# COMMAND ----------

# Read the CSV file into a DataFrame
df = spark.read.format("csv").schema(schema).option("header", "true").load(csv_path)

# Write the DataFrame into the Delta table
df.write.format("delta").mode("append").save(delta_table_path)

# COMMAND ----------

display(df)

# COMMAND ----------

delta_table.toDF().printSchema()

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY delta.`{}`".format(delta_table_path)).show(truncate=False))
