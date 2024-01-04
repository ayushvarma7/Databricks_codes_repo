# Databricks notebook source
input_path="s3://ayush-varma/Input/StreamingData/"
output_path="s3://ayush-varma/output/"

# COMMAND ----------

# MAGIC %md
# MAGIC creating a schema first where we will store our tables from autoloader

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists Ayush;

# COMMAND ----------

# MAGIC %sql
# MAGIC use Ayush

# COMMAND ----------

# MAGIC %md
# MAGIC each autoloader must have 
# MAGIC schemalocation folder to continually store updated schemas based upon incoming files,
# MAGIC checkpoint folder to continually store the checkpoints
# MAGIC in case of external table, one needs to mention the path of the table as well
# MAGIC and ofcourse the input folder to read data from
# MAGIC

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation", f"{output_path}/ayush/autoloader/schemalocation")
.load(f"{input_path}")
.writeStream
.option("checkpointLocation", f"{output_path}/ayush/autoloader/checkpoint")
.option("path", f"{output_path}/ayush/autoloader/output")  #since this is an external table
.table("ayush.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.ayush.autoloader

# COMMAND ----------

# MAGIC %md
# MAGIC This will infer the schema of the incoming files as well
# MAGIC

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation", f"{output_path}/ayush/autoloader1/schemalocation")
.option("cloudFiles.inferColumnTypes", True) # this makes the catalog infer the schema properly instead of infering all datatypes as string 
.load(f"{input_path}")
.writeStream
.option("checkpointLocation", f"{output_path}/ayush/autoloader1/checkpoint")
.option("path", f"{output_path}/ayush/autoloader1/output")  #since this is an external table
.table("ayush.autoloader1")
)

# COMMAND ----------

# MAGIC %md
# MAGIC but the problem is, if there comes another file with different schema  
# MAGIC it will fail with error UnknownFieldException since the **streaming stops when this error is encountered**
# MAGIC
# MAGIC **But note: before the stream throwing this error, the schema is updated in the schemalocation since schema inference is performed on this latest micro-batch of data**
# MAGIC
# MAGIC To handle that we need to use mergeSchema

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.ayush.autoloader1

# COMMAND ----------

# MAGIC %md
# MAGIC If we don't use merge schema we get an error of schema mismatch since schema is different wrt the files 
# MAGIC

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation", f"{output_path}/ayush/autoloader1/schemalocation")
.option("cloudFiles.inferColumnTypes", True)
.load(f"{input_path}")
.writeStream
.option("checkpointLocation", f"{output_path}/ayush/autoloader1/checkpoint")
.option("path", f"{output_path}/ayush/autoloader1/output")  #since this is an external table
.table("ayush.autoloader1")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Here the new data is not read since the schema was mismatched 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.ayush.autoloader1

# COMMAND ----------

# MAGIC %md
# MAGIC So we now use mergeSchema option -- we don't get an error!

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation", f"{output_path}/ayush/autoloader1/schemalocation")
.option("cloudFiles.inferColumnTypes", True)
.load(f"{input_path}")
.writeStream
.option("checkpointLocation", f"{output_path}/ayush/autoloader1/checkpoint")
.option("path", f"{output_path}/ayush/autoloader1/output") 
.option("mergeSchema", True)
.table("ayush.autoloader1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.ayush.autoloader1

# COMMAND ----------

# MAGIC %md
# MAGIC we can see now a new column, hobbies, that has been added!

# COMMAND ----------

# MAGIC %md
# MAGIC ####But the problem is, we have to MANUALLY stop the stream and rerun(execute it again) it to accept the schema
# MAGIC ####To make it automatic, we need to do schemaEvolutionMode as rescue which will make it automatic since now STREAM WON'T STOP 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC So in a production environment, we can't manually execute the code, we want it to happen automatically
# MAGIC we use schemaEvolutionMode as rescue which will not stop the stream
# MAGIC

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation", f"{output_path}/ayush/autoloader2/schemalocation")
.option("cloudFiles.inferColumnTypes", True)
.option("cloudFiles.schemaEvolutionMode", "rescue") # we have now used rescue mode in schemaEvolutionMode to not make it stop
.load(f"{input_path}")
.writeStream
.option("checkpointLocation", f"{output_path}/ayush/autoloader2/checkpoint")
.option("path", f"{output_path}/ayush/autoloader2/output")  
.table("ayush.autoloader2")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.ayush.autoloader2

# COMMAND ----------

# MAGIC %md
# MAGIC So here you can see, the stream did not stop and the excess data got stored in rescued_data column
