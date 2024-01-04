-- Databricks notebook source
create schema if not exists hr_silver

-- COMMAND ----------

use hr_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###cleaning table employees

-- COMMAND ----------

create table employees_clean as 
select * from hr_bronze.employees_raw

-- COMMAND ----------

select * from hive_metastore.hr_silver.employees_clean

-- COMMAND ----------

select count(*) from hr_silver.employees_clean
where COMMISSION_PCT=' - '


-- COMMAND ----------

update employees_clean set
COMMISSION_PCT=0 where COMMISSION_PCT = ' - '

-- COMMAND ----------

select * from hr_silver.employees_clean

-- COMMAND ----------

update employees_clean set MANAGER_ID = "NULL"
where MANAGER_ID= ' - '

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cleaning locations table 
-- MAGIC

-- COMMAND ----------

create table hr_silver.location_cleaned as
select * from hr_bronze.locations_raw


-- COMMAND ----------

select * from location_cleaned

-- COMMAND ----------

update location_cleaned set STATE_PROVINCE="NULL"
where STATE_PROVINCE=' - '

-- COMMAND ----------

update location_cleaned set POSTAL_CODE="NULL"
where POSTAL_CODE= ' - '

-- COMMAND ----------

select * from hr_silver.location_cleaned


-- COMMAND ----------


