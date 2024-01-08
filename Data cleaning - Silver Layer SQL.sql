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

-- MAGIC %md 
-- MAGIC Finding the number of distinct records

-- COMMAND ----------

select distinct(count(*)) from employees_clean

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Counting the number of rows having no commission percentage

-- COMMAND ----------

select count(*) as no_commission_percentage from hr_silver.employees_clean
where COMMISSION_PCT=' - '


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Replacing the commission percentage of such rows to commission percentage= 0 

-- COMMAND ----------

update employees_clean set
COMMISSION_PCT=0 where COMMISSION_PCT = ' - '

-- COMMAND ----------

select EMPLOYEE_ID, concat(FIRST_NAME, ' ', LAST_NAME ) as Full_NAME from hive_metastore.hr_silver.employees_clean limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC updating rows where no manager is assigned

-- COMMAND ----------

update employees_clean set MANAGER_ID = "NULL"
where MANAGER_ID= ' - '

-- COMMAND ----------

select * from hr_silver.employees_clean

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

-- MAGIC %md
-- MAGIC Deleting rows where both postal_code and state_province are not present

-- COMMAND ----------

delete from hr_silver.location_cleaned 
where POSTAL_CODE="NULL" and
STATE_PROVINCE="NULL"


-- COMMAND ----------

-- select * from
