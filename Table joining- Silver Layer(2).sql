-- Databricks notebook source
use hr_silver

-- COMMAND ----------

-- create table location

-- COMMAND ----------

-- drop table location

-- COMMAND ----------

select * from hr_bronze.countries_raw

-- COMMAND ----------

select * from hr_bronze.regions_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Since both countries table and region tables have common columns,
-- MAGIC so we left join it on countries

-- COMMAND ----------

create table hr_silver.misc as
select countries_raw.COUNTRY_ID, countries_raw.COUNTRY_NAME, countries_raw.REGION_ID, regions_raw.REGION_NAME  from hr_bronze.countries_raw left join
hr_bronze.regions_raw on countries_raw.REGION_ID=regions_raw.REGION_ID

-- COMMAND ----------

select * from hr_silver.misc

-- COMMAND ----------

-- drop table hr_silver.misc;

-- COMMAND ----------

select * from location_cleaned;

-- COMMAND ----------

create table hr_silver.full_address as
select location_cleaned.location_id, location_cleaned.street_address, location_cleaned.POSTAL_CODE, location_cleaned.CITY, location_cleaned.STATE_PROVINCE, location_cleaned.COUNTRY_ID, misc.COUNTRY_NAME, misc.REGION_ID from location_cleaned left join hr_silver.misc
on location_cleaned.country_id= hr_silver.misc.country_id

-- COMMAND ----------

select * from full_address  

-- COMMAND ----------

select count(*) as frequency, COUNTRY_NAME from full_address group by COUNTRY_NAME order by frequency desc


-- COMMAND ----------

-- select count(*) as `locations per region` from full_address group by REGION_ID

-- COMMAND ----------

-- create table hr_silver.full_address as
-- select location_cleaned.location_id, location_cleaned.street_address, location_cleaned.postal_code
-- from location_cleaned left join hr_silver.misc
-- on location_cleaned.country_id= hr_silver.misc.country_id;

-- COMMAND ----------

-- drop table full_address

-- COMMAND ----------

select * from hr_bronze.job_history_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Joining tables job_history, jobs, and department
-- MAGIC on **1) job_history_raw.JOB_ID=jobs_raw.JOB_ID**
-- MAGIC and **2) job_history_raw.department_id = departments_raw.department_id**
-- MAGIC respectively

-- COMMAND ----------

create table hr_silver.job_details as
select hr_bronze.job_history_raw.employee_id, hr_bronze.job_history_raw.start_date, hr_bronze.job_history_raw.end_date, hr_bronze.job_history_raw.job_id, hr_bronze.jobs_raw.job_title, hr_bronze.jobs_raw.min_salary, hr_bronze.jobs_raw.max_salary, hr_bronze.job_history_raw.department_id,  hr_bronze.departments_raw.department_name  from hr_bronze.job_history_raw left join hr_bronze.jobs_raw
on job_history_raw.JOB_ID=jobs_raw.JOB_ID
join hr_bronze.departments_raw on job_history_raw.department_id = hr_bronze.departments_raw.department_id

-- COMMAND ----------

select * from job_details

-- COMMAND ----------

-- drop table job_details

-- COMMAND ----------

-- select * from table_changes('employees_clean', 1 )
describe history employees_clean


-- COMMAND ----------

select * from employees_clean

-- COMMAND ----------

-- MAGIC %md
-- MAGIC drop the unrequired table
-- MAGIC

-- COMMAND ----------

drop table misc
