-- Databricks notebook source
create schema if not exists hr_gold


-- COMMAND ----------

use hr_gold


-- COMMAND ----------

select * from hr_silver.employees_clean limit 10

-- COMMAND ----------

create or replace temporary view 
employees_grouped_by_job_id
as 
select job_id, count(*) as employee_count
from hr_silver.employees_clean group by job_id
order by employee_count desc

-- COMMAND ----------

select * from  employees_grouped_by_job_id


-- COMMAND ----------

drop view employees_grouped_by_job_id

