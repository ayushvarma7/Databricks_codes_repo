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


-- COMMAND ----------

-- CREATE TEMPORARY VIEW employees_job_details AS
-- SELECT ec.employee_id, ec.salary, ec.department_id, jd.job_title, jd.min_salary, jd.max_salary
-- FROM hive_metastore.hr_silver.employees_clean ec
-- JOIN hive_metastore.hr_silver.job_details jd
-- ON ec.job_id=jd.job_id
-- GROUP BY ec.employee_id


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Checking the total number of employee records for job_details

-- COMMAND ----------

select employee_id from hive_metastore.hr_silver.job_details 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #WORKING ON JOB_DETAILS TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Checking the distinct employee records for job_details

-- COMMAND ----------

select distinct employee_id from hr_silver.job_details

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC This means we have employees having multiple jobs before

-- COMMAND ----------

select * from hr_silver.job_details order by employee_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Creating a view which shows employees and number of jobs done by them

-- COMMAND ----------

select employee_id, count(job_title) as jobs_by_employee from hr_silver.job_details group by employee_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Finding out employees who have done multiple jobs in company

-- COMMAND ----------

select employee_id, count(job_title) as jobs_by_employee from hr_silver.job_details group by employee_id having jobs_by_employee > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Counting number of jobs per department

-- COMMAND ----------

select department_name, count(job_title) as jobs_per_department  from hr_silver.job_details group by department_name order by jobs_per_department desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Finding out minimum, maximum and average salary drawn department-wise

-- COMMAND ----------

SELECT department_name, MIN(min_salary) as minimum_salary, MAX(max_salary) as maximum_salary, round(AVG((min_salary + max_salary)/2),2) as average_salary FROM hive_metastore.hr_silver.job_details GROUP BY department_name;


-- COMMAND ----------

SELECT job_title, department_name, round(AVG((min_salary + max_salary)/2),2) as average_salary FROM hr_silver.job_details GROUP BY job_title, department_name ORDER BY average_salary DESC;


-- COMMAND ----------

-- select job_title, date_diff(end_date,start_date) as tenure from hr_silver.job_details order by tenure

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####DRAWING FEATURES FROM ADDRESS TABLE

-- COMMAND ----------

create or replace table address as
select location_id, street_address, city, COUNTRY_NAME, REGION_NAME from hr_silver.full_address 

-- COMMAND ----------

select * from address

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Find number of locations per region
-- MAGIC

-- COMMAND ----------

select REGION_NAME,  count(COUNTRY_NAME) as `locations_in_region` from hr_gold.address group by REGION_NAME

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Find number of locations per country
-- MAGIC

-- COMMAND ----------

select COUNTRY_NAME, count(city) as `locations_per_city` from hr_gold.address group by COUNTRY_NAME order by locations_per_city desc

-- COMMAND ----------

select city, COUNTRY_NAME, REGION_NAME from hr_gold.address order by REGION_NAME 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Finding relations in employees_jobs table
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Counting number of employees under per manager

-- COMMAND ----------

select MANAGER_NAME, count(EMPLOYEE_ID) as Direct_Reports from hr_silver.emp_details group by MANAGER_NAME order by Direct_Reports desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Counting number of employees per job

-- COMMAND ----------

select JOB_ID, count(EMPLOYEE_ID) as employees_in_job from hr_gold.emp_details group by JOB_ID order by employees_in_job desc 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Querying using the bigtable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Finding the employees drawing more salary than department average
-- MAGIC

-- COMMAND ----------

select department_name, count(*) as `salary more than dept average` from hr_gold.bigtable where employee_salary > avg_dept_salary group by department_name order by `salary more than dept average` desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Employees drawing the highest compensation difference

-- COMMAND ----------

select distinct(EMPLOYEE_ID), NAME,  round(((employee_salary - avg_dept_salary )/avg_dept_salary)*100, 2) as `compensation_difference (%)`, department_name from hr_gold.bigtable where employee_salary > avg_dept_salary order by `compensation_difference (%)` desc
