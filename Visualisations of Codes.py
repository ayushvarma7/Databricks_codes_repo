# Databricks notebook source
# MAGIC %md
# MAGIC ###DISPLAYING VISUALISATIONS

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("visualisations").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Employee Count for Each Job ID

# COMMAND ----------

table_name = 'hr_gold.emp_details'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT job_id, COUNT(*) as employee_count
    FROM {table_name}
    GROUP BY job_id
    ORDER BY employee_count DESC
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Plotting the results using matplotlib
plt.figure(figsize=(10, 6))
plt.bar(df['job_id'], df['employee_count'], color='skyblue')
plt.xlabel('Job ID')
plt.ylabel('Employee Count')
plt.title('Employee Count for Each Job ID')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Jobs per Department

# COMMAND ----------

table_name = 'hr_silver.job_details'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT COUNT(job_title) as jobs_per_department, department_name
    FROM {table_name}
    GROUP BY department_name
    ORDER BY jobs_per_department DESC
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Plotting the results using matplotlib as a pie chart
plt.figure(figsize=(8, 8))
plt.pie(df['jobs_per_department'], labels=df['department_name'], autopct='%1.1f%%', startangle=90, colors=plt.cm.Paired.colors)
plt.title('Jobs per Department')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Finding out minimum, maximum and average salary drawn department-wise

# COMMAND ----------

import plotly.express as px
import pandas as pd
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Replace 'your_table_name' with the actual table name in Databricks
table_name = 'hive_metastore.hr_silver.job_details'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT department_name, 
           MIN(min_salary) as minimum_salary, 
           MAX(max_salary) as maximum_salary, 
           ROUND(AVG((min_salary + max_salary)/2), 2) as average_salary 
    FROM {table_name}
    GROUP BY department_name
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Create an interactive bar chart with plotly
fig = px.bar(df, 
             x='department_name', 
             y=['minimum_salary', 'maximum_salary', 'average_salary'],
             labels={'value': 'Salary'},
             title='Salary Statistics by Department',
             hover_data={'value': ':.2f'},
             barmode='group')

# Show the interactive chart
fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Average Salary for Each Job Title in Each Department

# COMMAND ----------

table_name = 'hr_silver.job_details'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT job_title, department_name, ROUND(AVG((min_salary + max_salary)/2), 2) as average_salary
    FROM {table_name}
    GROUP BY job_title, department_name
    ORDER BY average_salary DESC
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Create a grouped bar chart with plotly
fig = px.bar(df, 
             x='job_title', 
             y='average_salary',
             color='department_name',
             labels={'average_salary': 'Average Salary'},
             title='Average Salary for Each Job Title in Each Department')

# Show the interactive chart
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Using table address to draw out relations

# COMMAND ----------

# MAGIC %md
# MAGIC ####Number of locations in each Region

# COMMAND ----------

# Replace 'your_table_name' with the actual table name in Databricks
table_name = 'hr_gold.address'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT REGION_NAME, COUNT(COUNTRY_NAME) as locations_in_region
    FROM {table_name}
    GROUP BY REGION_NAME
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Create an interactive bar chart with plotly
fig = px.bar(df, 
             x='REGION_NAME', 
             y='locations_in_region',
             labels={'locations_in_region': 'Number of Locations'},
             title='Number of Locations in Each Region')

# Show the interactive chart
fig.show()

# COMMAND ----------

table_name = 'hr_gold.address'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT COUNTRY_NAME, COUNT(city) as locations_per_city
    FROM {table_name}
    GROUP BY COUNTRY_NAME
    ORDER BY locations_per_city DESC
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Create a grouped bar chart with matplotlib
plt.figure(figsize=(12, 6))
plt.bar(df['COUNTRY_NAME'], df['locations_per_city'], color='skyblue')
plt.xlabel('Country Name')
plt.ylabel('Number of Locations per City')
plt.title('Number of Locations per City for Each Country')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cities Distribution Across Countries and Regions

# COMMAND ----------

table_name = 'hr_gold.address'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT city, COUNTRY_NAME, REGION_NAME
    FROM {table_name}
    ORDER BY REGION_NAME
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Create an interactive scatter plot with plotly
fig = px.scatter_geo(df,
                     locations='COUNTRY_NAME',
                     locationmode='country names',
                     text='city',
                     title='Cities Distribution Across Countries and Regions',
                     color='REGION_NAME',
                     projection='natural earth')

# Show the interactive chart
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Number of Direct Reports for Each Manager

# COMMAND ----------

table_name = 'hr_gold.emp_details'


sql_query = f'''
    SELECT MANAGER_NAME, COUNT(EMPLOYEE_ID) as Direct_Reports
    FROM {table_name}
    GROUP BY MANAGER_NAME
    ORDER BY Direct_Reports DESC
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Create an interactive bar chart with plotly
fig = px.bar(df, 
             x='MANAGER_NAME', 
             y='Direct_Reports',
             labels={'Direct_Reports': 'Number of Direct Reports'},
             title='Number of Direct Reports for Each Manager',
             color='Direct_Reports')

# Show the interactive chart
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Number of employees per job

# COMMAND ----------

table_name = 'hr_gold.emp_details'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT JOB_ID, COUNT(EMPLOYEE_ID) as employees_in_job
    FROM {table_name}
    GROUP BY JOB_ID
    ORDER BY employees_in_job DESC
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Create an interactive bar chart with plotly
fig = px.bar(df,
             x='JOB_ID',
             y='employees_in_job',
             labels={'employees_in_job': 'Number of Employees'},
             title='Number of Employees in Each Job',
             color='employees_in_job')

# Show the interactive chart
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Number of Jobs per Department

# COMMAND ----------

table_name = 'hr_silver.job_details'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT COUNT(job_title) as jobs_per_department, department_name
    FROM {table_name}
    GROUP BY department_name
    ORDER BY jobs_per_department DESC
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Create a rotated bar chart with plotly
fig = px.bar(df,
             x='jobs_per_department',
             y='department_name',
             labels={'jobs_per_department': 'Number of Jobs'},
             title='Number of Jobs per Department',
             orientation='h',  # Set orientation to 'h' for horizontal bars
             color='jobs_per_department')

# Show the interactive chart
fig.show()

# COMMAND ----------

table_name = 'hr_silver.job_details'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT COUNT(job_title) as jobs_per_department, department_name
    FROM {table_name}
    GROUP BY department_name
    ORDER BY jobs_per_department DESC
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Create a sunburst chart with plotly
fig = px.sunburst(df,
                  path=['department_name'],
                  values='jobs_per_department',
                  title='Jobs per Department')

# Show the interactive chart
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Using bigtable for querying

# COMMAND ----------

# MAGIC %md
# MAGIC ####Finding the employees drawing more salary than department average
# MAGIC

# COMMAND ----------

table_name = 'hr_gold.bigtable'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT department_name, COUNT(*) as salary_more_than_dept_average
    FROM {table_name}
    WHERE employee_salary > avg_dept_salary
    GROUP BY department_name
    ORDER BY salary_more_than_dept_average DESC
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Create a bar chart with plotly
fig = px.bar(df, 
             x='department_name', 
             y='salary_more_than_dept_average',
             labels={'salary_more_than_dept_average': 'Number of Employees'},
             title='Number of Employees with Salary More Than Department Average')

# Show the interactive chart
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Employees drawing the highest compensation difference

# COMMAND ----------

table_name = 'hr_gold.bigtable'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT DISTINCT EMPLOYEE_ID, NAME, 
           ROUND(((employee_salary - avg_dept_salary) / avg_dept_salary) * 100, 2) as compensation_difference,
           department_name
    FROM {table_name}
    WHERE employee_salary > avg_dept_salary
    ORDER BY compensation_difference DESC
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Create a scatter plot with plotly
fig = px.scatter(df, 
                 x='department_name', 
                 y='compensation_difference',
                 color='department_name',
                 size='compensation_difference',
                 labels={'compensation_difference': 'Compensation Difference (%)'},
                 title='Compensation Difference for Employees with Salary > Department Average')

# Show the interactive chart
fig.show()
