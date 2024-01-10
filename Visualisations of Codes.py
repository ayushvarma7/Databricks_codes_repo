# Databricks notebook source
# MAGIC %md
# MAGIC ###DISPLAYING VISUALISATIONS

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# COMMAND ----------



# Replace 'your_table_name' with the actual table name in Databricks
table_name = 'hr_silver.emp_details'

# Replace your SQL query with the actual query
sql_query = f'''
    SELECT MANAGER_NAME, COUNT(EMPLOYEE_ID) as Direct_Reports
    FROM {table_name}
    GROUP BY MANAGER_NAME
    ORDER BY Direct_Reports DESC
'''

# Execute the SQL query and fetch the results into a Pandas DataFrame
df = spark.sql(sql_query).toPandas()

# Plotting the results using matplotlib
plt.figure(figsize=(10, 6))
plt.bar(df['MANAGER_NAME'], df['Direct_Reports'], color='skyblue')
plt.xlabel('Manager Name')
plt.ylabel('Number of Direct Reports')
plt.title('Number of Direct Reports for Each Manager')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md 
# MAGIC

# COMMAND ----------



# Replace 'your_table_name' with the actual table name in Databricks
table_name = 'hr_silver.employees_clean'

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

