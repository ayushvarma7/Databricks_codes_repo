-- Databricks notebook source
-- MAGIC %md
-- MAGIC All the SQL Queries under SQL Warehouse
-- MAGIC

-- COMMAND ----------



select tpep_pickup_datetime as pickup_time, tpep_dropoff_datetime as dropoff_time, timestampdiff(minute, tpep_pickup_datetime, tpep_dropoff_datetime ) as trip_duration_in_minutes,  trip_distance, fare_amount
 from samples.nyctaxi.trips


-- select * from samples.nyctaxi.trips
-- name as taxi_trip_query

-- COMMAND ----------

select o_orderkey as orderid, o_orderstatus as status_of_delivery, o_totalprice as price, 
o_comment as review, o_shippriority as prime_member
 from samples.tpch.orders

-- name as order_query

-- COMMAND ----------

  

select p_name as dish_name, p_type as dish_type, p_brand as brand_name, p_retailprice as price, p_comment as review
 from samples.tpch.part

-- select * from samples.tpch.part
-- name as dish_query

-- COMMAND ----------

-- select * from samples.tpch.region

select s_name as supplier_name, s_address as address, s_acctbal as amount_due , s_phone as s_comment from samples.tpch.supplier

-- name as supplier_info_query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Route Revenue of cab 
-- MAGIC

-- COMMAND ----------


