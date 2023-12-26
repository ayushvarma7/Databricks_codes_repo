-- Databricks notebook source
create schema if not exists xAyush;
create schema if not exists yAyush;

-- COMMAND ----------



create table if not exists xAyush.users(
  -- id integer primary key,
  -- name varchar(30) not null
);


create table if not exists yAyush.cars(
);

-- COMMAND ----------

GRANT select on table xAyush.users to `ayush123@gmail.com`
GRANT select on table xAyush.users to `ayush456@gmail.com`

-- COMMAND ----------

GRANT SELECT, INSERT, UPDATE, DELETE ON xAyush.users TO b;
GRANT SELECT ON yAyush.cars TO b;
GRANT SELECT, INSERT, UPDATE, DELETE ON xAyush.users TO a;
GRANT SELECT ON yAyush.cars TO a;

