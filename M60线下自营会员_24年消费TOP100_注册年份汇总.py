# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bigdata.ky_databricks_demo.`M60线下自营会员_24年消费TOP100_注册年份汇总`
# MAGIC (
# MAGIC   `注册入会年份` STRING,
# MAGIC   `会员人数` BIGINT,
# MAGIC   p_date STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY ( p_date )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.appendOnly' = 'false'
# MAGIC )
