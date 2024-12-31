# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/M60线下会员24年消费TOP100_自营__消费明细_20241230-1.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
# df = spark.read.format(file_type) \
#   .option("inferSchema", infer_schema) \
#   .option("header", first_row_is_header) \
#   .option("sep", delimiter) \
#   .load(file_location)

# Modify column types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

schema = StructType([
    StructField("消费年份", StringType(), True),
    StructField("baseid", StringType(), True),
    StructField("会员编号", StringType(), True),
    StructField("管理门店名称", StringType(), True),
    StructField("管理门店渠道", StringType(), True),
    StructField("订单编号", StringType(), True),
    StructField("订单类型", StringType(), True),
    StructField("支付日期", DateType(), True),
    StructField("商品款色码SKU", StringType(), True),
    StructField("商品中类", StringType(), True),
    StructField("个人累计消费金额", DoubleType(), True),
    StructField("商品数量", IntegerType(), True),
    StructField("商品消费金额", DoubleType(), True),
    StructField("商品吊牌价", DoubleType(), True),
    StructField("商品折扣率", DoubleType(), True)
    # Add other fields as necessary
])

df = spark.read.format(file_type) \
  .schema(schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------


# Create a view or table

temp_table_name = "m60_offline_member_24year_sales_top100_temp"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `m60_offline_member_24year_sales_top100_temp`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

# permanent_table_name = "m60_offline_member_24year_sales_top100"
permanent_table_name = "bigdata.ky_databricks_demo.m60_offline_member_24year_sales_top100"

# df.write.format("parquet").saveAsTable(permanent_table_name)
df.write.format("delta").saveAsTable(permanent_table_name)
