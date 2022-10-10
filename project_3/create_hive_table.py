#!/usr/bin/env python
"""Extract events from hdfs and use the corresponding schemas to define tables in hive
"""
import json
from pyspark.sql import SparkSession, Row


spark = SparkSession \
    .builder \
    .appName("ExtractEventsJob") \
    .enableHiveSupport() \
    .getOrCreate()


# purchase table
df = spark.read.parquet('/tmp/purchases')
df.registerTempTable('purchases')
query = """
create external table purchases
  stored as parquet
  location '/tmp/purchases'
  as
  select * from purchases
"""
spark.sql(query)