#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType


def purchase_events_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- description: string (nullable = true)
    |-- strength: string (nullable = true)
    |-- timestamp: string (nullable = true) 
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("strength", StringType(), True),
    ])

# filtering for purchase_a_sword, purchase_an_axe, & purchase_a_shield events
@udf('boolean')
def is_purchase(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'].startswith('purchase'):
        return True
    return False

# filtering for declare_war & declare_peace events
@udf('boolean')
def is_declare(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'].startswith('declare'):
        return True
    return False

# filtering for join_a_guild
@udf('boolean')
def is_join(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'].startswith('join'):
        return True
    return False

def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    purchases = raw_events \
        .filter(is_purchase(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          purchase_events_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')
    

    sink = purchases \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_purchases") \
        .option("path", "/tmp/purchases") \
        .trigger(processingTime="10 seconds") \
        .start()

    sink.awaitTermination()


if __name__ == "__main__":
    main()
