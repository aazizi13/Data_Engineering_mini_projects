#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf

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
    # initiate session
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .enableHiveSupport() \
        .getOrCreate()

    # subscribe to contents on events topic     
    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    # ######################
    # purchase events
    
    purchase_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_purchase('raw'))

    extracted_purchase_events = purchase_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_purchase_events.printSchema()
    extracted_purchase_events.show()

    extracted_purchase_events.registerTempTable("extracted_purchase_events")

#    extracted_purchase_events \
#        .write \
#        .mode('overwrite') \
#        .parquet('/tmp/purchases')
    
    spark.sql("""
        create external table purchases
        stored as parquet
        location '/tmp/purchases'
        as
        select * from extracted_purchase_events
    """)
    
    # ######################
    # declare events

    declare_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_declare('raw'))

    extracted_declare_events = declare_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_declare_events.printSchema()
    extracted_declare_events.show()

    extracted_declare_events.registerTempTable("extracted_declare_events")

#    extracted_declare_events \
#        .write \
#        .mode('overwrite') \
#        .parquet('/tmp/declarations')
    
    spark.sql("""
        create external table declarations
        stored as parquet
        location '/tmp/declarations'
        as
        select * from extracted_declare_events
    """)
    
    # ######################
    # join events

    join_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_join('raw'))

    extracted_join_events = join_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_join_events.printSchema()
    extracted_join_events.show()

    extracted_join_events.registerTempTable("extracted_join_events")
    
#    extracted_join_events \
#        .write \
#        .mode('overwrite') \
#        .parquet('/tmp/joins')
    
    spark.sql("""
        create external table joins
        stored as parquet
        location '/tmp/joins'
        as
        select * from extracted_join_events
    """)


if __name__ == "__main__":
    main()
