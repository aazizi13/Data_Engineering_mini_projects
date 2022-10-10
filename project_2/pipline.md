# Project 2: Pipline Commands

This document shows the commands that were run across the tools explained in `project-2-report.md`

## Docker-compose

```
# change directories
cd ~/w205/project-2-aazizi13/

#Putting the data in the directory
curl -L -o assessment-attempts-20180128-121051-nested.json https://goo.gl/ME6hjp

#putting the docker-compose.yml file in directory
cp ~/w205/course-content//08-Querying-Data/docker-compose.yml .

# shutting down any docker compose running
docker-compose down 

# running my docker compose 
docker-compose up -d

#checking if the containors are running 
dock-compose ps

# check that hadoop is running correctly
docker-compose exec cloudera hadoop fs -ls /tmp/

```

## Kafka

Kafka Commands to capture data. 

```
# create a topic called "assessment"
docker-compose exec kafka kafka-topics --create --topic assessment --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181

# check topic
docker-compose exec kafka kafka-topics --describe --topic assessment --zookeeper zookeeper:32181

# expected output: 
Topic:assessment   PartitionCount:1    ReplicationFactor:1 Configs:
Topic: assessment  Partition: 0    Leader: 1    Replicas: 1  Isr: 1

# publish test messages
docker-compose exec mids bash -c "cat /w205/project-2-aazizi13/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c | kafkacat -P -b kafka:29092 -t assessments && echo 'Produced 100 messages.'"

# use kafkacat to produce test messages to the assessment topic
docker-compose exec mids bash -c "cat /w205/project-2-aazizi13/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c | kafkacat -P -b kafka:29092 -t assessment"

# check out messages 
# Data is hard to read
docker-compose exec mids bash -c "cat /w205/project-2-aazizi13/assessment-attempts-20180128-121051-nested.json"

# check out individual messages
docker-compose exec mids bash -c "cat /w205/project-2-aazizi13/assessment-attempts-20180128-121051-nested.json | jq '.[]' -c"

# Make it human readable
docker-compose exec mids bash -c "cat /w205/project-2-aazizi13/assessment-attempts-20180128-121051-nested.json | jq '.'"

# consume messages & print word count 
docker-compose exec mids bash -c "kafkacat -C -b kafka:29092 -t assessment -o beginning -e" | wc -l

```

## Pyspark using container

Commands in spark to read the data from kafka

```
# Run Pyspark using spark container 
docker-compose exec spark pyspark

# read from kafka in pyspark console
raw_assessments = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe","assessment").option("startingOffsets", "earliest").option("endingOffsets", "latest").load()

# see schema
raw_assessments.printSchema()

# see messages
raw_assessments.show()

# cache the warnings
raw_assessments.cache()

# cast as strings
assessments = raw_assessments.select(raw_assessments.value.cast('string'))

# taking a look 
assessments.show()
assessments.printSchema()
assessments.count()

# unrolling json
# pulling out first entry 
assessments.select('value').take(1)

# Extract its value
assessments.select('value').take(1)[0].value

# using json to unroll 
import json

# pulling out first message and assgning it to first_message
first_assessment = json.loads(assessments.select('value').take(1)[0].value)

# taking a look
first_assessment

# printing an item from first message. Let's look at keen_id
print(first_assessment['keen_id'])

# writing assessments in its current form to hdfs
assessments.write.parquet("/tmp/assessments")

# checking out results from another terminal(make sure you are in the same directory)
docker-compose exec cloudera hadoop fs -ls /tmp/
# expected output: 
Found 3 items
drwxr-xr-x   - root   supergroup          0 2021-10-24 21:55 /tmp/assessments
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2021-10-24 20:43 /tmp/hive

docker-compose exec cloudera hadoop fs -ls /tmp/assessments/
# expected output:
Found 2 items
-rw-r--r--   1 root supergroup          0 2021-10-24 21:55 /tmp/assessments/_SUCCESS
-rw-r--r--   1 root supergroup    2513397 2021-10-24 21:55 /tmp/assessments/part-00000-4371eab7-fd19-4190-a7bf-58610f22bae0-c000.snappy.parquet

# Back to spark console 
assessments.show()
# expected output -- This is not useful at all. 
+--------------------+
|               value|
+--------------------+
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
|{"keen_timestamp"...|
+--------------------+
only showing top 20 rows


# Using sys to deal with with unicode
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

# Using rdd and taking at what we have 
import json
assessments.rdd.map(lambda x: json.loads(x.value)).toDF().show()

# After above assessments, saving the manipulate dataframe as extracted assessments 
extracted_assessments = assessments.rdd.map(lambda x: json.loads(x.value)).toDF()

# take a look at unrolled version 
extracted_assessments.show()
# expected output -- We can see that the "sequence" column has nested values because it has "Map" in its values
+--------------------+-------------+--------------------+------------------+--------------------+------------------+------------+--------------------+--------------------+--------------------+
|        base_exam_id|certification|           exam_name|   keen_created_at|             keen_id|    keen_timestamp|max_attempts|           sequences|          started_at|        user_exam_id|
+--------------------+-------------+--------------------+------------------+--------------------+------------------+------------+--------------------+--------------------+--------------------+
|37f0a30a-7464-11e...|        false|Normal Forms and ...| 1516717442.735266|5a6745820eb8ab000...| 1516717442.735266|         1.0|Map(questions -> ...|2018-01-23T14:23:...|6d4089e4-bde5-4a2...|
|37f0a30a-7464-11e...|        false|Normal Forms and ...| 1516717377.639827|5a674541ab6b0a000...| 1516717377.639827|         1.0|Map(questions -> ...|2018-01-23T14:21:...|2fec1534-b41f-441...|
|4beeac16-bb83-4d5...|        false|The Principles of...| 1516738973.653394|5a67999d3ed3e3000...| 1516738973.653394|         1.0|Map(questions -> ...|2018-01-23T20:22:...|8edbc8a8-4d26-429...|
|4beeac16-bb83-4d5...|        false|The Principles of...|1516738921.1137421|5a6799694fc7c7000...|1516738921.1137421|         1.0|Map(questions -> ...|2018-01-23T20:21:...|c0ee680e-8892-4e6...|
|6442707e-7488-11e...|        false|Introduction to B...| 1516737000.212122|5a6791e824fccd000...| 1516737000.212122|         1.0|Map(questions -> ...|2018-01-23T19:48:...|e4525b79-7904-405...|
|8b4488de-43a5-4ff...|        false|        Learning Git| 1516740790.309757|5a67a0b6852c2a000...| 1516740790.309757|         1.0|Map(questions -> ...|2018-01-23T20:51:...|3186dafa-7acf-47e...|
|e1f07fac-5566-4fd...|        false|Git Fundamentals ...|1516746279.3801291|5a67b627cc80e6000...|1516746279.3801291|         1.0|Map(questions -> ...|2018-01-23T22:24:...|48d88326-36a3-4cb...|
|7e2e0b53-a7ba-458...|        false|Introduction to P...| 1516743820.305464|5a67ac8cb0a5f4000...| 1516743820.305464|         1.0|Map(questions -> ...|2018-01-23T21:43:...|bb152d6b-cada-41e...|
|1a233da8-e6e5-48a...|        false|Intermediate Pyth...|  1516743098.56811|5a67a9ba060087000...|  1516743098.56811|         1.0|Map(questions -> ...|2018-01-23T21:31:...|70073d6f-ced5-4d0...|
|7e2e0b53-a7ba-458...|        false|Introduction to P...| 1516743764.813107|5a67ac54411aed000...| 1516743764.813107|         1.0|Map(questions -> ...|2018-01-23T21:42:...|9eb6d4d6-fd1f-4f3...|
|4cdf9b5f-fdb7-4a4...|        false|A Practical Intro...|1516744091.3127241|5a67ad9b2ff312000...|1516744091.3127241|         1.0|Map(questions -> ...|2018-01-23T21:45:...|093f1337-7090-457...|
|e1f07fac-5566-4fd...|        false|Git Fundamentals ...|1516746256.5878439|5a67b610baff90000...|1516746256.5878439|         1.0|Map(questions -> ...|2018-01-23T22:24:...|0f576abb-958a-4c0...|
|87b4b3f9-3a86-435...|        false|Introduction to M...|  1516743832.99235|5a67ac9837b82b000...|  1516743832.99235|         1.0|Map(questions -> ...|2018-01-23T21:40:...|0c18f48c-0018-450...|
|a7a65ec6-77dc-480...|        false|   Python Epiphanies|1516743332.7596769|5a67aaa4f21cc2000...|1516743332.7596769|         1.0|Map(questions -> ...|2018-01-23T21:34:...|b38ac9d8-eef9-495...|
|7e2e0b53-a7ba-458...|        false|Introduction to P...| 1516743750.097306|5a67ac46f7bce8000...| 1516743750.097306|         1.0|Map(questions -> ...|2018-01-23T21:41:...|bbc9865f-88ef-42e...|
|e5602ceb-6f0d-11e...|        false|Python Data Struc...|1516744410.4791961|5a67aedaf34e85000...|1516744410.4791961|         1.0|Map(questions -> ...|2018-01-23T21:51:...|8a0266df-02d7-44e...|
|e5602ceb-6f0d-11e...|        false|Python Data Struc...|1516744446.3999851|5a67aefef5e149000...|1516744446.3999851|         1.0|Map(questions -> ...|2018-01-23T21:53:...|95d4edb1-533f-445...|
|f432e2e3-7e3a-4a7...|        false|Working with Algo...| 1516744255.840405|5a67ae3f0c5f48000...| 1516744255.840405|         1.0|Map(questions -> ...|2018-01-23T21:50:...|f9bc1eff-7e54-42a...|
|76a682de-6f0c-11e...|        false|Learning iPython ...| 1516744023.652257|5a67ad579d5057000...| 1516744023.652257|         1.0|Map(questions -> ...|2018-01-23T21:46:...|dc4b35a7-399a-4bd...|
|a7a65ec6-77dc-480...|        false|   Python Epiphanies|1516743398.6451161|5a67aae6753fd6000...|1516743398.6451161|         1.0|Map(questions -> ...|2018-01-23T21:35:...|d0f8249a-597e-4e1...|
+--------------------+-------------+--------------------+------------------+--------------------+------------------+------------+--------------------+--------------------+--------------------+
only showing top 20 rows

# print the schema
extracted_assessments.printSchema()
# expected output -- Confirming that sequence has nested values
root
 |-- base_exam_id: string (nullable = true)
 |-- certification: string (nullable = true)
 |-- exam_name: string (nullable = true)
 |-- keen_created_at: string (nullable = true)
 |-- keen_id: string (nullable = true)
 |-- keen_timestamp: string (nullable = true)
 |-- max_attempts: string (nullable = true)
 |-- sequences: map (nullable = true)
 |    |-- key: string
 |    |-- value: array (valueContainsNull = true)
 |    |    |-- element: map (containsNull = true)
 |    |    |    |-- key: string
 |    |    |    |-- value: boolean (valueContainsNull = true)
 |-- started_at: string (nullable = true)
 |-- user_exam_id: string (nullable = true)


# saving this as a parquet file
extracted_assessments.write.parquet("/tmp/extracted_assessments")

# checking out the newly saves extracted_assements in another terminal
docker-compose exec cloudera hadoop fs -ls /tmp/extracted_assessments/

# expected output:
Found 2 items
-rw-r--r--   1 root supergroup          0 2021-10-24 22:20 /tmp/extracted_assessments/_SUCCESS
-rw-r--r--   1 root supergroup     345388 2021-10-24 22:20 /tmp/extracted_assessments/part-00000-7b951fe8-6589-44cb-93bb-166f0ed90bb9-c000.snappy.parquet

# Registering the extracted_assessments table as a temporary table names assessment 
extracted_assessments.registerTempTable('assessments')

# Finding out how many assessmetns are in the dataset?
spark.sql("select count(keen_id) from assessments").show()

# output
+--------------+
|count(keen_id)|
+--------------+
|          3280|
+--------------+

# figuring out the 5 most and 5 least common courses taken
# 5 most common
spark.sql("select exam_name, count(exam_name)  from assessments group by exam_name order by count(exam_name) desc").show(5)

#ouput

+--------------------+----------------+                                         
|           exam_name|count(exam_name)|
+--------------------+----------------+
|        Learning Git|             394|
|Introduction to P...|             162|
|Introduction to J...|             158|
|Intermediate Pyth...|             158|
|Learning to Progr...|             128|
+--------------------+----------------+

# 5 least common
spark.sql("select exam_name, count(exam_name)  from assessments group by exam_name order by count(exam_name)").show(5)

#output

+--------------------+----------------+                                         
|           exam_name|count(exam_name)|
+--------------------+----------------+
|Native Web Apps f...|               1|
|Nulls, Three-valu...|               1|
|Learning to Visua...|               1|
|Operating Red Hat...|               1|
|Learning Spring P...|               2|
+--------------------+----------------+

#Determining which 5 exams had the highest scores on average? Lowest scores?
#Since "sequence" is nested, we have to write a function so that we can pull our desired data. 
#Our function will go to each row and look at the sequence value, if it exists, then it goes to check if counts is in the sequence. 
#Then our funtion checks if total and correct are in the count.
#if so, then they are put in inside my_dict
#it iterates and appends the results to my_list each time. 
#the function will then return my_list 

def extract_correct_total(x):
    # loading json
    raw_dict = json.loads(x.value)
    # Creating empty list
    my_list = []
    
    # if sequences in the in dictionary
    if "sequences" in raw_dict:
        # if counts are in the sequences
        if "counts" in raw_dict["sequences"]:
            # if correct & total values are in counts
            if "correct" in raw_dict["sequences"]["counts"] and "total" in raw_dict["sequences"]["counts"]:
                # Pull the exam name, count of the correctly answered questions, and the total into a new dictionary called my_dict   
                my_dict = {"exam_name": raw_dict["exam_name"],
                           "correct": raw_dict["sequences"]["counts"]["correct"], 
                           "total": raw_dict["sequences"]["counts"]["total"]}
                # append rows to my_list
                my_list.append(Row(**my_dict))
    # return my_list
    return my_list

# using RDD to create a table 
correct_total = assessments.rdd.flatMap(extract_correct_total).toDF()

#creating a temporary data table and naming it ect
correct_total.registerTempTable("ect")

# pull the 5 highest average scored exams
spark.sql("select exam_name, avg(correct / total) as avg_score from ect group by exam_name order by avg_score desc").show(5)
# output

+--------------------+------------------+
|           exam_name|         avg_score|
+--------------------+------------------+
|Learning to Visua...|               1.0|
|The Closed World ...|               1.0|
|Nulls, Three-valu...|               1.0|
|Learning SQL for ...|0.9772727272727273|
|Introduction to J...|0.8759493670886073|
+--------------------+------------------+


# pull the 5 lowest average scored exams
spark.sql("select exam_name, avg(correct / total) as avg_score from ect group by exam_name order by avg_score").show(5)
# output

+--------------------+------------------+
|           exam_name|         avg_score|
+--------------------+------------------+
|Client-Side Data ...|               0.2|
|Native Web Apps f...|              0.25|
|       View Updating|              0.25|
|Arduino Prototypi...|0.3333333333333333|
|Mastering Advance...|0.3602941176470588|
+--------------------+------------------+

#exiting spark console 
exit()

```
