---
# Data Pipeline

### Docker Containers

The docker containers are configured within the **docker-compose.yml** file. There are a total of 6 different docker containers being run in this pipeline; zookeeper, kafka, cloudera, spark, presto, and mids. zookeper is a dependency for kafka which is the initial software that streams data and produces events from the web application. Cloudera is run to provision Hadoop used for storing the generated events. We use spark to subscribe to data from kafka and land them onto HDFS. Presto is a query engine (on top of hive) used to analyze the generated events. 

To spin up the docker containers we use:

```
docker-compose up -d
```

This will return:

```
Creating network "final_default" with the default driver
Creating final_zookeeper_1 ... done
Creating final_cloudera_1  ... done
Creating final_presto_1    ... done
Creating final_mids_1      ... done
Creating final_kafka_1     ... done
Creating final_spark_1     ... done
```

To monitor the status of our containers we use:

```
docker-compose ps
```

This will return:

```
      Name                     Command               State                                    Ports                                 
------------------------------------------------------------------------------------------------------------------------------------
final_cloudera_1    /usr/bin/docker-entrypoint ...   Up      10000/tcp, 50070/tcp, 8020/tcp,                                        
                                                             0.0.0.0:8888->8888/tcp,:::8888->8888/tcp, 9083/tcp                     
final_kafka_1       /etc/confluent/docker/run        Up      29092/tcp, 9092/tcp                                                    
final_mids_1        /bin/bash                        Up      0.0.0.0:5000->5000/tcp,:::5000->5000/tcp, 8888/tcp                     
final_presto_1      /usr/bin/docker-entrypoint ...   Up      8080/tcp                                                               
final_spark_1       docker-entrypoint.sh bash        Up      8888/tcp                                                               
final_zookeeper_1   /etc/confluent/docker/run        Up      2181/tcp, 2888/tcp, 32181/tcp, 3888/tcp   
```

We can further debug by reading logs using the command:

```
docker-compose logs -f cloudera
```

(Optional) we can also set up a jupyter notebook virtual environment provisioned using spark to run python code more conveniently:

```
docker-compose exec spark env PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port 7000 --ip 0.0.0.0 --allow-root --notebook-dir=/w205/' pyspark
```

That command will return a token/URL to access the jupyter environment on a separate tab within the web browser.

### Kafka

The first step in the pipeline is to create a kafka topic that can hold the produced events. That is performed using the following command:

```
docker-compose exec kafka kafka-topics --create --topic events --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
```

The command above creates a topic called events (if it does not already exist). It only includes 1 partition with a replication factor of 1. 

Note, when trying to create the topic it is possible to get a strange issue similar to:

```
(base) jupyter@python-20211129-182356:~/w205/full-stack2$ docker-compose exec kafka kafka-topics --create --topic events --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
Exception in thread "main" joptsimple.UnrecognizedOptionException: zookeeper is not a recognized option
        at joptsimple.OptionException.unrecognizedOption(OptionException.java:108)
        at joptsimple.OptionParser.handleLongOptionToken(OptionParser.java:510)
        at joptsimple.OptionParserState$2.handleArgument(OptionParserState.java:56)
        at joptsimple.OptionParser.parse(OptionParser.java:396)
        at kafka.admin.TopicCommand$TopicCommandOptions.<init>(TopicCommand.scala:517)
        at kafka.admin.TopicCommand$.main(TopicCommand.scala:47)
        at kafka.admin.TopicCommand.main(TopicCommand.scala)
```

As a workaround it is possible to use this command to use a bootstrapped server instead:

```
docker-compose exec kafka kafka-topics --create --topic events --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server kafka:29092
```

If the topic is not already created it will return a response message:

```
Created topic events.
```

This confirms that a topic named "events" has been successfully created.


In a second terminal, run the following code to observe kafka events generated

```
docker-compose exec mids kafkacat -C -b kafka:29092 -t events -o beginning

```

Back to first terminal

### Flask App

Flask is a micro web framework written in Python. The code for our flask app is stored in the **game_api.py** file. The job generates several modules made available as a request via an API server for a client. Additionally, the file includes several functions that define logic for various client requests.

The specific functionality includes:
1) default_response = a default response
2) purchase_a_sword = mimics purchasing a sword in the theoritical world. A strength of the purchased weapon is randomly generated from a normal distribution (mean of 5, sigma of 4, minimum of 0, maximium of 10) 
3) purchase_an_axe = mimics purchasing an axe in the theoritical world. A strength of the purchased weapon is randomly generated from a normal distribution (mean of 5, sigma of 4, minimum of 0, maximium of 10)
4) purchase_a_shield = mimics purchasing a shield in the theoritical world. A strength of the purchased weapon is randomly generated from a normal distribution (mean of 5, sigma of 4, minimum of 0, maximium of 10)
5) join_a_guild = mimics joining a guild in the theoritical world. 
6) declare_peace = mimics declaring peace in the theoritical world.
7) declare_a_war = mimics declaring a war in the theoritical world.

To run the app you can run:

```
docker-compose exec mids env FLASK_APP=/w205/project-3-aazizi13/game_api.py flask run --host 0.0.0.0
```

If the flask runs successfully the terminal will show:

```
 * Serving Flask app "game_api"
 * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
```

A feed of requests will show up if a client makes them. If we want to make a single API request we can enter a command like the following (in a third terminal window):

```
docker-compose exec mids curl http://localhost:5000/purchase_a_sword
```

Each request has a different response. For the purchase_a_sword request it returns:

```
Sword Purchased!
```

In the first terminal window, the request will show up:

```
127.0.0.1 - - [02/Dec/2021 04:59:03] "GET /purchase_a_sword HTTP/1.1" 200 -
```
## Apache Bend Command USED.
For generating our event log we used apache bench to simulate thousands of client requests on the api server. The commands used include: 



```
docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/
docker-compose exec mids ab -n 500 -H "Host: knight1.comcast.com" http://localhost:5000/purchase_a_sword
docker-compose exec mids ab -n 200 -H "Host: knight1.comcast.com" http://localhost:5000/purchase_an_axe
docker-compose exec mids ab -n 500 -H "Host: knight3.yahoo.com" http://localhost:5000/purchase_a_shield
docker-compose exec mids ab -n 500 -H "Host: knight1.comcast.com" http://localhost:5000/join_a_guild
docker-compose exec mids ab -n 1 -H "Host: commanding_officer.comcast.com" http://localhost:5000/declare_a_war
docker-compose exec mids ab -n 1 -H "Host: commanding_officer.comcast.com" http://localhost:5000/declare_peace

docker-compose exec mids ab -n 10 -H "Host: user2.att.com" http://localhost:5000/
docker-compose exec mids ab -n 10 -H "Host: user2.att.com" http://localhost:5000/purchase_a_sword
docker-compose exec mids ab -n 10 -H "Host: user2.att.com" http://localhost:5000/buy_a_sword
docker-compose exec mids ab -n 10 -H "Host: user2.att.com" http://localhost:5000/join_guild
```

These commands generate the events subsequently processed and analyzed within the data-pipeline for this project. Each line represents a unique function call similuated N amount of times based on the value after -n. Within the game_api.py file, each method logs a json blob to the kafka events topic. 

Once messages have been produced to the events topic, they can be displayed using the following command (which displays the contents in its entirety):

```
docker-compose exec mids kafkacat -C -b kafka:29092 -t events -o beginning -e
```

Careful as if there are alot of events it will list each one suquentially in the order they were generated. At the bottom of the response there will be a count of total lines processed i.e:

```
{"Host": "commanding_officer.comcast.com", "User-Agent": "ApacheBench/2.3", "event_type": "declare_peace", "Accept": "*/*", "description": "lastful peace"}
% Reached end of topic events [0] at offset 1713: exiting
```

### Spark

The spark container is first used when calling the job writted in the **write_hive_table.py** file. This job is run via the following command:

The job perfoms the following general steps;

1) Generate a spark session.
2) Subscribe to events on the kafka topic.
3) Read the raw events, decorate a timestamp of when it was read, and filter for a subset of event types. Specifically, we organize the events into purchase events (i.e purchase_a_sword, purchase_an_axe, purchase_a_shield), join events (i.e join a guild), and declaration events (i.e declare_a_war, declare_peace). Each category is stored into its own dataframe that has one column per key in the json blob of the respective event messages.
4) Write the dataframes into folders within HDFS as parquet files. The folder locations include 'tmp/purchases', 'tmp/joins', and 'tmp/declarations' for purchase, join, and declaration categories respectively. 
5) Define 3 tables in hive (to query using presto); purchases, joins, declarations. 

We run the job with the following command (third terminal)

```
docker-compose exec spark spark-submit /w205/project-3-azizi13/write_hive_table_batch.py
```

If everything up to this point has been done correctly, the folders should be successfully created in HDFS with corresponding parquet files. We can check the contents of HDFS using:

```
docker-compose exec cloudera hadoop fs -ls /tmp
```

It should return something similar to:

```
Found 6 items
drwxr-xr-x   - root   supergroup          0 2021-12-02 05:05 /tmp/declarations
drwxrwxrwt   - mapred mapred              0 2016-04-06 02:26 /tmp/hadoop-yarn
drwx-wx-wx   - hive   supergroup          0 2021-12-02 05:04 /tmp/hive
drwxr-xr-x   - root   supergroup          0 2021-12-02 05:05 /tmp/joins
drwxrwxrwt   - mapred hadoop              0 2016-04-06 02:28 /tmp/logs
drwxr-xr-x   - root   supergroup          0 2021-12-02 05:05 /tmp/purchases
```

We can subsequently look at a specific parquet file within one of the tmp folders using this command:

```
docker-compose exec cloudera hadoop fs -ls /tmp/purchases
```

It will return something similar to:

```
Found 2 items
-rw-r--r--   1 root supergroup          0 2021-12-02 05:05 /tmp/purchases/_SUCCESS
-rw-r--r--   1 root supergroup       8812 2021-12-02 05:05 /tmp/purchases/part-00000-37b3e04b-03ea-4cd0-b723-c8bffb716e50-c000.snapp
```

### Presto

Hive enables in memory tabular representations of files. For this data pipeline, we stored our dummy data in parquet files. Parquet is a file format that works well in distributed computing environments because it efficiently in stores and retrieves data. Presto is the query engine on top of hive that allows analyze our stored data. We could've also used spark instead of presto, but presto scales better, handles a wider range of sql syntax, and can be configured to talk with other services such as s3. This provides a good front end for a data lake.

The following command is used to create a presto session: 

```
docker-compose exec presto presto --server presto:8080 --catalog hive --schema default
```

Within the presto terminal we can perform various commands such as:

* View tables:

```
show tables;
```

* Show the schema of a table (i.e purchases):

```
describe {table};
```

* Query contents for a table:

```
select * from {table};
```

Below are a few commands and their respective output when performed using presto:

1) View tables

```
presto:default> show tables;
    Table     
--------------
 declarations 
 joins        
 purchases    
(3 rows)

Query 20211202_053517_00002_7v59p, FINISHED, 1 node
Splits: 2 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

2) View purchases table schema

```
presto:default> describe purchases;
   Column    |  Type   | Comment 
-------------+---------+---------
 accept      | varchar |         
 host        | varchar |         
 user-agent  | varchar |         
 description | varchar |         
 event_type  | varchar |         
 strength    | varchar |         
 timestamp   | varchar |         
(7 rows)

Query 20211202_053614_00003_7v59p, FINISHED, 1 node
Splits: 2 total, 1 done (50.00%)
0:01 [0 rows, 0B] [0 rows/s, 0B/s]
```

3) Contents of the purchases dataset

```
presto:default> select * from purchases limit 10;
 accept |        host         |   user-agent    | description |   event_type   | strength |        timestamp        
--------+---------------------+-----------------+-------------+----------------+----------+-------------------------
 */*    | localhost:5000      | curl/7.47.0     | large sword | purchase_sword |        0 | 2021-12-02 05:45:22.148 
 */*    | knight1.comcast.com | ApacheBench/2.3 | large sword | purchase_sword |        7 | 2021-12-02 05:45:34.787 
 */*    | knight1.comcast.com | ApacheBench/2.3 | large sword | purchase_sword |        0 | 2021-12-02 05:45:34.794 
 */*    | knight1.comcast.com | ApacheBench/2.3 | large sword | purchase_sword |        5 | 2021-12-02 05:45:34.8   
 */*    | knight1.comcast.com | ApacheBench/2.3 | large sword | purchase_sword |        2 | 2021-12-02 05:45:34.811 
 */*    | knight1.comcast.com | ApacheBench/2.3 | large sword | purchase_sword |        3 | 2021-12-02 05:45:34.817 
 */*    | knight1.comcast.com | ApacheBench/2.3 | large sword | purchase_sword |        3 | 2021-12-02 05:45:34.825 
 */*    | knight1.comcast.com | ApacheBench/2.3 | large sword | purchase_sword |        2 | 2021-12-02 05:45:34.832 
 */*    | knight1.comcast.com | ApacheBench/2.3 | large sword | purchase_sword |        4 | 2021-12-02 05:45:34.838 
 */*    | knight1.comcast.com | ApacheBench/2.3 | large sword | purchase_sword |        0 | 2021-12-02 05:45:34.851 
(10 rows)

Query 20211202_054746_00003_dwfux, FINISHED, 1 node
Splits: 2 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

4) Unique event types in the purchases dataset

```
presto:default> select distinct event_type from purchases;
   event_type    
-----------------
 purchase_sword  
 purchase_axe    
 purchase_shield 
(3 rows)

Query 20211202_054820_00004_dwfux, FINISHED, 1 node
Splits: 3 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

5) Average strength of purchase item. As mentioned above, the strength value is randomly sampled from a normal distribution preconfigured. 

```
presto:default> select avg(cast(strength as integer)) from purchases;
       _col0        
--------------------
 4.5154038301415484 
(1 row)

Query 20211202_054913_00005_dwfux, FINISHED, 1 node
Splits: 2 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

---

# Further Engineering

1. Build a functional streaming data-pipeline. We attempted this using the following two scripts

* **write_events_to_hdfs_stream.py**: Meant to be triggered on the command line. This will be a continual streaming job that checks for new data and batch processes every 10sec indefinitely. 

* **create_hive_table.py**: Meant to be triggered on the command line. It is meant as a one time job that is run once some data is landed on HDFS. It is meant to be a one time job that creates the tables in hive based on the parquet file structure.

We weren't able to get it to work successfully. The _stream.py file seems to properly land parquet files into HDFS. I ran the following commands

```
docker-compose exec spark spark-submit /w205/project-3-aazizi13/write_events_to_hdfs_stream.py
```

I then checked to see the contents on HDFS and do see the new folders as expected. I even can see a parquet file within the purchases folder:

```
(base) jupyter@python-20211129-205657:~/w205/project-3-aazizi13$ docker-compose exec cloudera hadoop fs -ls /tmp
Found 5 items
drwxrwxrwt   - root   supergroup          0 2021-12-03 05:09 /tmp/checkpoints_for_purchases
drwxrwxrwt   - mapred mapred              0 2016-04-06 02:26 /tmp/hadoop-yarn
drwx-wx-wx   - hive   supergroup          0 2021-12-03 05:09 /tmp/hive
drwxrwxrwt   - mapred hadoop              0 2016-04-06 02:28 /tmp/logs
drwxr-xr-x   - root   supergroup          0 2021-12-03 05:09 /tmp/purchases
(base) jupyter@python-20211129-205657:~/w205/project-3-aazizi13$ docker-compose exec cloudera hadoop fs -ls /tmp/purchases
Found 2 items
drwxr-xr-x   - root supergroup          0 2021-12-03 05:09 /tmp/purchases/_spark_metadata
-rw-r--r--   1 root supergroup        860 2021-12-03 05:09 /tmp/purchases/part-00000-dc96ac98-5fd5-4344-9e3b-b12898af2bbe-c000.snappy.parquet
(base) jupyter@python-20211129-205657:~/w205/project-3-aazizi13$ docker-compose exec cloudera hadoop fs -ls /tmp/purchases
Found 2 items
drwxr-xr-x   - root supergroup          0 2021-12-03 05:09 /tmp/purchases/_spark_metadata
-rw-r--r--   1 root supergroup        860 2021-12-03 05:09 /tmp/purchases/part-00000-dc96ac98-5fd5-4344-9e3b-b12898af2bbe-c000.snappy.parquet
```

I then run the **create_hive_table.py** file, which appears to run successfully:

```
docker-compose exec spark spark-submit /w205/project-3-aazizi13/create_hive_table.py
```

But, I then notice the parquet file in HDFS is missing & no tables exist in presto:

```
(base) jupyter@python-20211129-205657:~/w205/project-3-aazizi13$ docker-compose exec cloudera hadoop fs -ls /tmp/purchases
(base) jupyter@python-20211129-205657:~/w205/project-aazizi13$ 

```

Thus, I think somethings is working incorrectly in the **create_hive_table.py** file.

2. Add functionality to the game_api.py flask app. Currently this acts as a minimally viable product that runs as an API server producing responses to a client request and logging events to a kafka topic. But, we can theoritically add many additional request functions that make for a more entertaining game. 
