## Project 3.

This repository is built around a flask application. The application instruments an API server hosting a web application where a client can run a set of specific functions mimicing interactions within a mobile game. The game is set during medieval times where the client can perform various themed tasks such as purchase a sword, purchase a sword, join a guild, declare a war, etc. It is built as a minimically viable product to produce dummy data to be processed within the data-pipeline.

The repository also includes a data-pipeline that runs on a several docker containers. The main services used include kafka, spark, hadoop, & presto. kafka is used for event streaming and produces events generated from the web application. Spark subscribes to the messages, filters specific event types from kafka and lands them into HDFS as parquet files making them available for analysis using presto (via Hive). The data_pipeline.md file walks through the reproducible commands used to operationalize the data-pipeline while describing what is happening as we spin up the docker containers, generate API responses, and batch process the events.

## List of Files Within the Repository.

1. **docker-compose.yml**: This is the docker image (aka a blueprint) used to build the 6 docker containers used in theis data-pipeline.
2. **data_pipeline.md**: A markdown file with reproducible code to build and operationalize the data-pipeline. At the end, it runs a few sql queries in presto to analyze dummy data generated while interacting with the flask app. 
3. **game_api.py**: This is the code for the flask application. It defines the methods a client can request using the API server.
4. **write_hive_table_batch.py**: A job that is called from the command line. It uses spark streaming to subscribe to events from the kafka topic, land clean dataframes as parquet files in hdfs. This allows the generated data to be queried using presto. 
5. **write_events_to_hdfs_stream.py**: A job that is called from the command line. It uses spark streaming to subscribe to events from the kafka topic, land clean dataframes as parquet files in hdfs. This is built as a streaming job to continually load new events to hdfs.
6. **create_hive_table.py**: A job that is called from the command line. It uses spark streaming to create a table in hive to be queried in presto.

