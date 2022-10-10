# Project 1: Query Project

- In the Query Project, you will get practice with SQL while learning about
  Google Cloud Platform (GCP) and BiqQuery. You'll answer business-driven
  questions using public datasets housed in GCP. To give you experience with
  different ways to use those datasets, you will use the web UI (BiqQuery) and
  the command-line tools, and work with them in Jupyter Notebooks.

#### Problem Statement

- You're a data scientist at Lyft Bay Wheels (https://www.lyft.com/bikes/bay-wheels), formerly known as Ford GoBike, the
  company running Bay Area Bikeshare. You are trying to increase ridership, and
  you want to offer deals through the mobile app to do so. 
  
- What deals do you offer though? Currently, your company has several options which can change over time.  Please visit the website to see the current offers and other marketing information. Frequent offers include: 
  * Single Ride 
  * Monthly Membership
  * Annual Membership
  * Bike Share for All
  * Access Pass
  * Corporate Membership
  * etc.

- Through this project, you will answer these questions: 

  * What are the 5 most popular trips that you would call "commuter trips"? 
  
  * What are your recommendations for offers (justify based on your findings)?

- Please note that there are no exact answers to the above questions, just like in the proverbial real world.  This is not a simple exercise where each question above will have a simple SQL query. It is an exercise in analytics over inexact and dirty data. 

- You won't find a column in a table labeled "commuter trip".  You will find you need to do quite a bit of data exploration using SQL queries to determine your own definition of a communter trip.  In data exploration process, you will find a lot of dirty data, that you will need to either clean or filter out. You will then write SQL queries to find the communter trips.

- Likewise to make your recommendations, you will need to do data exploration, cleaning or filtering dirty data, etc. to come up with the final queries that will give you the supporting data for your recommendations. You can make any recommendations regarding the offers, including, but not limited to: 
  * market offers differently to generate more revenue 
  * remove offers that are not working 
  * modify exising offers to generate more revenue
  * create new offers for hidden business opportunities you have found
  * etc. 

#### All Work MUST be done in the Google Cloud Platform (GCP) / The Majority of Work MUST be done using BigQuery SQL / Usage of Temporary Tables, Views, Pandas, Data Visualizations

A couple of the goals of w205 are for students to learn how to work in a cloud environment (such as GCP) and how to use SQL against a big data data platform (such as Google BigQuery).  In keeping with these goals, please do all of your work in GCP, and the majority of your analytics work using BigQuery SQL queries.

You can make intermediate temporary tables or views in your own dataset in BigQuery as you like.  Actually, this is a great way to work!  These make data exploration much easier.  It's much easier when you have made temporary tables or views with only clean data, filtered rows, filtered columns, new columns, summary data, etc.  If you use intermediate temporary tables or views, you should include the SQL used to create these, along with a brief note mentioning that you used the temporary table or view.

In the final Jupyter Notebook, the results of your BigQuery SQL will be read into Pandas, where you will use the skills you learned in the Python class to print formatted Pandas tables, simple data visualizations using Seaborn / Matplotlib, etc.  You can use Pandas for simple transformations, but please remember the bulk of work should be done using Google BigQuery SQL.

#### GitHub Procedures

In your Python class you used GitHub, with a single repo for all assignments, where you committed without doing a pull request.  In this class, we will try to mimic the real world more closely, so our procedures will be enhanced. 

Each project, including this one, will have it's own repo.

Important:  In w205, please never merge your assignment branch to the master branch. 

Using the git command line: clone down the repo, leave the master branch untouched, create an assignment branch, and move to that branch:
- Open a linux command line to your virtual machine and be sure you are logged in as jupyter.
- Create a ~/w205 directory if it does not already exist `mkdir ~/w205`
- Change directory into the ~/w205 directory `cd ~/w205`
- Clone down your repo `git clone <https url for your repo>`
- Change directory into the repo `cd <repo name>`
- Create an assignment branch `git branch assignment`
- Checkout the assignment branch `git checkout assignment`

The previous steps only need to be done once.  Once you your clone is on the assignment branch it will remain on that branch unless you checkout another branch.

The project workflow follows this pattern, which may be repeated as many times as needed.  In fact it's best to do this frequently as it saves your work into GitHub in case your virtual machine becomes corrupt:
- Make changes to existing files as needed.
- Add new files as needed
- Stage modified files `git add <filename>`
- Commit staged files `git commit -m "<meaningful comment about your changes>"`
- Push the commit on your assignment branch from your clone to GitHub `git push origin assignment`

Once you are done, go to the GitHub web interface and create a pull request comparing the assignment branch to the master branch.  Add your instructor, and only your instructor, as the reviewer.  The date and time stamp of the pull request is considered the submission time for late penalties. 

If you decide to make more changes after you have created a pull request, you can simply close the pull request (without merge!), make more changes, stage, commit, push, and create a final pull request when you are done.  Note that the last data and time stamp of the last pull request will be considered the submission time for late penalties.

Make sure you receive the emails related to your repository! Your project feedback will be given as comment on the pull request. When you receive the feedback, you can address problems or simply comment that you have read the feedback. 
AFTER receiving and answering the feedback, merge you PR to master. Your project only counts as complete once this is done.

---

## Parts 1, 2, 3

We have broken down this project into 3 parts, about 1 week's work each to help you stay on track.

**You will only turn in the project once at the end of part 3!**

- In Part 1, we will query using the Google BigQuery GUI interface in the cloud.

- In Part 2, we will query using the Linux command line from our virtual machine in the cloud.

- In Part 3, we will query from a Jupyter Notebook in our virtual machine in the cloud, save the results into Pandas, and present a report enhanced by Pandas output tables and simple data visualizations using Seaborn / Matplotlib.

---

## Part 1 - Querying Data with BigQuery

### SQL Tutorial

Please go through this SQL tutorial to help you learn the basics of SQL to help you complete this project.

SQL tutorial: https://www.w3schools.com/sql/default.asp

### Google Cloud Helpful Links

Read: https://cloud.google.com/docs/overview/

BigQuery: https://cloud.google.com/bigquery/

Public Datasets: Bring up your Google BigQuery console, open the menu for the public datasets, and navigate to the the dataset san_francisco.

- The Bay Bike Share has two datasets: a static one and a dynamic one.  The static one covers an historic period of about 3 years.  The dynamic one updates every 10 minutes or so.  THE STATIC ONE IS THE ONE WE WILL USE IN CLASS AND IN THE PROJECT. The reason is that is much easier to learn SQL against a static target instead of a moving target.

- (USE THESE TABLES!) The static tables we will be using in this class are in the dataset **san_francisco** :

  * bikeshare_stations

  * bikeshare_status

  * bikeshare_trips

- The dynamic tables are found in the dataset **san_francisco_bikeshare**

### Some initial queries

Paste your SQL query and answer the question in a sentence.  Be sure you properly format your queries and results using markdown. 

- What's the size of this dataset? (i.e., how many trips)
SQL Query and Answer

SELECT count(distinct trip_id)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips

--I used trip-id to find how many trips are there.
--result: 983648


- What is the earliest start date and time and latest end date and time for a trip?
-SQL Query and Answer: 

SELECT min(start_date)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
LIMIT 100;

--Earliest start date and time: 2013-08-29 09:08:00 UTC

SELECT max(end_date)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
LIMIT 100;

--Latest end date and time: 2016-08-31 23:48:00 UTC


- How many bikes are there?
SQL Query and Answer:

SELECT count(distinct bike_number)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips

--There are 700 bikes. 

### Questions of your own
- Make up 3 questions and answer them using the Bay Area Bike Share Trips Data.  These questions MUST be different than any of the questions and queries you ran above.

- Question 1: What are the different subscriber types? And how many of each type are there?
SQL Query and Answer:

  * Answer: Two types, customers and subscribers. Number of subscriber types that are "Customer":136809. Number of subscriber types that are "Subscriber": 846839
  * SQL query:
SELECT distinct subscriber_type
FROM `bigquery-public-data`.san_francisco.bikeshare_trips

SELECT count(subscriber_type)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
WHERE subscriber_type = 'Customer'

SELECT count(subscriber_type)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
WHERE subscriber_type = 'Subscriber'

- Question 2: What are the number of trips from one station to the next for all stations? Sort by descending order. 

  * Answer:
+------------------------------------------+------------------------------------------+-------------+
|            start_station_name            |             end_station_name             | no_of_trips |
+------------------------------------------+------------------------------------------+-------------+
| Harry Bridges Plaza (Ferry Building)     | Embarcadero at Sansome                   |        9150 |
| San Francisco Caltrain 2 (330 Townsend)  | Townsend at 7th                          |        8508 |
| 2nd at Townsend                          | Harry Bridges Plaza (Ferry Building)     |        7620 |
| Harry Bridges Plaza (Ferry Building)     | 2nd at Townsend                          |        6888 |
| Embarcadero at Sansome                   | Steuart at Market                        |        6874 |
| Townsend at 7th                          | San Francisco Caltrain 2 (330 Townsend)  |        6836 |
| Embarcadero at Folsom                    | San Francisco Caltrain (Townsend at 4th) |        6351 |
| San Francisco Caltrain (Townsend at 4th) | Harry Bridges Plaza (Ferry Building)     |        6215 |
| Steuart at Market                        | 2nd at Townsend                          |        6039 |
| Steuart at Market                        | San Francisco Caltrain (Townsend at 4th) |        5959 |
+------------------------------------------+------------------------------------------+-------------+

  
  * SQL query:
  
SELECT start_station_name, end_station_name, count(*) as no_of_trips 
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
GROUP BY start_station_name, end_station_name 
ORDER BY no_of_trips DESC 
LIMIT 10;


- Question 3: What is the average trip time? What is the max trip time? What is the min trip time? Are there any outliers? 
  * Answer: Answer for each query below it. 
  
  * SQL query:
SELECT min(duration_sec)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
-- 60 seconds (1 min)

SELECT avg(duration_sec)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
-- 1018.9 seconds (~17 mins)

SELECT max(duration_sec)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
-- 17,270,400 seconds (~287,840 mins or 4797.33 hours or 199.88 days)

SELECT count(duration_sec)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
WHERE duration_sec > 86400
-- 296 trips 
 
SELECT count(duration_sec)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
WHERE duration_sec > 1000000
-- 4 that are over 1,000,000 seconds (16666.7 mins or 277.8 hours)


### Bonus activity queries (optional - not graded - just this section is optional, all other sections are required)

The bike share dynamic dataset offers multiple tables that can be joined to learn more interesting facts about the bike share business across all regions. These advanced queries are designed to challenge you to explore the other tables, using only the available metadata to create views that give you a broader understanding of the overall volumes across the regions(each region has multiple stations)

We can create a temporary table or view against the dynamic dataset to join to our static dataset.

Here is some SQL to pull the region_id and station_id from the dynamic dataset.  You can save the results of this query to a temporary table or view.  You can then join the static tables to this table or view to find the region:
```sql
#standardSQL
select distinct region_id, station_id
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info`
```

- Top 5 popular station pairs in each region

- Top 3 most popular regions(stations belong within 1 region)

- Total trips for each short station name in each region

- What are the top 10 used bikes in each of the top 3 region. these bikes could be in need of more frequent maintenance.

---

## Part 2 - Querying data from the BigQuery CLI 

- Use BQ from the Linux command line:

  * General query structure

    ```
    bq query --use_legacy_sql=false '
        SELECT count(*)
        FROM
           `bigquery-public-data.san_francisco.bikeshare_trips`'
    ```

### Queries

1. Rerun the first 3 queries from Part 1 using bq command line tool (Paste your bq
   queries and results here, using properly formatted markdown):

  * What's the size of this dataset? (i.e., how many trips)
bq query --use_legacy_sql=false 'SELECT count(distinct trip_id) FROM `bigquery-public-data`.san_francisco.bikeshare_trips'
+--------+
|  f0_   |
+--------+
| 983648 |
+--------+

  * What is the earliest start time and latest end time for a trip?
bq query --use_legacy_sql=false 'SELECT min(start_date) FROM `bigquery-public-data`.san_francisco.bikeshare_trips LIMIT 100;'
+---------------------+
|         f0_         |
+---------------------+
| 2013-08-29 09:08:00 |
+---------------------+

bq query --use_legacy_sql=false 'SELECT max(end_date) FROM `bigquery-public-data`.san_francisco.bikeshare_trips LIMIT 100;'
+---------------------+
|         f0_         |
+---------------------+
| 2016-08-31 23:48:00 |
+---------------------+

  * How many bikes are there?
  
bq query --use_legacy_sql=false 'SELECT count(distinct bike_number)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips'
+-----+
| f0_ |
+-----+
| 700 |
+-----+

2. New Query (Run using bq and paste your SQL query and answer the question in a sentence, using properly formatted markdown):

  * How many trips are in the morning vs in the afternoon?
  
bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM `bigquery-public-data.san_francisco.bikeshare_trips` WHERE EXTRACT(HOUR FROM start_date) >= 6 and EXTRACT(HOUR FROM start_date) < 12'

--399821 trips between 6:00 am to 12:00 pm (I consider the hours between 6:00 am to 12:00 pm to be morning)


bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM `bigquery-public-data.san_francisco.bikeshare_trips` WHERE EXTRACT(HOUR FROM start_date) >= 12 and EXTRACT(HOUR FROM start_date) < 18'

--391199 trips between 12:00 pm to 6:00 pm (I consider the hours between 6:00 am to 12:00 pm to be afternoon) 

### Project Questions
Identify the main questions you'll need to answer to make recommendations (list
below, add as many questions as you need).

- Question 1: How many trips are taken during rush hour by Customers? (Rush hour: Monday-Friday, 6-9am or 4-6pm)

- Question 2: How many trips are taken during rush hour by Subscribers? (Rush hour: Monday-Friday, 6-9am or 4-6pm)

- Question 3: How many trips are there during rush hours? (Rush hour: Monday-Friday, 6-9am or 4-6pm)

- Question 4: How many trips are there during weekdays? How trips are there during weekends?

- Question 5: On Average, how long are customers and subscribers trips times? 

- Question 6: How many stations are there in each city? Sort by descending order.  

- Question 7: What are the total number of trips from one station to the next for all stations? How about during weekdays? How about during weekends? 

- Question 8: What are the different subscriber types? And how many of each type are there?

### Answers

Answer at least 4 of the questions you identified above You can use either
BigQuery or the bq command line tool.  Paste your questions, queries and
answers below.

- Question 1: How many trips are taken during rush hour by Customers? (Rush hour: Monday-Friday, 6-9am or 4-6pm)

  * Answer: 31526 trips are taken by customers between the rush hours
  * SQL query:
  
SELECT COUNT(*), 
FROM `bigquery-public-data.san_francisco.bikeshare_trips` 
WHERE subscriber_type = "Customer" and 
EXTRACT(DAYOFWEEK FROM start_date) NOT IN (1, 7) and (
(EXTRACT(HOUR FROM start_date) >= 6 and EXTRACT(HOUR FROM start_date) <= 9) 
OR (EXTRACT(HOUR FROM start_date) >= 16 and EXTRACT(HOUR FROM start_date) <= 18))


- Question 2:How many trips are taken during rush hour by Subscribers? (Rush hour: Monday-Friday, 6-9am or 4-6pm)

  * Answer:548987 trips are taken by subscribers between rush hours. 
  * SQL query:
  
SELECT COUNT(*), 
FROM `bigquery-public-data.san_francisco.bikeshare_trips` 
WHERE subscriber_type = "Subscriber" and 
EXTRACT(DAYOFWEEK FROM start_date) NOT IN (1, 7) and (
(EXTRACT(HOUR FROM start_date) >= 6 and EXTRACT(HOUR FROM start_date) <= 9) 
OR (EXTRACT(HOUR FROM start_date) >= 16 and EXTRACT(HOUR FROM start_date) <= 18))

- Question 3: How many trips are there during rush hours? (Rush hour: Monday-Friday, 6-9am or 4-6pm)

  * Answer:total number of trips from both subscribers and customers take during rush hours is 580513
  * SQL query:
SELECT COUNT(subscriber_type), 
FROM `bigquery-public-data.san_francisco.bikeshare_trips` 
WHERE EXTRACT(DAYOFWEEK FROM start_date) NOT IN (1, 7) and (
(EXTRACT(HOUR FROM start_date) >= 6 and EXTRACT(HOUR FROM start_date) <= 9) 
OR (EXTRACT(HOUR FROM start_date) >= 16 and EXTRACT(HOUR FROM start_date) <= 18))
  
  
  
- Question 4:How many trips are there during weekdays? How trips are there during weekends?
  * Answer: Answers under each of the query. 
  
  * SQL query:

SELECT COUNT(subscriber_type), 
FROM `bigquery-public-data.san_francisco.bikeshare_trips` 
WHERE EXTRACT(DAYOFWEEK FROM start_date) NOT IN (1, 7)
-- total number of trips from both subscribers and customers take during weekdays is 871994

SELECT COUNT(subscriber_type), 
FROM `bigquery-public-data.san_francisco.bikeshare_trips` 
WHERE EXTRACT(DAYOFWEEK FROM start_date) IN (1, 7) 
-- total number of trips from both subscribers and customers take during weekdays is 111654


- Questions 5: On Average, how long are customers and subscribers trips times? 
  * Answer: I exluded any trip time that was over 24 hours because they would sku the data and are considered outliers in this situation. See Part 1
            --Average Customer trip time: 53.45 mins
            --Average Subscriber trip time: 9.50 mins

  * SQL query:
  
SELECT subscriber_type, AVG(duration_sec)/60 as average_trip_time_in_min
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
WHERE duration_sec < 86400
GROUP BY subscriber_type
  

- Question 6: How many stations are there in each city? Sort by descending order. 
  * Answer:
+--------------------+---------------+
| number_of_stations |     City      |
+--------------------+---------------+
|                 37 | San Francisco |
|                 18 | San Jose      |
|                  7 | Mountain View |
|                  7 | Redwood City  |
|                  5 | Palo Alto     |
+--------------------+---------------+

  * SQL query:
  
SELECT count(name) AS number_of_stations, landmark AS City 
FROM `bigquery-public-data`.san_francisco.bikeshare_stations
GROUP BY City
ORDER BY number_of_stations DESC 


- Question 7: What are the total number of trips from one station to the next for all stations? How about during weekdays? How about during weekends? 

   * Answer: Answers uncder each Query
   
SELECT start_station_name, end_station_name, count(*) as no_of_trips 
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
WHERE EXTRACT(DAYOFWEEK FROM start_date) IN (1, 7) 
GROUP BY start_station_name, end_station_name 
ORDER BY no_of_trips DESC 
LIMIT 10;

This is the results for weekends. 
+--------------------------------------+--------------------------------------+-------------+
|          start_station_name          |           end_station_name           | no_of_trips |
+--------------------------------------+--------------------------------------+-------------+
| Harry Bridges Plaza (Ferry Building) | Embarcadero at Sansome               |        2078 |
| Embarcadero at Sansome               | Harry Bridges Plaza (Ferry Building) |        1183 |
| Embarcadero at Sansome               | Embarcadero at Sansome               |        1146 |
| Harry Bridges Plaza (Ferry Building) | Harry Bridges Plaza (Ferry Building) |        1122 |
| Embarcadero at Bryant                | Embarcadero at Sansome               |         701 |
| Embarcadero at Bryant                | Harry Bridges Plaza (Ferry Building) |         612 |
| Embarcadero at Vallejo               | Embarcadero at Sansome               |         603 |
| University and Emerson               | University and Emerson               |         599 |
| 2nd at Townsend                      | Harry Bridges Plaza (Ferry Building) |         577 |
| Powell Street BART                   | Market at 10th                       |         564 |
+--------------------------------------+--------------------------------------+-------------+


SELECT start_station_name, end_station_name, count(*) as no_of_trips 
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
WHERE EXTRACT(DAYOFWEEK FROM start_date) NOT IN (1, 7) 
GROUP BY start_station_name, end_station_name 
ORDER BY no_of_trips DESC 
LIMIT 10;

This is the results for weekdays. 
+-----------------------------------------------+------------------------------------------+-------------+
|              start_station_name               |             end_station_name             | no_of_trips |
+-----------------------------------------------+------------------------------------------+-------------+
| San Francisco Caltrain 2 (330 Townsend)       | Townsend at 7th                          |        8048 |
| Harry Bridges Plaza (Ferry Building)          | Embarcadero at Sansome                   |        7072 |
| 2nd at Townsend                               | Harry Bridges Plaza (Ferry Building)     |        7043 |
| Townsend at 7th                               | San Francisco Caltrain 2 (330 Townsend)  |        6471 |
| Embarcadero at Sansome                        | Steuart at Market                        |        6419 |
| Harry Bridges Plaza (Ferry Building)          | 2nd at Townsend                          |        6378 |
| Embarcadero at Folsom                         | San Francisco Caltrain (Townsend at 4th) |        6158 |
| Steuart at Market                             | 2nd at Townsend                          |        5794 |
| Steuart at Market                             | San Francisco Caltrain (Townsend at 4th) |        5772 |
| Temporary Transbay Terminal (Howard at Beale) | San Francisco Caltrain (Townsend at 4th) |        5764 |
+-----------------------------------------------+------------------------------------------+-------------+
   
   
SELECT start_station_name, end_station_name, count(*) as no_of_trips 
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
GROUP BY start_station_name, end_station_name 
ORDER BY no_of_trips DESC 
LIMIT 10;

This is for weekends plus weekdays. 
+------------------------------------------+------------------------------------------+-------------+
|            start_station_name            |             end_station_name             | no_of_trips |
+------------------------------------------+------------------------------------------+-------------+
| Harry Bridges Plaza (Ferry Building)     | Embarcadero at Sansome                   |        9150 |
| San Francisco Caltrain 2 (330 Townsend)  | Townsend at 7th                          |        8508 |
| 2nd at Townsend                          | Harry Bridges Plaza (Ferry Building)     |        7620 |
| Harry Bridges Plaza (Ferry Building)     | 2nd at Townsend                          |        6888 |
| Embarcadero at Sansome                   | Steuart at Market                        |        6874 |
| Townsend at 7th                          | San Francisco Caltrain 2 (330 Townsend)  |        6836 |
| Embarcadero at Folsom                    | San Francisco Caltrain (Townsend at 4th) |        6351 |
| San Francisco Caltrain (Townsend at 4th) | Harry Bridges Plaza (Ferry Building)     |        6215 |
| Steuart at Market                        | 2nd at Townsend                          |        6039 |
| Steuart at Market                        | San Francisco Caltrain (Townsend at 4th) |        5959 |
+------------------------------------------+------------------------------------------+-------------+
   
 
- Question 8: What are the different subscriber types? And how many of each type are there?

   * Answer: Answers are under each query 
   
   * SQL Queries:
   
SELECT distinct subscriber_type
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
--Two types, customers and subscribers. 

SELECT count(subscriber_type)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
WHERE subscriber_type = 'Customer'
--Number of subscriber types that are "Customer":136809

SELECT count(subscriber_type)
FROM `bigquery-public-data`.san_francisco.bikeshare_trips
WHERE subscriber_type = 'Subscriber'
--Number of subscriber types that are "Subscriber": 846839
   
   
   
   
---

## Part 3 - Employ notebooks to synthesize query project results

### Get Going

Create a Jupyter Notebook against a Python 3 kernel named Project_1.ipynb in the assignment branch of your repo.

#### Run queries in the notebook 

At the end of this document is an example Jupyter Notebook you can take a look at and run.  

You can run queries using the "bang" command to shell out, such as this:

```
! bq query --use_legacy_sql=FALSE '<your-query-here>'
```

- NOTE: 
- Queries that return over 16K rows will not run this way, 
- Run groupbys etc in the bq web interface and save that as a table in BQ. 
- Max rows is defaulted to 100, use the command line parameter `--max_rows=1000000` to make it larger
- Query those tables the same way as in `example.ipynb`

Or you can use the magic commands, such as this:

```sql
%%bigquery my_panda_data_frame

select start_station_name, end_station_name
from `bigquery-public-data.san_francisco.bikeshare_trips`
where start_station_name <> end_station_name
limit 10
```

```python
my_panda_data_frame
```

#### Report in the form of the Jupter Notebook named Project_1.ipynb

- Using markdown cells, MUST definitively state and answer the two project questions:

  * What are the 5 most popular trips that you would call "commuter trips"? 
  
  * What are your recommendations for offers (justify based on your findings)?

- For any temporary tables (or views) that you created, include the SQL in markdown cells

- Use code cells for SQL you ran to load into Pandas, either using the !bq or the magic commands

- Use code cells to create Pandas formatted output tables (at least 3) to present or support your findings

- Use code cells to create simple data visualizations using Seaborn / Matplotlib (at least 2) to present or support your findings

### Resource: see example .ipynb file 

[Example Notebook](example.ipynb)

