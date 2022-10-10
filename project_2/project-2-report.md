## Project 2: Report 

### Overview

For this project, we were given a dataset in JSON format. We were instructed to create a service that delivers assessments, and different customers want to public their assessments on it. We are to get it ready for data scientists who work for these customers to run queries on the data. 

### Data Pipeline

For this project, the following tools were used:

* Docker-compose: It was used to form containers and provide an environment to work in. 
* Kafka: It was used to capture data in a topic. Kafka was also used to manipulat the JSON file a bit.
* Spark: It was used to read data from Kafka in JSON format and exract it. Spark was the main tool used to manipulate the data.
* Hadoop: It was used to store data in certain formats so that data scientists can easily access the data and answer business questions. 


Refer to `pipline.md` for the commands that were used for the above tools. Also, `aazizi13-sparkhistory` shows all the code in spark terminal.  

 
### JSON file structure 

For this project, we were given a JSON file that was multi-layered. In other words, it had nested dictionaries that had nested dictionaries and so on. 

Looking at the JSON file, the following variables were present. *Note: the definition of the values were not present for the data. Therefore, I am only guessing what the values mean.*

* base_exam_id: exam ID
* exam_name: exam name 
* certification: I am assuming people who passed may have gotten some sort of certification. 
* keen_id: perhaps the profile created by assessment-takers to record an assessment attempt
* keen_created_at: Time stamp for when "keen" was created. I do not know what keen means.  
* keen_timestamp: Not sure what is the difference between this value and keen_created_at value. 
* max_attempts: number of maximum attempts allowed 
* sequences: nested value with the following sub-values:
  * attempt: the number of attempt
  * counts: another nested value that contains the following sub-values:
    * all_correct: whether all questions were answered correctly or not
    * correct: number of correct answers
    * incomplete: number of incomplete answers
    * incorrect: number of incorrect answers
    * submitted: number of submitted answers
    * total: total number of questions that was attempted
    * unanswered: number of questions left unanswered. Maybe this values referes to question that was not even opened. 
  * id: not sure what this referes to. Maybe ID of each question. 
  * questions: nested value with following sub-values:
    * id: ID of the question
    * options: nested value with the following sub-values:
      * at: timestamp when the question was attempted
      * checked: question was checked
      * correct: question was answered correctly
      * id: I do not know what this refers to. 
      * submitted: tracking whether the question was submitted 
    * user_correct: relating to questions correct
    * user_incomplete: relating to completion of the questions
    * user_result: relating to results of the user
    * user_submitted: relating to submission by the user 
 * started_at: When the exam was started
 * user_exam_id: Not sure what this refers to 

### Important Code bit

An important thing to point out. Since the JSON file had a nested value, once my data was ready to be manipulated in pyspark and the JSON nested file ready to be unrolled, I wrote a function in PySpark directly that extracted "total" and "correct" sub-values from the "sequences" values. The fucntion could be tweated as necessary to extract other sub-values as needed. *Refer to `pipline.md` for more details.* 


```Python 

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
```

### Sample Business Questions to Answer Using the Data

Before I answer any questions, since I do not have information about what the actual values mean, I am assuming the following. 

* I am assuming my definitions for each value as listed above is correct. 
* I am also assuming that each row is unique and that there are no duplicates.

After exploring the data set, I will answer the following the questions. 

* How many total assessments are in our data set?
* What are the 5 most common and 5 least common course taken?
* Which 5 exams had the highest scores on average? Lowest scores?

I chose the above questions because those are the sort of question data scientist typically would like to study for a preliminary assessment. 
We can use the extracted_assessments table that we have saved in hadoop to answer the first and second questions 

```
extracted_assessments.registerTempTable('assessments')
```
*Question 1: How many total assessments are our data set?*

*Answer: it appears that there are a total of 3280 assessments in our data base.* 

```
spark.sql("select count(keen_id) from assessments").show()

#output

+--------------+
|count(keen_id)|
+--------------+
|          3280|
+--------------+

```

*Question 2: What are the 5 most common and 5 least common course taken?*

*Answer: To answer this question, I used `exam_name` value and counted the 5 most common and 5 least courses taken. Note: The result might be differnt if different values are used to answer this question. Nevertheless, refer to the below outputs for answers.*

```
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
```

*Question 3: Which 5 exams had the highest scores on average? Lowest scores?*

*Answer: To answer this question, I had to use sub-values from the "sequences" value and to avoid any missing information if there are any, I wrote a function such that it only runs for each row if "correct" and "total" sub-values were present ("correct" and "total" sub-values were needed to answer this question). The answers can be found in tables below*

```
from pyspark.sql import Row
```

```python
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
```

### Guide for using this data set:

To study the desired data, please refer to `pipeline.md` and tweak the function in spark to get designated sub-values from "sequences". You can further create temporary tables and queries as you can see above. Once the desired table structure is achieved, you can store them in hdfs. 


```
#your desired information with the designated sql query. Below is a sample query

your_desired_assessment_info = spark.sql("select keen_id, exam_name from assessments limit 10")
```

```
#You can then store that table to hdfs 
your_desired_assessment_info.write.parquet("/tmp/your_desired_assessment_info")
```

Check the results in Hadoop by using the following code:

```
# check all the files stored as hdfs
docker-compose exec cloudera hadoop fs -ls /tmp/

# check specific files that you would like to use for your analysis

docker-compose exec cloudera hadoop fs -ls /tmp/your_desired_assessment_info/
```
