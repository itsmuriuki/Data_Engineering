# Project 1: Data modelling with Apache Cassandra

## Summary
* [Introduction](#Introduction)
* [Project_Description](#Project_Description)
* [Data](#Data)
* [How to run](#How-to-run)
* [Project structure](#Project-structure)
* [Project steps](#project-steps)
* [CQL queries](#CQL-queries)
* [Notes](#Notes)
--------------------------------------------

### Introduction
    
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. There is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. My role is to create an Apache Cassandra database which can create queries on song play data to answer the questions.

### Project_Description

In this project, I would be applying Data Modeling with Apache Cassandra and complete an ETL pipeline using Python. The ETL pipeline transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

### Data
For this project, you'll be working with one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv


### How to run

You need a Cassandra instance up and running. <br>
You have to install also Python3 and Jupyter Notebook.

#### Project structure
This is the project structure, if the bullet contains ``/`` <br>
means that the resource is a folder:

* <b> /event_data </b> - The directory of CSV files partitioned by date
* <b> /images </b> - Simply a folder with images that are used in Project_1B_ Project_Template notebook
* <b> Project_1B_ Project_Template.ipynb </b> - It is a notebook that illustrates the project step by step
* <b> event_datafile_new.csv </b> - The aggregated CSV composed by all event_data files

### Project Steps

Below are steps you can follow to complete each component of this project.

<b>Modelling your NoSQL Database or Apache Cassandra Database:</b>
    
1.	Design tables to answer the queries outlined in the project template
2.	Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
3.	Develop your CREATE statement for each of the tables to address each question
4.	Load the data with INSERT statement for each of the tables
5.	Include IF NOT EXISTS clauses in your CREATE statements to create tables only if the tables do not already exist. We recommend you also include DROP TABLE statement for each table, this way you can run drop and create tables whenever you want to reset your database and test your ETL pipeline
6.	Test by running the proper select statements with the correct WHERE clause

<b>Build ETL Pipeline:</b>
1.	Implement the logic in section Part I of the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
2.	Make necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and INSERT three statements to load processed records into relevant tables in your data model
3.	Test by running three SELECT statements after running the queries on your database
4.	Finally, drop the tables and shutdown the cluster

#### CQL queries

<I> Query 1:  Give me the artist, song title and song's length in the music app history that was heard <br> during
 sessionId = 338 and itemInSession = 4 </I>
``` SQL
CREATE TABLE IF NOT EXISTS song_data_by_session (session_id INT,
        session_item_number INT,
        artist_name TEXT,
        song_title TEXT,
        song_length DOUBLE,
        PRIMARY KEY ((session_id, session_item_number)))
```
 In this case <b>session_id</b> and <b>session_item_number</b> are enough to
 make a record unique for our request.
 Our complete ``PRIMARY KEY`` is composed by <b>session_id</b>, <b>session_item_number </b>
 
``` SQL
SELECT artist_name, song_title, song_length
 FROM song_data_by_session
 WHERE session_id = 338 AND session_item_number =  4
```

<I> Query 2: Give me only the following: name of artist, song (sorted by itemInSession)  <br> and user (first and last name) for userid = 10, sessionid = 182 </I>
``` SQL
CREATE TABLE IF NOT EXISTS song_user_data_by_user_and_session_data (user_id INT,
        session_id INT,
        session_item_number INT,
        artist_name TEXT,
        song_title TEXT,
        user_first_name TEXT,
        user_last_name TEXT,
        PRIMARY KEY ((user_id, session_id), session_item_number))
```
In this case <b>user_id</b> and <b>session_id</b> are the ``COMPOUND PARTITION KEY``  
this allows us to have a unique ``PRIMARY KEY`` for our query, but for this request we have to 
order by session_item_number but not to query on that, so we have to declare <b>session_item_number</b> as ``CLUSTERING KEY``.  
Our complete PRIMARY KEY is composed by <b>user_id</b>, <b>session_id</b>, <b>session_item_number</b>
``` SQL
SELECT artist_name, song_title, user_first_name, user_last_name
 FROM song_user_data_by_user_and_session_data
 WHERE user_id = 10 AND session_id = 182
```

<I> Query 3: Give me every user name (first and last) in my music app history who listened <br> to the song 'All Hands Against His Own' </I>
``` SQL
CREATE TABLE IF NOT EXISTS user_data_by_song_title (song_title TEXT, user_id INT,
        user_first_name TEXT,
        user_last_name TEXT,
        PRIMARY KEY ((song_title), user_id))
```
In this case <b>song_title</b> is the ``PARTITION KEY`` and <b>user_id </b>
is the ``CLUSTERING KEY``, the request asks to retrieve the user name 
by song title, so we have to set <b>song_title</b> as ``PARTITION KEY``, but 
more users can listen to the same song so we may have many ``INSERT`` with the 
same key, Cassandra overwrites data with the same key so we need to add a ``CLUSTERING KEY`` 
because we need to have a unique record but not to query on that. 
Our complete ``PRIMARY KEY`` is composed by <b>song_title</b>, <b>user_id</b>
``` SQL
SELECT user_first_name, user_last_name
 FROM user_data_by_song_title
 WHERE song_title = 'All Hands Against His Own'
```

-------------------------------------------

### Notes

How Cassandra differs from relational database data modeling.

Let's start with ``PRIMARY KEY``, in Cassandra primary keys works slightly different from RDBMS ones, <br> in fact in Cassandra, the ``PRIMARY KEY`` is made up of either just the ``PARTITION KEY`` <br> or with the addition of ``CLUSTERING COLUMNS``.
And you can have just one record unique, <br> if we insert data with same ``PRIMARY KEY`` then data will be overwritten with the latest record state

In ``WHERE`` conditions the ``PRIMARY KEY`` must be included and you can use your ``CLUSTERING KEY`` <br> to order your data
but they are not necessary, by default data will be ordered as ``DESC``


In Cassandra the ``denormalization`` process is a must, you cannot apply normalization like an RDBMS <br> because you cannot use JOINs. More the denormalization process is done more the query will <br> run faster, in fact, Cassandra has been optimized for fast writes not for fast reads. <br>
To reach the complete denormalization you have to follow the pattern ``1 Query - 1 Table``.<br> This leads to data duplication but it does not matter, the denormalization process <br> by nature itself produces data duplication.


Following the ``CAP theorem``, Cassandra embraces the ``AP`` guarantees. <br>
It provides only AP because of its structure, data are shared by nodes so if <br>
a node goes down another one can satisfy the client request but due <br>
the high number of nodes data could be not updated for each node that is why <br>
Cassandra offers only ``Eventual Consistency``
