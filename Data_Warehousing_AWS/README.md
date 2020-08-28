# Building a Data warehouse and ETL pipeline on Redshift

## Summary
* [Introduction](#Introduction)
* [Project_Description](#Project_Description)
* [Data](#Data)
* [How to run](#How-to-run)
* [Project structure](#Project-structure)
-------------------------------------------

### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Task is building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional and fact tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project_Description
Application of Data warehouse and AWS to build an ETL Pipeline for a database hosted on Redshift Will need to load data from S3 to staging tables on Redshift and execute SQL Statements that create fact and dimension tables from these staging tables to create analytics

### Data
### Log Dataset

{"artist":"Pavement", "auth":"Logged In", "firstName":"Sylvie", "gender", "F", "itemInSession":0, "lastName":"Cruz", "length":99.16036, "level":"free", "location":"Klamath Falls, OR", "method":"PUT", "page":"NextSong", "registration":"1.541078e+12", "sessionId":345, "song":"Mercy:The Laundromat", "status":200, "ts":1541990258796, "userAgent":"Mozilla/5.0(Macintosh; Intel Mac OS X 10_9_4...)", "userId":10}

### Song Dataset

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
## Schema for fact table and dimensional tables

### fact table - songplays
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimensional tables - users, songs, artists, time

users - user_id, first_name, last_name, gender, level
songs - song_id, title, artist_id, year, duration
artists - artist_id, name, location, latitude, longitude
time - start_time, hour, day, week, month, year, weekday

## Schema for staging tables - stagingevents, stagingsongs

stagingevents - event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId

stagingsongs - num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year

<b>Build ETL Pipeline</b>

Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.

### How to run
1) Configuration setup - Fill the dwh.cfg with the necessary information to start a redshift cluster
2) Run create_redshift_cluster - Run this jupyter notebook and create the cluster
3) Run create_tables.py - Use this python file to drop and create tables
4) Run etl.py - Run this python file to create the etl pipeline to insert data into the created tables.
5) Run test_analytics.py - This python file to run some basic analytical queries.

#### Don't forget to run the last steps in the jupyter notebook to delete the cluster.

### Project Structure

Project Template include four files:

1. create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.

2. etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

3. sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.

4. create_redshift_cluster is where you create your redshift cluster 

5. README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.


