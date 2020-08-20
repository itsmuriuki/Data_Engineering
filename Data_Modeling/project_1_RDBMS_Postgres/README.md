# Project 1: Data modelling with RDBMS (Postgres) 

## Summary
* [Introduction](#Introduction)
* [ProjectDescription](#Project Description)
* [Schema definition](#Schema-definition)
* [Project structure](#Project-structure)
* [Example queries](#Example-queries)
--------------------------------------------


#### Introduction

A startup called Sparkify want to analyze the data they have been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.

The aim is to create a Postgres Database Schema and ETL pipeline to optimize queries for song play analysis.

#### Project Description

In this project, I have to model data with Postgres and build and ETL pipeline using Python. On the database side, I have to define fact and dimension tables for a Star Schema for a specific focus. On the other hand, ETL pipeline would transfer data from files located in two local directories into these tables in Postgres using Python and SQL

#### Data

There are 2 different types of Data that is available for the Sparkify music streaming application amd they are stored as JSON files. The following are the details regarding the same:

Song Files: It has all Songs, Albums and Artist related details. Here is one sample row: { "num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0 }

Log Files: It has the logs of the user's music listening activity on the app. Here is one sample row: { "artist":null, "auth":"Logged In", "firstName":"Walter", "gender":"M", "itemInSession":0, "lastName":"Frye", "length":null, "level":"free", "location":"San Francisco-Oakland-Hayward, CA", "method":"GET", "page":"Home", "registration":1540919166796.0, "sessionId":38, "song":null, "status":200, "ts":1541105830796, "userAgent":""Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36"","userId":"39" }

#### Schema definition
This is the schema of the database

How to read the schema: 
* Blank bullets are used to identify the fields that can be null <br>
* Black bullets are used to identify the fields that can not be null <br>
* If the field is underlined means that is a primary key <br>


![schema](./imgs/SongPlayAnalysisSchema.png)


To represent this context a ``Star schema`` has been used <br>

The songplays table is the core of this schema, is it our fact table and <br>
it contains foreign keys to four tables;
* start_time REFERENCES time(start_time)
* user_id REFERENCES time(start_time)
* song_id REFERENCES songs(song_id)
* artist_id REFERENCES artists(artist_id)

--------------------------------------------

