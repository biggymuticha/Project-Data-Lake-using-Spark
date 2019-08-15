**Instructions**

1. Business case
Sparkify company  has grown and is moving its data warehouse to a data lake. It's data is stored in Amazon S3 in the form of JSON log files containing it's users' activities as well as it's songs' metadata. The company needs an ETL pipeline developed which will extract data from the json files in S3 , processes them using Spark, and loads the data back into S3 as a set of dimensional tables so that the analytics team can continue unlocking insights into what songs their users are listening to.

2. Data extraction , transformation and loading
A python script file etl.py has been developed to extract, transform and load user activity information from S3 using Spark and writing back into S3

3. Execute ETL pipeline
i. Open the Execute.ipynb notebook
ii. Run the first line of code  which runs the etl.py script to extract data from S3 using Spark and writing the transformed fact and dimension tables back to S3


4. Database design

**Fact table**

**i. songplays table : play records from the log data**

columns:

songplay_id bigint monotonically_increasing_id(1) 
start_time timestamp 
user_id int 
level string 
song_id string
artist_id string
session_id int 
location string 
user_agent string 
year int  -> partition column
month int  -> partition column


**Dimension tables**

**i. users : record of users in the app **

columns:

user_id int 
first_name string 
last_name string 
gender string
level string


**ii.  songs : records of available songs in the music database **

columns: 

song_id string
title string 
artist_id string
year int
duration double 


**iii. artists : artists in the music database**

columns:

artist_id string
name string
location string
latitude float
longitude float

**iv: time : timestamps of records in songplays broken down into specific units**


columns:

start_time timestamp  
hour int 
day int 
week int 
month int 
year int 
weekday int 
