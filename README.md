# Data Lake Udacity project

## Goal of this project 
The goal of this project is to create an ETL process to extract, transform and load data from existing JSON log and song files from S3 bucket to AWS S3 data lake as parquet format with denormalised star schema. Denormalised star schema will allow to quick analysys of data by using simple queries without JOIN statements. EMR AWS service allows to choose several cores to process the data.  My personal goal in this project is to gain experience in building ETL pipelines with AWS EMR :blush::blush::blush:.


# ETL Process

Data is read in data spark frames, which allows to implement lazy evaluation of the data. Data is read from JSON song and log files from S3 bucket. After that final denormalised parquet files created and saved in S3 bucket.

# Parquet files

3. **songplays_table.parquet** - records with page NextSong

> - **songplay_id**, **start_time**, **user_id**, **level**, **song_id**, **artist_id**, **session_id**, **location**, **user_agent**

4. **users_table.parquet** - users in the app 

> - **user_id**, **first_name**, **last_name**, **gender**, **level**

5. **songs_table.parquet** - songs 

> - **song_id**, **title**, **artist_id**, **year**, **duration**

6. **artists_table.parquet** - artists 

> - **artist_id** , **name**, **location**, **latitude**, **longitude**

7. **time_table.parquet** - timestamps of records in **songplays** broken down into specific units

> - **start_time** , **hour** , **day** , **week** , **month** , **year** , **weekday**

# Datasets

## Song Dataset

This dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).
Example of JSON file in data/songs directory:

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

## Log Dataset

This dataset represents activities of users which are then stored in JSON log files.
Example of JSON file in data/logs directory:

{"artist":null,"auth":"Logged In","firstName":"Adler","gender":"M","itemInSession":0,"lastName":"Barrera","length":null,"level":"free","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"GET","page":"Home","registration":1540835983796.0,"sessionId":248,"song":null,"status":200,"ts":1541470364796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.78.2 (KHTML, like Gecko) Version\/7.0.6 Safari\/537.78.2\"","userId":"100"}


# Project Files Structure Description

1. **dwh.cfg** - contains credentials for S3 buckets 

2. **etl.py** - extract transform and load data from logs and songs datasets to S3 bucket in parquet file format



