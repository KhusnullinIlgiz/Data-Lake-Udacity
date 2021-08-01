import configparser
from datetime import datetime
import os
import glob
from pyspark.sql import SparkSession, types
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col,concat_ws, from_unixtime, substring, monotonically_increasing_id  
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pandas as pd


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('default','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('default','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    This function creates spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark



def get_files(filepath):
    """
    This function is used to join paths and read all filenames with their paths. For local file system only.
    Input: filepath -> String
    Output: all_files -> list of String
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files



def process_song_data(spark, input_data, output_data):
    """
    This function is used for preprocess data from song_data files.
    Creates parquet songs_table.parquet and artists_table.parquet files and writes them to the output directory.
    Input: spark -> spark session; input_data -> song_data files directory; output_data files directory
    Output: None
    """
    # get filepath to song data file
    #song_data =get_files(input_data) -> for local use only
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json") 
    
    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(os.path.join(output_data, 'songs_table.parquet'))

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude')
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, 'artists_table.parquet'))


def process_log_data(spark, input_data, output_data):
    """
    This function is used for preprocess data from log_data files.
    Creates parquet users_table.parquet, time_table.parquet, songplays_table.parquet files and writes them to the output directory.
    Input: spark -> spark session; input_data -> song_data files directory; output_data files directory
    Output: None
    """
    
    # get filepath to log data file
    #log_data =get_files(input_data) -> for local use only
    log_data =os.path.join(input_data,"log_data/*/*/*.json") 
    

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    #df.printSchema()

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id","firstName as first_name","lastName as last_name","gender","level")
    #users_table.printSchema()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, 'users_table.parquet'))

    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", from_unixtime((col("ts")/1000),"yyyy-MM-dd HH:mm:ss"))
    
    # extract columns to create time table
    time_table = df.selectExpr("timestamp as start_time","hour(timestamp) as hour","day(timestamp) as day","weekofyear(timestamp) as week","month(timestamp) as month", "year(timestamp) as year","dayofweek(timestamp) as weekday")
    
    #time_table.show(10)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode("overwrite").parquet(os.path.join(output_data, 'time_table.parquet'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet('songs_table.parquet')
    song_df.printSchema()

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("songsView")
    df.createOrReplaceTempView("logsView")
    songplays_table = spark.sql("""
        SELECT  logsView.timestamp as start_time,
                logsView.userId as user_id,
                logsView.level as level,
                songsView.song_id as song_id,
                songsView.artist_id as artist_id,
                songsView.sessionId as session_id,
                songsView.location as location,
                songsView.userAgent as user_agent,
                year(logsView.timestamp) as year,
                month(logsView.timestamp) as month
        FROM logsView
         JOIN songsView ON (logsView.song=songsView.title)
    """)
    
    
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())
    songplays_table = songplays_table.select("songplay_id","start_time","user_id","level","song_id","artist_id","session_id","location","user_agent")
    # write songplays table to parquet files 
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet(os.path.join(output_data, 'songplays.parquet'))
    


def main():
    """
    This function is used for execution ETL process by executing process_song_data and process_log_data functions.
    Input: None
    Output: None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    


if __name__ == "__main__":
    main()
