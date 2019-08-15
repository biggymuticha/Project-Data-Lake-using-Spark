import configparser
from datetime import datetime
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, unix_timestamp, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_utc_timestamp, monotonically_increasing_id
import zipfile
import re
import json
import pandas as pd
import numpy as np
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, TimestampType
from pyspark.sql.types import StructType, StructField
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SQLContext
from pyspark import SparkContext
import pyspark.sql.functions as F
import s3fs
import boto3
from boto.s3.connection import S3Connection
from s3fs import S3FileSystem
#s3 = S3FileSystem() # or s3fs.S3FileSystem(key=ACCESS_KEY_ID, secret=SECRET_ACCESS_KEY)
from botocore.client import ClientError

s3 = boto3.resource('s3')
songs_bucket = 'songs'
#s3.Bucket('songs')
songplays_bucket = 'songplays'
artists_bucket = 'artists'



config = configparser.ConfigParser()
config.read('dl.cfg')
KEY                         = config.get('AWS','AWS_ACCESS_KEY_ID')
SECRET                      = config.get('AWS','AWS_SECRET_ACCESS_KEY')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
fs = s3fs.S3FileSystem(key=KEY, secret=SECRET)


def create_spark_session():
    """
    Purpose: this function creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """Summary or Description of the Function
    Parameters:
    argument1 (spark): spark session
    argument2 (input_data): Path to the directory with the json zipped files containing songs data
    argument3 (output_data) : Path to the directory to which transformed songs data is written
    
    Purpose: function extracts songs data and writes songs (partitioned by year and artist) and artists tables data onto the directory name of the output_data variable 
    
    return value : song_artist_df  (data frame): song_artist records to combine with song event longs to produce the songplay data
    """
    
    sc = spark.sparkContext
    #spark = SparkSession(sc)  
    # get filepath to song data file
    song_data_files = []
    song_data = []
    #sd_df = []
    file_paths = []
    df = []
    
    client                      = boto3.client('s3',
                                    region_name="us-west-2",
                                    aws_access_key_id=KEY,
                                   aws_secret_access_key=SECRET
                                )
    
    song_dataSchema = StructType([
      StructField("num_songs", IntegerType()),
      StructField("artist_id", StringType()),
      StructField("artist_latitude", StringType()),
      StructField("artist_longitude", StringType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType())]    
    )
    song_data_columns=["num_songs","artist_id","artist_latitude","artist_longitude","artist_location",\
                       "artist_name","song_id","title","duration","year"]
  
       

    
    #hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop:hadoop-aws:2.7.0")
    #hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    #hadoop_conf.set("fs.s3n.awsAccessKeyId", KEY)
    #hadoop_conf.set("fs.s3n.awsSecretAccessKey", SECRET)
    
    #song_data = spark.read.json(input_data + "/*/*/*", song_dataSchema)
    
    cnt = 0
    all_files = []
    all_folders = []
    song_data_df = []
    
     # Get list of folders with json files
    print('Scanning folders with json files.....')
    foldersL1 = client.list_objects_v2(Bucket='udacity-dend', Prefix='song_data/', Delimiter='/')
    for folder1 in foldersL1.get('CommonPrefixes'):
        #print(folder1.get('Prefix'))
        foldersL2=client.list_objects_v2(Bucket='udacity-dend', Prefix=folder1.get('Prefix'), Delimiter='/')
        for folder2 in foldersL2.get('CommonPrefixes'):
            #print(folder2.get('Prefix'))
            foldersL3=client.list_objects_v2(Bucket='udacity-dend', Prefix=folder2.get('Prefix'), Delimiter='/')
            for folder3 in foldersL3.get('CommonPrefixes'):
                foldername = input_data + folder3.get('Prefix')        # 's3://udacity-dend/' 
                all_folders.append(foldername)
                print(foldername)
                cnt = cnt + 1
    
    if len(all_folders)>0:
        print('{} folders with json files found. First folder: {}.  Last folder: {}'.format(len(all_folders), all_folders[0], all_folders[len(all_folders)-1])) 
    
    cnts = 0
    df_songs = spark.createDataFrame(sc.emptyRDD(), song_dataSchema)
    print("Extracting all songs data...")
    for folder_path in all_folders:
        cnts = cnts + 1
        songs = spark.read.json(folder_path + "*.json",song_dataSchema)
        #songs.show()
        #print(folder_path + "*.json")
        #print("Type of songs: " + str(type(songs)))
        #print("Type of df_songs: " + str(type(df_songs)))
        df_songs = df_songs.unionAll(songs)
        #df_songs.show()
        
        #Uncomment to test run with files in only 2 directories
        #if cnts == 2:
        #    break
    #song_data.show()
    #song_data_df = spark.read.json("s3a://udacity-dend/song_data/A/A/A/*.json")
    #df_songs = pd.DataFrame((song_data), columns = song_data_columns)
    #print(df)
    #song_data_df = spark.createDataFrame(df_songs,song_dataSchema)
    song_data_df = df_songs
    song_data_df.show()
    
    song_data_df.createOrReplaceTempView("vwsong_data")
    spark.sql("SELECT count(*) FROM vwsong_data").show()
    
    print("Printing songs data schema..")
    song_data_df.printSchema()
     # extract columns to create songs table
    print("Extracting data for songs table ....")

    songs_table = spark.sql("SELECT song_id, title, artist_id, year, duration FROM vwsong_data")
    
    songs_table.show()
    print("Data  extraction completed successfully")
    
    # writing songs table to parquet files partitioned by year and artist
    print('Writing songs table data...')
    songs_table_pa = pa.Table.from_pandas(songs_table.toPandas())
    print(songs_table_pa)
    
               
    pa.parquet.write_to_dataset(songs_table_pa, root_path= output_data + '/songs', partition_cols=['year', 'artist_id'], filesystem=fs)
   
    
    print('*Songs table data successfully written.')
    
    # read in song data to use for songplays table
    song_artist_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name, title, duration FROM vwsong_data") 

    # extract columns to create artists table
    print("Extracting data for artist table ....")
    spark.sql("SELECT artist_id, artist_name as name, artist_location as location, \
              cast(artist_latitude as float) as latitude, cast(artist_longitude as float) as longitude FROM vwsong_data").show()
    
    artists_table = spark.sql("SELECT artist_id, artist_name as name, artist_location as location, \
                               cast(artist_latitude as float) as latitude, cast(artist_longitude as float) as longitude FROM vwsong_data")
    print("Data  extraction completed successfully")
    
    # write artists table to parquet files
    print('Writing artists table data...')
    artists_table_pa = pa.Table.from_pandas(artists_table.toPandas())
    print(artists_table_pa)
    pa.parquet.write_to_dataset(artists_table_pa, root_path=output_data + '/artists', filesystem=fs)
    print('*Artists table data successfully written.')
    
    
    return song_artist_df



def process_log_data(spark, input_data, output_data, song_artist_df):
    """Summary or Description of the Function
    Parameters:
    argument1 (spark): spark session
    argument2 (input_data): Path to the directory with the json zipped files containing log data files for the song play events
    argument3 (data frame): song_artist records to combine with song event longs to produce the songplay data
    
    Purpose: function extracts log file events data and writes users, time (partitioned by year and month) and songplays tables data (partitioned by year and month) onto the directory name of the output_data variable 
    """
    
    client                      = boto3.client('s3',
                                    region_name="us-west-2",
                                    aws_access_key_id=KEY,
                                   aws_secret_access_key=SECRET
                                )
    
    cnt = 0
    log_data = []
    log_data_files = []
    file_paths = []
    all_files = []
    all_folders = []
    
    log_dataSchema = StructType([
      StructField("artist", StringType()),
      StructField("auth", StringType()),
      StructField("firstName", StringType()),
      StructField("gender", StringType()),
        StructField("ItemInSession", StringType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", StringType()),
        StructField("sessionId", IntegerType()),
        StructField("song", StringType()),
        StructField("status", StringType()),
        StructField("ts", LongType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType())
    ]    
    )
    log_data_columns=["artist","auth","firstName","gender","ItemInSession",\
                       "lastName","length","level","location","method","page","registration",\
                       "sessionId","song","status","ts","userAgent","userId"]
    
    
    # Get list of folders with json files
    cnt = 0
    all_files = []
    all_folders = []
    
     # Get list of folders with json files
    print('Scanning folders with json files.....')
    foldersL1 = client.list_objects_v2(Bucket='udacity-dend', Prefix='log_data/', Delimiter='/')
    for folder1 in foldersL1.get('CommonPrefixes'):
        #print(folder1.get('Prefix'))
        foldersL2=client.list_objects_v2(Bucket='udacity-dend', Prefix=folder1.get('Prefix'), Delimiter='/')
        for folder2 in foldersL2.get('CommonPrefixes'):
                foldername = input_data + folder2.get('Prefix')        # 's3://udacity-dend/' 
                all_folders.append(foldername)
                print(foldername)
                cnt = cnt + 1
                cntf = 0
    
    objs = client.list_objects_v2(Bucket='udacity-dend', Prefix='log_data/', StartAfter='log_data/')
    #while 'Contents' in objs.keys() :
    objs_contents = objs['Contents']
    print("Length :=>" + str(len(objs_contents)) )
    for i in range(len(objs_contents)):
        cntf = cntf + 1
        filename = input_data + objs_contents[i]['Key']
        all_files.append(filename)
            
        # Uncomment to print the name of every json songs log  file 
        print('Filename: {}'.format(filename))
        
        # Uncomment to test with 3 log files
        #if cntf == 3:
        #    break
     
    # Check the number of files found
    # Print the the number of files found
    print('{} files found. First file: {}.  Last file: {}'.format(len(all_files), all_files[0], all_files[len(all_files)-1]))
    
    if len(all_folders)>0:
        print('{} folders with json files found. First folder: {}.  Last folder: {}'.format(len(all_folders), all_folders[0], all_folders[len(all_folders)-1])) 
    
    for file_path in all_files:
        log_data = spark.read.json(file_path)
    
    
    #df_log = spark.createDataFrame(log_data, columns = log_data_columns)
    #print(df)
    log_data_df =  log_data  #spark.createDataFrame(log_data,log_dataSchema)
    #song_data_df.show()
    
    # filter by actions for song plays
    df = log_data_df[log_data_df["page"]=="NextSong"]
    df.createOrReplaceTempView("vwlog_data")
    spark.sql("SELECT artist, auth, firstName, page, ts FROM vwlog_data").show()
    
    #Uncomment to print number of log data records
    spark.sql("SELECT count(*) FROM vwlog_data").show()
    print("Printing log data schema..")
    log_data_df.printSchema()
       

    # extract columns for users table    
    #df.show()
    users_table = spark.sql("SELECT DISTINCT cast(userId as int) as user_id, firstName as first_name, lastName as last_name, gender, level FROM vwlog_data ORDER BY user_id")
    # users_table.show()
    
    # write users table to parquet files
    print('Writing users table data...')
    users_table_pa = pa.Table.from_pandas(users_table.toPandas())
    print(users_table_pa)
    pa.parquet.write_to_dataset(users_table_pa, root_path=output_data + '/users', filesystem=fs)
    print('*Users table data successfully written.')
    
    # def get_ts(timestamp):
    #    return to_datetime(timestamp,unit='ms') 
   
    # create timestamp column from original timestamp column
    spark.udf.register("get_timestamp", lambda x: pd.to_datetime(x, unit='ms'))
        
    df_ts =  spark.sql("""SELECT DISTINCT(TO_DATE(Cast(get_timestamp(ts) as Timestamp))) as start_time FROM vwlog_data""")
    
    #print(df_ts.show())
    #spark.sql("SELECT DISTINCT get_timestamp(ts) as ts1 FROM vwlog_data").show()
    td=df.toPandas()
    td1=pd.to_datetime(td['ts'], unit='ms')
    #df1.rename(columns={'ts':'start_time'}, inplace=True)
    #print(td1)
    td2 = [td1.tolist(), td1.dt.hour.values.tolist(),td1.dt.day.values.tolist(),td1.dt.week.values.tolist(), td1.dt.month.values.tolist(), td1.dt.year.values.tolist(), td1.dt.weekday.values.tolist()]
    column_labels = ["start_time","hour","day","week","month","year","weekday"]
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, td2)))
    stime_df = spark.createDataFrame(time_df)
    stime_df.createOrReplaceTempView("vw_time")
    #spark.sql("SELECT * FROM vw_time").show()
    
    # extract columns to create time table
    time_table = spark.sql("SELECT * FROM vw_time")
    
     
    # write time table to parquet files partitioned by year and month
       
    print('Writing time table data...')
    time_table.show()
    time_table_pa = pa.Table.from_pandas(time_table.toPandas())
    print(time_table_pa)
    #write time table in folder 'time' and partition files by year and month
    pa.parquet.write_to_dataset(time_table_pa, root_path=output_data + '/time',partition_cols=['year', 'month'], filesystem=fs)
    print('*Time table data successfully written.')
    
                       
    song_data_df = song_artist_df    #(spark, input_data)
    df1 = df.withColumn('start_time', from_unixtime(F.col('ts')/1000).cast("timestamp"))
    df2 = df1.withColumn("year", year(col("start_time"))).withColumn("month", month(col("start_time")))
    df2.createOrReplaceTempView("vwlog_data1")
    #spark.sql("select * from vwlog_data1").show()
   
    print("Extracting data for songplays table ....")
    song_data_df.createOrReplaceTempView("vwsong_data")
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name, title, duration FROM vwsong_data")
    #song_df.show()
    song_df.createOrReplaceTempView("vwsong_artist_data")
    #print("New view:")
    #spark.sql("Select * from vwsong_artist_data").show()
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT vwlog_data1.start_time ,  \
    cast(vwlog_data1.userId as int), vwlog_data1.level, vwsong_artist_data.song_id, vwsong_artist_data.artist_id ,vwlog_data1.sessionId, \
    vwlog_data1.location, vwlog_data1.userAgent, vwlog_data1.year, vwlog_data1.month  FROM vwlog_data1  LEFT OUTER JOIN
    vwsong_artist_data ON   vwlog_data1.song = vwsong_artist_data.title AND \
    vwlog_data1.artist = vwsong_artist_data.artist_name AND vwlog_data1.length = vwsong_artist_data.duration""")
    songplays_table1 = songplays_table.withColumn("songplay_id",monotonically_increasing_id() + 1) 
    songplays_table1.show()
    print("Data  extraction completed successfully")
    
    # write songplays table to parquet files partitioned by year and month
    print('Writing songplays table data...')
    songplays_table_pa = pa.Table.from_pandas(songplays_table1.toPandas())
    print(songplays_table_pa)
    pa.parquet.write_to_dataset(songplays_table_pa, root_path=output_data + '/songplays', partition_cols=['year', 'month'], filesystem=fs)
    print('*Songplays table data successfully written.')



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "bm-bucket-udacity"
    
    #Uncomment to test writing the formatted tables onto local folders
    #input_data =  'data'
    #output_data =  'spark-warehouse'
    

    
    print('Processing song data in {} ...'.format(input_data ))
    song_artist_df = process_song_data(spark, input_data, output_data) 
    print('Processing log data .....' + input_data)
    process_log_data(spark, input_data, output_data, song_artist_df)


if __name__ == "__main__":
    main()
