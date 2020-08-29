import configparser
from datetime import datetime 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']= config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']= config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    this functions loads song_data from s3, processes songs and artists tables loads them back to s3
        Parameters:
            spark       = Spark Session
            input_data  = location of song_data where the file is loaded to process
            output_data = location of the stored results
    """
    #get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    #read song data file 
    song_df = spark.read.json(song_data)

    #create song view to write SQL querries 
    song_df.createOrReplaceTempview("songs_table")

    #extract columns to create songs table
    songs_schema_table = spark.sql("""
                                    SELECT distinct s.song_id, 
                                        s.title,
                                        s.artist_id, 
                                        s.year, 
                                        s.duration
                                    FROM songs_table s
                                    WHERE song_id is NOT NULL
                                     """)
    
    #write songs table to parquet files partitioned by year and artist 
    songs_schema_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data+ 'songs_schema_table/')

    #extract columns to create artist table 
    artist_schema_table = spark.sql("""
                                    SELECT distinct a.artist_id,
                                        a.artist_name,
                                        a.artist_location,
                                        a.artist_latitude,
                                        a.artist_lngitude,
                                    FROM songs-table a
                                    WHERE a.artist_id is NOT NULL
                                    """)

    #write artists table to parquet files 
    artist_schema_table.write.mode('overwrite').parquet(output_data + 'artist_schema_table/')


def process_log_data(spark, input_data, output_data):
    """
        This function loads log data from s3, process songs and artist tables and loads the data
        back to S3
        parameters: 
            spark = spark session
            input_data = location of song_data where the file is loaded to process
            output_data = location of the stored results 
    """
    
    #Get filepath to log data file
    log_data_path = input_data + 'log_data/*.json'

    #read log data file
    log_df = spark.read.json(log_data_path)
   
    #filter by actions for song plays  
    log_df = log_df.filter(log_df.page == 'NextSong')

    #creat log view to write sql queries 
    log_df.createOrReplaceTempview("logs_table")

    #extract columns to create users table
    users_schema_table = spark.sql(""" 
                            SELECT distinct u.userID as u.user_id,
                                u.first_name,
                                u.last_name,
                                u.gender,
                                u.level
                            FROM logs_table u
                            WHERE u.userId IS NOT NULL
                            """)
    
    # write users tables to parquet files
    users_schema_table.write.mode('overwrite').parquet(output_data + 'user_schema_table/')

    #Extract columns to create time table 
    time_schema_table = spark.sql("""
                        SELECT  subquery.starttime_sub as start_time, 
                            hour(subquery.starttime_sub) as hour,
                            dayofmonth(subquery.starttime_sub) as day,
                            weekofyear(subquery.starttime_sub) as week,
                            month(subquery.starttime_sub) as month,
                            year(subquery.starttime_sub) as year,
                            dayofweek(subquery.starttime_sub) as weekday,
                        FROM (SELECT totimestamp(timestamp.ts/1000) 
                            as starttime_sub FROM logs_table timestamp
                            WHERE timestamp.ts IS NOT NULL) subquery """)

    #Write time table to parquet file partitinoned by year and month 
    time_schema_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + 'time_schema_table/')

    #Extract columns from joined song and log datasets to create songsplay table 
    songsplay_schema_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                    totimestamp(u.ts/1000) as start_time,
                                    month(to_timestamp(u.ts/1000)) as month,
                                    year(to_timestamp(u.ts/1000)) as year,
                                    u.userId as user_id, 
                                    u.level as level,
                                    s.song_id as song_id,
                                    s.artist_id as artist_id,
                                    u.sessionId as session_id,
                                    u.location as location,
                                    u.userAgent as user_agent
                                FROM logs_table u 
                                JOIN songs_table s on u.artist = s.artist_name and u.song = s.title
                                """ )

    #write songplays table to parquet files partitioned by year and month 
    songsplay_schema_table.write.mode('overwrite').partitionBy('year', 'month'). parquet(output_data + 'songsplay_schema_table/')


def main():

    spark = create_spark_session()

    input_data = config.get('IO', 'INPUT_DATA')
    output_data = config.get('IO', 'OUTPUT_DATA')

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main 
        

    



