import configparser
from datetime import datetime 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, Col
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

def proccess_song_data(spark, input_data, output_data):
    """
    this functions loads song_data from s3, processes songs and artists tables loads them back to s3
        Parameters:
            spark       = Spark Session
            input_data  = location of song_data where the file is loaded to process
            output_data = location of the results stored
    """
    #get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    #read song data file 
    song_df = spark.read.json(song_data)

    

   

