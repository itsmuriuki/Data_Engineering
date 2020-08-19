import os
import glob
import psycopg2
import pandas as import pd 
from sql_queries import *

#Geting the data files
#process songs
#process log file

def process_song_file(cur, filepath):

    #open song file 
    df = pd.read_json(filepath, typ='series')

    #insert song record
    songs_data = df[['song_id','title','artist_id', 'year', 'duration']] 

    # check for song_id duplicates
    cur.execute(song_select, (song_data[['song_id']]))
    results = cur.fetchone()

    if result[0]



