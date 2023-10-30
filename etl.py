import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    ''' Read JSON files in data/song_data into dataframe. Extract songID, title, artistID, year, and duration for songs table. 
        Extract artistID, artist name, artist location, latitude, and longitude for artists table.'''
    # open song file
    df = pd.read_json(filepath, lines=True, dtype={'year': int})

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    ''' Read JSON files in data/log_data into dataframe.
        Filter dataframe by NextSong in page column.
        Convert timestamp column to datetime with unit ms
        Extract timestamp, hour, day, week, month, year, and weekday from ts column
            Put into list time_data
        Make labels for columns and set to column_labels
        Create time_df dataframe using time_data and column_labels
        Load data into times table
        Extract userID, firstName, lastName, gender, and level into user_df
        Insert records into users table
        Extract timestamp, userID, level, songID, artistID, sessionID, location, and userAgent into songplay_data
        Insert records into songplays'''
    
    # open log file
    df = pd.read_json(filepath, lines=True, dtype={'year': int})

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = ( t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location,           row.userAgent)
    
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    ''' Get all files matching extension from directory.
        Get total number of files found
        Iterate over files and process'''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    ''' Connect to database and close when done'''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()