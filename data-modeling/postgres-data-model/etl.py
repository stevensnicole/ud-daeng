import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Arguments :
        cur      - Database Cursor
        filepath - location of JSON Files
    
    - Opens JSON song files and reads in the data
    
    - Inserts the song info into the song table
    
    - Inserts the artist data
    
    - Closes the connection. 
    
    - Return :
        none
    """
    # open song file
    df = pd.read_json(filepath, lines=True, orient='columns')

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0]
    cur.execute(song_table_insert, song_data)
     
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0]
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    - Arguments :
        cur      - Database Cursor
        filepath - location of JSON Files
    
    - Opens JSON log files and reads in the data
    
    - Transforms the ts field into a timestamp and extracts the hour, day, week, month, year, weekday info from it
    
    - Inserts the timestamp and time extracts into the time table
    
    - Inserts users into the user table
    
    - Uses the song title, artist name and song duration to lookup the already loaded artist_id and song_id from artists and songs
    
    - Adds the song_id and artist_id into the remaining log file data columns for insert into songplay
    
    - Return :
        none
    """
    
    # open log file
    #print (filepath)
    df = pd.read_json(filepath, lines=True, orient='columns')

    # filter by NextSong action
    df = df[df['page'].str.match('NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = pd.concat([t,t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday],sort=True,axis=1)
    column_labels = ['start_time','hour','day','week','month','year','weekday'] 
    time_df = pd.DataFrame(time_data.values,columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            print (results)
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = songplay_data = (pd.to_datetime(row.ts, unit='ms').to_pydatetime(), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Arguments :
        cur      - Database Cursor
        conn     - database connection
        filepath - location of JSON Files
        func     - the funciton to be called based on the filepath
    
    - walks the given filepath and returns a list of .json files on that path
    
    - Outputs the number of .json files on the path
    
    - Iterates through the .json files on the filepath passing the filename to the passed in function name
    
    - Commits the transaction after each file has been processed
       
    - Return :
        none
    """
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
    """
    - Connects to the database
    
    - Extracts, transforms and loads the song file data
    
    - Extracts, transforms and loads the log file data
    
    - Finally, closes the connection. 
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()