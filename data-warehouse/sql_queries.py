import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                artist TEXT,
                                auth TEXT,
                                first_name TEXT,
                                gender TEXT,
                                item_in_session INTEGER,
                                last_name  TEXT,
                                length NUMERIC,
                                level TEXT,
                                location TEXT,
                                method TEXT,
                                page TEXT,
                                registration NUMERIC,
                                session_id INTEGER,
                                song TEXT,
                                status INTEGER,
                                ts BIGINT,
                                user_agent TEXT,
                                user_id INTEGER
                                );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs INTEGER,
                                artist_id TEXT,
                                artist_latitude NUMERIC,
                                artist_longitude NUMERIC,
                                artist_location TEXT,
                                artist_name TEXT,
                                song_id TEXT,
                                title TEXT,
                                duration NUMERIC,
                                year INTEGER
                                );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays ( 
                            songplay_id INTEGER identity(1,1) PRIMARY KEY, 
                            start_time timestamp NOT NULL DISTKEY, 
                            user_id int NOT NULL,
                            level TEXT NOT NULL,
                            song_id TEXT NOT NULL,
                            artist_id TEXT NOT NULL,
                            session_id INTEGER NOT NULL,
                            location TEXT NOT NULL,
                            user_agent TEXT NOT NULL
                            ); 
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users ( 
                        user_id int PRIMARY KEY DISTKEY, 
                        first_name TEXT NOT NULL,
                        last_name TEXT NOT NULL SORTKEY,
                        gender TEXT NOT NULL,
                        level TEXT NOT NULL
                        ); 
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs ( 
                        song_id TEXT PRIMARY KEY NOT NULL DISTKEY, 
                        title TEXT NOT NULL,
                        artist_id TEXT NOT NULL,
                        year int NOT NULL,
                        duration FLOAT NOT NULL
                        )
                        SORTKEY(year,duration); 
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists ( 
                            artist_id TEXT PRIMARY KEY DISTKEY,
                            name TEXT NOT NULL SORTKEY,
                            location TEXT NOT NULL, 
                            latitude FLOAT NOT NULL, 
                            longitude FLOAT NOT NULL
                            ); 
                    """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time timestamp PRIMARY KEY SORTKEY DISTKEY,
                        hour INTEGER NOT NULL,
                        day INTEGER NOT NULL,
                        week INTEGER NOT NULL,
                        month INTEGER NOT NULL,
                        year INTEGER NOT NULL,
                        weekday INTEGER NOT NULL
                        ); 
                    """)

# STAGING TABLES

staging_events_copy = ("""copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json' compupdate off region 'us-west-2';
""").format(config.get("IAM_ROLE", "ARN"))

staging_songs_copy = ("""copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    json 'auto' compupdate off region 'us-west-2';
""").format(config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

#songplay_table_insert = ("""
#""")

user_table_insert = ("""INSERT INTO users (user_id,first_name,last_name,gender,level)
                        SELECT DISTINCT user_id,first_name,last_name,gender,level FROM staging_events
                        WHERE staging_events.user_id NOT IN (SELECT DISTINCT user_id FROM users)
                        AND user_id IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs (song_id,title,artist_id,year,duration)
                        SELECT DISTINCT song_id, title, artist_id,year,duration FROM staging_songs
                        WHERE staging_songs.song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

#artist_table_insert = ("""
#""")

#time_table_insert = ("""
#""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
insert_table_queries = [song_table_insert,user_table_insert]
