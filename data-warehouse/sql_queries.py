import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                artist text,
                                auth text,
                                first_name text,
                                gender text,
                                item_in_session integer,
                                last_name  text,
                                length numeric,
                                level text,
                                location text,
                                method text,
                                page text,
                                registration numeric,
                                session_id integer,
                                song text,
                                status integer,
                                ts bigint,
                                user_agent text,
                                user_id integer
                                );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs integer,
                                artist_id text,
                                artist_latitude numeric,
                                artist_longitude numeric,
                                artist_location text,
                                artist_name text,
                                song_id text,
                                title text,
                                duration numeric,
                                year integer
                                );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays ( 
                            songplay_id integer identity(1,1) primary key, 
                            start_time timestamp not null, 
                            user_id int not null,
                            level text not null,
                            song_id text not null,
                            artist_id text not null,
                            session_id integer not null,
                            location text not null,
                            user_agent text not null
                            ); 
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users ( 
                        user_id int PRIMARY KEY not null, 
                        first_name text not null,
                        last_name text not null,
                        gender text not null,
                        level text not null
                        ); 
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs ( 
                        song_id text primary key not null, 
                        title text not null,
                        artist_id text not null,
                        year int not null,
                        duration float not null,
                        ); 
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists ( 
                            artist_id text PRIMARY KEY NOT NULL,
                            name text NOT NULL,
                            location text NOT NULL, 
                            latitude float NOT NULL, 
                            longitude float NOT NULL
                            ); 
                    """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time timestamp PRIMARY KEY NOT NULL,
                        hour integer NOT NULL,
                        day integer not null,
                        week integer not null,
                        month integer not null,
                        year integer not null,
                        weekday integer not null
                        ); 
                    """)

# STAGING TABLES

staging_events_copy = ("""copy staging_events from 's3://udacity-labs/tickets/full/full.csv.gz'
    credentials 'aws_iam_role={}'
    gzip delimiter ';' compupdate off region 'us-west-2';
""".format(DWH_ROLE_ARN)

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
