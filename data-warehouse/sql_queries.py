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
                            start_time timestamp NOT NULL, 
                            user_id int NOT NULL,
                            level TEXT NOT NULL,
                            song_id TEXT NOT NULL DISTKEY,
                            artist_id TEXT NOT NULL,
                            session_id INTEGER NOT NULL,
                            location TEXT NOT NULL,
                            user_agent TEXT NOT NULL
                            )
                            SORTKEY(song_id,start_time); 
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users ( 
                        user_id int PRIMARY KEY SORTKEY, 
                        first_name TEXT NOT NULL,
                        last_name TEXT NOT NULL,
                        gender TEXT NOT NULL,
                        level TEXT NOT NULL
                        )
                        DISTSTYLE ALL;
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs ( 
                        song_id TEXT PRIMARY KEY NOT NULL DISTKEY SORTKEY, 
                        title TEXT NOT NULL,
                        artist_id TEXT NOT NULL,
                        year int NOT NULL,
                        duration FLOAT NOT NULL
                        ); 
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists ( 
                            artist_id TEXT PRIMARY KEY,
                            name TEXT NOT NULL,
                            location TEXT, 
                            latitude FLOAT, 
                            longitude FLOAT
                            )
                            DISTSTYLE ALL
                            SORTKEY(artist_id,name); 
                    """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time timestamp PRIMARY KEY SORTKEY,
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
    json 's3://udacity-dend/log_json_path.json' compupdate off statupdate off region 'us-west-2';
""").format(config.get("IAM_ROLE", "ARN"))

staging_songs_copy = ("""copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    json 'auto' compupdate off statupdate off region 'us-west-2';
""").format(config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
                            SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
                            staging_events.user_id,
                            staging_events.level,
                            staging_songs.song_id,
                            staging_songs.artist_id,
                            staging_events.session_id,
                            staging_events.location,
                            staging_events.user_agent
                            FROM staging_events
                            JOIN staging_songs ON (staging_events.song = staging_songs.title AND staging_events.artist = staging_songs.artist_name)
                            WHERE page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id,first_name,last_name,gender,level)
                        SELECT user_id,
                        first_name,
                        last_name,
                        gender,
                        level
                        FROM
                            (SELECT staging_events.user_id,
                            staging_events.first_name,
                            staging_events.last_name,
                            staging_events.gender,
                            staging_events.level,
                            ROW_NUMBER() OVER (PARTITION BY staging_events.user_id ORDER BY ts desc) AS user_id_order
                            FROM staging_events
                            LEFT JOIN users ON users.user_id = staging_events.user_id
                            WHERE users.user_id IS NULL
                            AND staging_events.user_id IS NOT NULL)
                        WHERE user_id_order = 1
""")

song_table_insert = ("""INSERT INTO songs (song_id,title,artist_id,year,duration)
                        SELECT DISTINCT staging_songs.song_id,
                        staging_songs.title,
                        staging_songs.artist_id,
                        staging_songs.year,
                        staging_songs.duration
                        FROM staging_songs
                        LEFT JOIN songs ON songs.song_id = staging_songs.song_id
                        WHERE songs.song_id IS NULL
""")

artist_table_insert = ("""INSERT INTO artists (artist_id,name,location,latitude,longitude)
                            SELECT DISTINCT staging_songs.artist_id,
                            staging_songs.artist_name,
                            staging_songs.artist_location,
                            staging_songs.artist_latitude,
                            staging_songs.artist_longitude
                            FROM staging_songs
                            LEFT JOIN artists ON artists.artist_id = staging_songs.artist_id
                            WHERE artists.artist_id IS NULL
""")

time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday)
                        SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
                        EXTRACT(HOUR FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS hour,
                        EXTRACT(DAY FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS day,
                        EXTRACT(WEEK FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS week,
                        EXTRACT(MONTH FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS month,
                        EXTRACT(YEAR FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS year,
                        EXTRACT(DAY FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS weekday
                        FROM staging_events LEFT JOIN time on time.start_time = (timestamp 'epoch' + staging_events.ts/1000 * interval '1 second')
                        WHERE time.start_time IS NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]




