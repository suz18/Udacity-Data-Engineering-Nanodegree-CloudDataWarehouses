import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
    (artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession numeric,
    lastName varchar,
    length numeric,
    level varchar NOT NULL,
    location varchar,
    method varchar,
    page varchar,
    registration numeric,
    sessionId numeric NOT NULL,
    song varchar,
    status numeric,
    ts TIMESTAMPTZ NOT NULL,
    userAgent varchar,
    userId numeric 
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
    (num_songs int NOT NULL,
    artist_id varchar NOT NULL,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar,
    artist_name varchar NOT NULL,
    song_id varchar NOT NULL,
    title varchar NOT NULL,
    duration numeric,
    year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
    (songplay_id int IDENTITY(0,1) NOT NULL, 
    start_time timestamp NOT NULL, 
    user_id int NOT NULL, 
    level varchar NOT NULL, 
    song_id varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    session_id int NOT NULL, 
    location varchar, 
    user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
    (user_id int NOT NULL, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar NOT NULL);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
    (song_id varchar NOT NULL, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int, 
    duration numeric);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
    (artist_id varchar NOT NULL, 
    name varchar NOT NULL, 
    location varchar, 
    latitude numeric, 
    longitude numeric);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
    (start_time timestamp NOT NULL, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data/'
    credentials 'aws_iam_role={}'
    JSON 's3://udacity-dend/log_json_path.json'
    TIMEFORMAT as 'epochmillisecs'
    ;
""").format(ARN)

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    ;
""").format(ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(SELECT 
    staging_events2.ts as start_time, 
    staging_events2.userId as user_id, 
    staging_events2.level, 
    staging_songs.song_id, 
    staging_songs.artist_id, 
    staging_events2.sessionId as session_id, 
    staging_events2.location, 
    staging_events2.userAgent as user_agent 
 FROM (select * from staging_events where page ='NextSong') as staging_events2
 JOIN staging_songs
 on staging_events2.song = staging_songs.title 
);
""")

user_table_insert = ("""
INSERT INTO users
(SELECT distinct
    userId as user_id, 
    firstName as first_name, 
    lastName as last_name, 
    gender, 
    level 
 FROM staging_events
 WHERE userId IS NOT NULL 
);
""")

song_table_insert = ("""
INSERT INTO songs
(SELECT distinct
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
 FROM staging_songs);
""")

artist_table_insert = ("""
INSERT INTO artists
(SELECT distinct
    artist_id, 
    artist_name as name, 
    artist_location as location, 
    artist_latitude as latitude, 
    artist_longitude as longitude
 FROM staging_songs);
""")

time_table_insert = ("""
INSERT INTO time
(SELECT distinct
    ts as start_time, 
    extract(h from ts) as hour, 
    extract(d from ts) as day, 
    extract(w from ts) as week, 
    extract(mon from ts) as month, 
    extract(y from ts) as year, 
    extract(dw from ts) as weekday
 FROM staging_events);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
