"""
File: sql_queries.py
Attribution: Mixed
Background:
The boilerplate for this file was provided by Udacity for the student
to complete. The purpose of this file is to define the queries necessary
to load staging tables from log files and then analytics tables from the staging
tables. The student also defined some basic analytical queries. Each query is defined and then
queries are grouped into lists to be executed in the etl.py file.
"""
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

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist              VARCHAR,
    auth                VARCHAR,
    firstName           VARCHAR,
    gender              VARCHAR,
    itemInSession       INTEGER,
    lastName            VARCHAR,
    length              FLOAT  ,
    level               VARCHAR,
    location            VARCHAR,
    method              VARCHAR,
    page                VARCHAR,
    registration        FLOAT  ,
    sessionId           INTEGER,
    song                VARCHAR,
    status              INTEGER,
    ts                  BIGINT ,
    userAgent           VARCHAR,
    userId              INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year FLOAT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     INTEGER     IDENTITY(0,1) SORTKEY,
    start_time      TIMESTAMP   NOT NULL,
    user_id         INTEGER     NOT NULL,
    level           VARCHAR     NOT NULL,
    song_id         VARCHAR     NOT NULL,
    artist_id       VARCHAR     NOT NULL,
    session_id      INTEGER     NOT NULL,
    location        VARCHAR,
    user_agent      VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER     PRIMARY KEY,
    first_name  VARCHAR     NOT NULL,
    last_name   VARCHAR     NOT NULL,
    gender      VARCHAR,
    level       VARCHAR     NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR     PRIMARY KEY,
    title       VARCHAR     UNIQUE,
    artist_id   VARCHAR     NOT NULL,
    year        INT,
    duration    DECIMAL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id       VARCHAR     PRIMARY KEY,
    name            VARCHAR     UNIQUE,
    location        VARCHAR,
    latitude        INTEGER,
    longitude       INTEGER
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time      TIMESTAMP       PRIMARY KEY,
    hour            INTEGER,
    day             INTEGER,
    week            INTEGER,
    month           INTEGER,
    year            INTEGER,
    weekday         VARCHAR
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
region 'us-west-2'
iam_role {}
compupdate off statupdate off
timeformat as 'epochmillisecs'
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
region 'us-west-2'
iam_role {}
compupdate off statupdate off
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
FROM staging_songs ss
INNER JOIN staging_events se
ON (ss.title = se.song AND se.artist = ss.artist_name)
AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT
    DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
insert into time
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events;
""")

# Analytical Queries - Provided by Student
top_artists_by_playcount = ("""
    SELECT a.name, COUNT(songplay_id) AS plays
    FROM artists a
    JOIN songplays sp
    ON a.artist_id = sp.artist_id
    GROUP BY a.name
    ORDER BY plays DESC
    LIMIT 10
""")

top_free_songs = ("""
    SELECT s.title, COUNT(songplay_id) AS plays
    FROM songplays sp
    JOIN songs s
    ON sp.song_id = s.song_id
    WHERE level='free'
    GROUP BY s.title
    ORDER BY plays DESC
    LIMIT 10
""")

songplays_by_gender = ("""
    SELECT u.gender, COUNT(songplay_id) as plays
    FROM songplays sp
    JOIN users u
    ON u.user_id = sp.user_id
    GROUP BY u.gender
""")

songplays_by_weekday = ("""
    SELECT t.weekday, COUNT(songplay_id) as plays
    FROM songplays sp
    JOIN time t
    ON t.start_time = sp.start_time
    GROUP BY t.weekday
    ORDER BY plays DESC
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]

analytical_queries = [top_artists_by_playcount,
                      top_free_songs, songplays_by_gender, songplays_by_weekday]
