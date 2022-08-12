import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwg.cfg')

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
    songplay_id     INTEGER     PRIMARY KEY,
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
iam_role {}
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
) VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
);
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
) VALUES (
    %s,
    %s,
    %s,
    %s,
    %s
) ON CONFLICT (user_id) DO UPDATE SET (
    first_name,
    last_name,
    gender,
    level
) = (
    EXCLUDED.first_name,
    EXCLUDED.last_name,
    EXCLUDED.gender,
    EXCLUDED.level
);
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
) VALUES (
    %s,
    %s,
    %s,
    %s,
    %s
) ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
) VALUES (
    %s,
    %s,
    %s,
    %s,
    %s
) ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
) VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
) ON CONFLICT (start_time) DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]

analytical_queries = []
