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
    artist              VARCHAR         NOT NULL,
    auth                VARCHAR         NOT NULL,
    first_name          VARCHAR         NOT NULL,
    gender              VARCHAR         NOT NULL,
    item_in_session     INT             NOT NULL,
    last_name           VARCHAR         NOT NULL,
    length              NUMERIC(9, 5)   NOT NULL,
    level               VARCHAR         NOT NULL,
    location            VARCHAR         NOT NULL,
    method              VARCHAR         NOT NULL,
    page                VARCHAR         NOT NULL,
    registration        BIGINT          NOT NULL,
    session_id          INT             NOT NULL,
    song                VARCHAR         NOT NULL,
    status              INT             NOT NULL,
    ts                  TIMESTAMPTZ     NOT NULL,
    user_agent          VARCHAR         NOT NULL,
    user_id             INT             NOT NULL
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INT             NOT NULL,
    artist_id           VARCHAR         NOT NULL,
    artist_latitude     NUMERIC(9, 6),
    artist_longitude    NUMERIC(9, 6),
    artist_location     VARCHAR,
    artist_name         VARCHAR         NOT NULL,
    song_id             VARCHAR         NOT NULL,
    title               VARCHAR,
    duration            NUMERIC(8, 4)   NOT NULL,
    year                INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     INTEGER IDENTITY (1, 1)     PRIMARY KEY,
    start_time      TIMESTAMP                   NOT NULL,
    user_id         INT                         NOT NULL,
    level           VARCHAR                     NOT NULL,
    song_id         VARCHAR                     NOT NULL,
    artist_id       VARCHAR                     NOT NULL,
    session_id      INT                         NOT NULL,
    location        VARCHAR,
    user_agent      VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     INT         PRIMARY KEY,
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
    latitude        INT,
    longitude       INT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time      TIMESTAMP       PRIMARY KEY,
    hour            INT             NOT NULL,
    day             INT             NOT NULL,
    week            INT             NOT NULL,
    month           INT             NOT NULL,
    year            INT             NOT NULL,
    weekday         VARCHAR         NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = (f"""
COPY staging_events
FROM 's3://udacity-dend/log_data'
iam_role 'arn:aws:iam::702908485663:role/myRedshiftRole'
region 'us-west-2'
FORMAT AS json 's3://udacity-dend/log_json_path.json'
""")

staging_songs_copy = (f"""
COPY staging_songs
FROM 's3://udacity-dend/song_data'
iam_role 'arn:aws:iam::702908485663:role/myRedshiftRole'
region 'us-west-2'
FORMAT AS json 'auto'
""")

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
