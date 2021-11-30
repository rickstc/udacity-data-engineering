# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = (
    "CREATE TABLE IF NOT EXISTS songplays (songplay_id varchar primary key, start_time timestamp, user_id varchar, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar)")

user_table_create = (
    "CREATE TABLE IF NOT EXISTS users (user_id varchar primary key, first_name varchar, last_name varchar, gender varchar, level varchar)")

song_table_create = (
    "CREATE TABLE IF NOT EXISTS songs (song_id varchar primary key, title varchar UNIQUE, artist_id varchar, year int, duration decimal)")

artist_table_create = (
    "CREATE TABLE IF NOT EXISTS artists (artist_id varchar primary key, name varchar UNIQUE, location varchar, latitude int, longitude int)")

time_table_create = (
    "CREATE TABLE IF NOT EXISTS time (start_time timestamp, hour int, day int, week int, month int, year int, weekday varchar)")

# INSERT RECORDS

songplay_table_insert = (
    "INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")

user_table_insert = (
    "INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING")

song_table_insert = (
    "INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING")

artist_table_insert = (
    "INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING")


time_table_insert = (
    "INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)")

# FIND SONGS

song_select = ("SELECT s.song_id, a.artist_id \
    FROM songs s JOIN artists a ON s.artist_id = a.artist_id \
    WHERE s.title=%s AND a.name=%s AND s.duration=%s")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
