# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS user_song_play"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS song_duration"

# CREATE TABLES

songplay_table_create = ("CREATE TABLE IF NOT EXISTS user_song_play ()")

user_table_create = ("CREATE TABLE IF NOT EXISTS user ()")

song_table_create = ("CREATE TABLE IF NOT EXISTS song ()")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artist ()")

time_table_create = ("CREATE TABLE IF NOT EXISTS song_duration ()")

# INSERT RECORDS

songplay_table_insert = ("INSERT INTO user_song_play ()")

user_table_insert = ("INSERT INTO user ()")

song_table_insert = ("INSERT INTO song ()")

artist_table_insert = ("INSERT INTO artist ()")


time_table_insert = ("INSERT INTO song_duration ()")

# FIND SONGS

song_select = ("")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
