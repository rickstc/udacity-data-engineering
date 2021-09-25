import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import uuid


def process_song_file(cur, filepath):
    """
    Processes a song file, inserting records into the song and artist tables.

    Arguments:
    - cur - Postgres database cursor
    - filepath - Filepath of the song file to process

    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # Handle NaN lat/lngs
    df['artist_latitude'] = df['artist_latitude'].fillna(0).astype(int)
    df['artist_longitude'] = df['artist_longitude'].fillna(0).astype(int)

    for index, row in df.iterrows():
        # insert song record
        song_data = (row.song_id, row.title,
                     row.artist_id, row.year, row.duration)
        try:
            cur.execute(song_table_insert, song_data)
        except Exception as e:
            print("There was an error writing to the song table.")
            print("The query was: ")
            print(f"{song_table_insert}, {song_data}")

        # insert artist record
        artist_data = (row.artist_id, row.artist_name, row.artist_location,
                       row.artist_latitude, row.artist_longitude)
        try:
            cur.execute(artist_table_insert, artist_data)
        except Exception as e:
            print("There was an error writing to the artist table.")
            print("The query was: ")
            print(f"{song_table_insert}, {artist_data}")


def process_log_file(cur, filepath):
    """
    Processes an event file, inserting records into the user, time, and songplay tables.

    Arguments:
    - cur - Postgres database cursor
    - filepath - Filepath of the song file to process

    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[(df.page == 'NextSong')]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df.copy()

    # insert time data records
    time_data = (t.ts, t.ts.dt.hour, t.ts.dt.day, t.ts.dt.dayofweek,
                 t.ts.dt.month, t.ts.dt.year, t.ts.dt.weekday)
    column_labels = ['start_time', 'hour', 'day',
                     'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            str(uuid.uuid4()),
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        )

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
