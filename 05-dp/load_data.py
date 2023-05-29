import psycopg2
import os
import traceback
import json
import datetime

"""
This helper file allowed the student to load data into a local postgres instance (because the COPY FROM S3
syntax is unique to redshift, and the student wanted to first test the airflow pipeline locally).
"""


def pg_connect():
    """ Connect to the Postgres Database """
    host = os.environ.get('POSTGRES_HOST', '127.0.0.1')
    database = os.environ.get('POSTGRES_DB', 'studentdb')
    user = os.environ.get('POSTGRES_USER', 'student')
    password = os.environ.get('POSTGRES_PASSWORD', 'student')

    try:
        connection = psycopg2.connect(
            dbname=database,
            host=host,
            user=user,
            password=password,
        )
        connection.set_session(autocommit=True)

    except Exception as ex:
        print("Exception ocurred getting a connection to the database")
        print(traceback.format_exc())
        return None

    return connection, connection.cursor()


def get_data_files(directory):
    """ Returns a set containing the file paths of any json files found in the directory, recursively """
    file_paths = set()
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith('.json'):
                file_paths.add(os.path.join(dirpath, filename))
    return file_paths


def parse_data_files(data_files):
    """
    Reads each data file in the set of data files and returns a list of json 'entries'
    based on the lines contained in the files

    Params:
        - data_files - Iterable containing file paths to json files

    Returns:
        - list of dictionaries representing the parsed entries
    """
    entries = []
    for log_file in data_files:
        with open(log_file, 'r') as lf:
            for line in lf.readlines():
                entries.append(json.loads(line))
    return entries


def load_log_entries(cursor, log_entries):
    for entry in log_entries:
        # Skip some entries we know are going to fail to save on
        # cursor execution attempts
        if entry.get('artist', None) == None:
            continue

        try:
            cursor.execute(
                """
                INSERT INTO staging_events (
                    artist,
                    auth,
                    firstname,
                    gender,
                    iteminsession,
                    lastname,
                    length,
                    level,
                    location,
                    method,
                    page,
                    registration,
                    sessionid,
                    song,
                    status,
                    ts,
                    useragent,
                    userid
                ) VALUES (
                    %(artist)s,
                    %(auth)s,
                    %(firstName)s,
                    %(gender)s,
                    %(itemInSession)s,
                    %(lastName)s,
                    %(length)s,
                    %(level)s,
                    %(location)s,
                    %(method)s,
                    %(page)s,
                    %(registration)s,
                    %(sessionId)s,
                    %(song)s,
                    %(status)s,
                    %(ts)s,
                    %(userAgent)s,
                    %(userId)s
                )
                """, entry
            )
        except:
            # Continuing here because there is a lot of bad data
            continue

    cursor.execute("SELECT COUNT(*) FROM staging_events")
    num_events = cursor.fetchall()[0][0]
    print(f"Number of staging events: {num_events}")
    assert num_events > 0


def load_song_entries(cursor, song_entries):
    for entry in song_entries:
        # Skip some entries we know are going to fail to save on
        # cursor execution attempts
        if entry.get('artist_id', None) == None:
            continue
        try:
            cursor.execute(
                """
                INSERT INTO staging_songs (
                    num_songs,
                    artist_id,
                    artist_name,
                    artist_latitude,
                    artist_longitude,
                    artist_location,
                    song_id,
                    title,
                    duration,
                    year
                ) VALUES (
                    %(num_songs)s,
                    %(artist_id)s,
                    %(artist_name)s,
                    %(artist_latitude)s,
                    %(artist_longitude)s,
                    %(artist_location)s,
                    %(song_id)s,
                    %(title)s,
                    %(duration)s,
                    %(year)s
                )
                """, entry
            )
        except:
            # Continuing here because there is a lot of bad data
            continue

    cursor.execute("SELECT COUNT(*) FROM staging_songs")
    num_songs = cursor.fetchall()[0][0]
    print(f"Number of staging_songs: {num_songs}")
    assert num_songs > 0


def init_db():
    """ Populates the Redshift database with the Log and Song Datasets """
    connection, cursor = pg_connect()

    cursor.execute("DELETE FROM staging_events;")
    cursor.execute("DELETE FROM staging_songs;")

    # Import Log Data
    log_files = get_data_files('./udacity_dend/log_data')
    log_entries = parse_data_files(log_files)
    load_log_entries(cursor, log_entries)

    # Import Song Data
    song_files = get_data_files('./udacity_dend/song_data')
    song_entries = parse_data_files(song_files)
    load_song_entries(cursor, song_entries)

    cursor.close()
    connection.close()


if __name__ == '__main__':
    init_db()
