import cassandra
from cassandra.cluster import Cluster
from utils import CassUtils
import unittest

SONG_DATA = [
    {"artist_name": "The Beatles", "album_name": "Let it Be", "year": 1970},
    {"artist_name": "The Beatles", "album_name": "Rubber Soul", "year": 1965},
    {"artist_name": "The Who", "album_name": "My Generation", "year": 1965},
    {"artist_name": "The Monkees", "album_name": "The Monkees", "year": 1966},
    {"artist_name": "The Carpenters", "album_name": "Close to You", "year": 1970},
]


def execute_query(session, query):
    """
    Accepts a query and executes it
    Basically reuses a try/except block to cut down on redundant code
    """
    try:
        return session.execute(query)
    except Exception as e:
        print(e)


def execute_query_with_params(session, query, params):
    """
    Executes a query given a set of params
    """
    try:
        return session.execute(query, params)
    except Exception as e:
        print(e)


def insert_music_library(session, year, artist_name, album_name):
    """
    Inserts data into the music library table
    """
    query = "INSERT INTO music_library (year, artist_name, album_name)"
    query = query + " VALUES (%s, %s, %s)"
    execute_query_with_params(query, (year, artist_name, album_name))


def insert_artist_library(session, artist_name, year, album_name):
    """
    Inserts data into the music library table
    """
    query = "INSERT INTO artist_library (artist_name, year, album_name)"
    query = query + " VALUES (%s, %s, %s)"
    execute_query_with_params(query, (artist_name, year, album_name))


def insert_album_library(session, album_name, artist_name, year):
    """
    Inserts data into the music library table
    """
    query = "INSERT INTO artist_library (artist_name, year, album_name)"
    query = query + " VALUES (%s, %s, %s)"
    execute_query_with_params(query, (album_name, artist_name, year))


def create_tables(session):
    """ Creates the tables for the exercise """
    music_lib_create = "CREATE TABLE IF NOT EXISTS music_library "
    music_lib_create = music_lib_create + \
        "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"

    artist_lib_create = "CREATE TABLE IF NOT EXISTS artist_library "
    artist_lib_create = artist_lib_create + \
        "(year int, artist_name text, album_name text, PRIMARY KEY (artist_name, year))"

    album_lib_create = "CREATE TABLE IF NOT EXISTS album_library "
    album_lib_create = album_lib_create + \
        "(year int, artist_name text, album_name text, PRIMARY KEY (album_name, year))"

    execute_query(session, music_lib_create)
    execute_query(session, artist_lib_create)
    execute_query(session, album_lib_create)


def populate_db(session):
    """
    Puts the data from SONG_DATA into each of the three DB tables
    """
    for sd in SONG_DATA:
        insert_music_library(session, sd.get('year'), sd.get(
            'artist_name'), sd.get('album_name'))
        insert_artist_library(session, sd.get('artist_name'), sd.get(
            'year'), sd.get('album_name'))
        insert_album_library(session, sd.get('album_name'), sd.get(
            'artist_name'), sd.get('year'))


def drop_tables(session):
    """ Drops the Tables - Unnecessary in my setup, but part of the Exercise"""
    execute_query(session, "DROP TABLE music_library")
    execute_query(session, "DROP TABLE album_library")
    execute_query(session, "DROP TABLE artist_library")


class CassandraTests(unittest.TestCase):
    """ Test Cases instead of Print Statements """

    def setUp(self):
        """ Runs before each test """
        CassUtils.reset_cluster()
        self.session = CassUtils.connect()
        CassUtils.create_keyspace(self.session, 'udacity')
        self.session.set_keyspace('udacity')
        create_tables(self.session)

    def tearDown(self):
        drop_tables(self.session)
        self.session.shutdown()
        CassUtils.reset_cluster()

    def test_music_library(self):
        rows = execute_query(
            self.session, "SELECT * FROM music_library WHERE year=1970")
        sample_data = {"The Beatles": "Let It Be",
                       "The Carpenters": "Close to You"}
        for row in rows:
            self.assertEqual(row.year, 1970)
            self.assertTrue(row.artist_name in sample_data.keys())
            self.assertEqual(row.album_name, sample_data[row.artist_name])

    def test_artist_library(self):
        rows = execute_query(self.session,
                             "SELECT * FROM artist_library WHERE artist_name='The Beatles'")
        sample_data = {"Rubber Soul": 1965,
                       "Let it Be": 1970}
        for row in rows:
            self.assertEqual(row.artist_name, "The Beatles")
            self.assertTrue(row.album_name in sample_data.keys())
            self.assertEqual(row.year, sample_data[row.album_name])

    def test_album_library(self):
        rows = execute_query(self.session,
                             "SELECT * FROM album_library WHERE album_name='Close To You'")
        sample_data = {"The Carpenters": "Close to You"}
        for row in rows:
            self.assertEqual(row.year, 1970)
            self.assertTrue(row.artist_name in sample_data.keys())
            self.assertEqual(row.album_name, sample_data[row.artist_name])


if __name__ == '__main__':
    unittest.main()
