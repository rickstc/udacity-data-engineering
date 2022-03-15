import cassandra
from cassandra.cluster import Cluster
from utils import CassUtils
import unittest

SONG_DATA = [
    {"artist_name": "The Beatles", "album_name": "Let it Be",
        "year": 1970, "city": "Liverpool"},
    {"artist_name": "The Beatles", "album_name": "Rubber Soul",
        "year": 1965, "city": "Oxford"},
    {"artist_name": "The Monkees", "album_name": "The Monkees",
        "year": 1966, "city": "Los Angeles"},
    {"artist_name": "The Carpenters", "album_name": "Close to You",
        "year": 1970, "city": "San Diego"},
    {"artist_name": "The Beatles", "year": 1964,
        "city": "London", "album_name": "Beatles for Sale"}
]


def execute_query(session, query, params=None):
    """ Wrapper on session.execute so we don't have to clutter the code up with try catch blocks everywhere """
    try:
        if params is not None:
            return session.execute(query, params)
        return session.execute(query)
    except Exception as ex:
        print(ex)
        return None


def drop_table(session):
    """ Drops the table """
    query = "DROP TABLE album_library"
    return execute_query(session, query)


def create_tables_composite_key(session):
    """ Creates the Music Library Table with a single key on artist_name"""
    query = "CREATE TABLE IF NOT EXISTS album_library "
    query = query + \
        "(year int, artist_name text, album_name text, city text, PRIMARY KEY (album_name, artist_name))"
    return execute_query(session, query)


def load_data(session):
    """ Loads data from the SONG_DATA dictionary into the database """
    query = "INSERT INTO album_library (year, artist_name, album_name, city)"
    query = query + " VALUES (%s, %s, %s, %s)"
    for sd in SONG_DATA:
        execute_query(session, query, (sd.get('year'), sd.get(
            'artist_name'), sd.get('album_name'), sd.get('city')))


class CassandraTests(unittest.TestCase):
    """ Test Cases instead of Print Statements """

    def setUp(self):
        """ Runs before each test """
        CassUtils.reset_cluster()
        self.session = CassUtils.connect()
        CassUtils.create_keyspace(self.session, 'udacity')
        self.session.set_keyspace('udacity')

    def tearDown(self):
        drop_table(self.session)
        self.session.shutdown()
        CassUtils.reset_cluster()

    def test_album_library_single_primary(self):
        """ Retrieve the album_name of 'Close to You' and test the result """
        create_tables_composite_key(self.session)
        load_data(self.session)

        rows = execute_query(
            self.session, "SELECT * FROM album_library WHERE album_name='Close to You'")

        row_count = 0

        carpenters = SONG_DATA[3]

        for row in rows:
            row_count += 1
            self.assertEqual(row.year, carpenters.get('year'))
            self.assertEqual(row.album_name, carpenters.get('album_name'))
            self.assertEqual(row.city, carpenters.get('city'))
            self.assertEqual(row.artist_name, carpenters.get('artist_name'))
        self.assertEqual(row_count, 1)

        all_data = execute_query(self.session, 'SELECT * FROM album_library')
        row_count = 0
        for ad in all_data:
            row_count += 1

        self.assertEqual(row_count, len(SONG_DATA))


if __name__ == '__main__':
    unittest.main()
