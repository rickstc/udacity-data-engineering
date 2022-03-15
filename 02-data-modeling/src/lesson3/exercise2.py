import cassandra
from cassandra.cluster import Cluster
from utils import CassUtils
import unittest

SONG_DATA = [
    {"artist_name": "The Beatles", "album_name": "Let it Be",
        "year": 1970, "city": "Liverpool"},
    {"artist_name": "The Beatles", "album_name": "Rubber Soul",
        "year": 1965, "city": "Oxford"},
    {"artist_name": "The Who", "album_name": "My Generation",
        "year": 1965, "city": "London"},
    {"artist_name": "The Monkees", "album_name": "The Monkees",
        "year": 1966, "city": "Los Angeles"},
    {"artist_name": "The Carpenters", "album_name": "Close to You",
        "year": 1970, "city": "San Diego"}
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
    query = "DROP TABLE music_library"
    return execute_query(session, query)


def create_tables_single_key(session):
    """ Creates the Music Library Table with a single key on artist_name"""
    query = "CREATE TABLE IF NOT EXISTS music_library "
    query = query + \
        "(year int, city text, artist_name text, album_name text, PRIMARY KEY (artist_name))"
    return execute_query(session, query)


def load_data(session):
    """ Loads data from the SONG_DATA dictionary into the database """
    query = "INSERT INTO music_library (year, artist_name, album_name, city)"
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

    def test_music_library_single_primary(self):
        """ Test that the single primary key caused a row of data to be dropped """
        create_tables_single_key(self.session)
        load_data(self.session)
        rows = execute_query(
            self.session, "SELECT * FROM music_library WHERE artist_name='The Beatles'")

        row_count = 0

        for row in rows:
            row_count += 1
        self.assertEqual(row_count, 1)

        all_data = execute_query(self.session, 'SELECT * FROM music_library')
        row_count = 0
        for ad in all_data:
            row_count += 1

        self.assertEqual(row_count, len(SONG_DATA) - 1)


if __name__ == '__main__':
    unittest.main()
