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
    {"artist_name": "The Who", "year": 1965,
        "city": "London", "album_name": "My Generation"}
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


def create_table(session):
    """
    The example tells us we will want to make four queries:
    - Every album released in a given year
    - Album released in a given year by a given artist
    - Album released in a given year recorded in a given city
    - City where an album was recorded

    So we know we want a composite key comprised of:
    - year: parition key
    - artist_name (clustering column)
    - album_name (clustering column)
    """
    query = "CREATE TABLE IF NOT EXISTS music_library "
    query = query + \
        "(year int, artist_name text, album_name text, city text, PRIMARY KEY ((year), artist_name, album_name))"
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
        create_table(self.session)
        load_data(self.session)

    def tearDown(self):
        drop_table(self.session)
        self.session.shutdown()
        CassUtils.reset_cluster()

    def test_query_one(self):
        """ Every album in library released in 1965"""
        rows = execute_query(
            self.session, 'SELECT * FROM music_library WHERE year=1965')

        results = 0
        for row in rows:
            results += 1

        self.assertEqual(2, results)

    def test_query_two(self):
        """ Every album released in 1965 by the beatles """
        rows = execute_query(
            self.session, "SELECT * FROM music_library WHERE year=1965 AND artist_name = 'The Beatles'")

        results = 0
        for row in rows:
            results += 1
            self.assertEqual(row.year, SONG_DATA[1]['year'])
            self.assertEqual(row.album_name, SONG_DATA[1]['album_name'])
            self.assertEqual(row.artist_name, SONG_DATA[1]['artist_name'])
            self.assertEqual(row.city, SONG_DATA[1]['city'])

        self.assertEqual(1, results)

    def test_query_three(self):
        """
        All albums released in a given year made in london
        this should warn about an error and return None, as the DB is clustered by year
        and we haven't provided it
        """
        rows = execute_query(
            self.session, "SELECT * FROM music_library WHERE city = 'London'")

        self.assertEqual(rows, None)

    def test_query_four(self):
        """
        City where album 'rubber soul' was recorded 
        this should also warn about an error and return None, as the DB is clustered by year
        and we haven't provided it
        """
        rows = execute_query(
            self.session, "SELECT city FROM music_library WHERE album_name='Rubber Soul'")

        self.assertEqual(rows, None)


if __name__ == '__main__':
    unittest.main()
