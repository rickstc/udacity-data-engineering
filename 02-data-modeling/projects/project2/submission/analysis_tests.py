"""
File: analysis_tests.py
Attribution: Student

Purpose:
The project only requires the student to demonstrate successful query execution
using print statements, however the student would prefer to guarantee consistent
execution against the data set using test cases.
"""

import unittest
from song_analysis import UdacityUtils, query_one, query_two, query_three, create_keyspace, execute_query


class SongAnalysisProjectTests(unittest.TestCase):
    def setUp(self):
        """ Runs at the beginning of each test """
        self.cluster, self.session = UdacityUtils.open_db_connection()
        self.keyspace = create_keyspace(self.session)
        self.session.set_keyspace(self.keyspace)

    def tearDown(self):
        """ Runs after every test """
        UdacityUtils.close_db_connection(self.cluster, self.session)

    def test_query_one(self):
        """
        Expected Results:
        Row(artist_name='Faithless', song_name='Music Matters (Mark Knight Dub)', length=495.30731201171875)
        """
        rows = query_one(self.session)
        num_results = 0
        for row in rows:
            num_results += 1
            self.assertEqual(row.artist_name, 'Faithless')
            self.assertEqual(row.song_name,
                             'Music Matters (Mark Knight Dub)')
            self.assertEqual(row.length, 495.30731201171875)
        self.assertEqual(num_results, 1)

    def test_query_two(self):
        """
        Expected Results:
        Row(artist_name='Down To The Bone', song_name="Keep On Keepin' On", first_name='Sylvie', last_name='Cruz')
        Row(artist_name='Three Drives', song_name='Greece 2000', first_name='Sylvie', last_name='Cruz')
        Row(artist_name='Sebastien Tellier', song_name='Kilometer', first_name='Sylvie', last_name='Cruz')
        Row(artist_name='Lonnie Gordon', song_name='Catch You Baby (Steve Pitron & Max Sanna Radio Edit)', first_name='Sylvie', last_name='Cruz')
        """
        results = [{
            "artist_name": "Down To The Bone",
            "song_name": "Keep On Keepin' On",
        }, {
            "artist_name": "Three Drives",
            "song_name": "Greece 2000",
        }, {
            "artist_name": "Sebastien Tellier",
            "song_name": "Kilometer",
        }, {
            "artist_name": "Lonnie Gordon",
            "song_name": "Catch You Baby (Steve Pitron & Max Sanna Radio Edit)",
        }]
        rows = query_two(self.session)
        num_results = 0
        for row in rows:
            self.assertEqual(
                row.artist_name, results[num_results]['artist_name'])
            self.assertEqual(row.song_name, results[num_results]['song_name'])
            self.assertEqual(row.first_name, 'Sylvie')
            self.assertEqual(row.last_name, 'Cruz')
            num_results += 1
        self.assertEqual(num_results, 4)

    def test_query_three(self):
        """
        Expected Results:
        Row(last_name='Lynch', first_name='Jacqueline')
        Row(last_name='Levine', first_name='Tegan')
        Row(last_name='Johnson', first_name='Sara')
        """

        results = [{
            "last_name": "Lynch",
            "first_name": "Jacqueline"
        },
            {
            "last_name": "Levine",
            "first_name": "Tegan"
        }, {
            "last_name": "Johnson",
            "first_name": "Sara"
        }]
        rows = query_three(self.session)
        num_results = 0
        for row in rows:
            self.assertEqual(row.last_name, results[num_results]['last_name'])
            self.assertEqual(
                row.first_name, results[num_results]['first_name'])
            num_results += 1
        self.assertEqual(num_results, 3)


if __name__ == '__main__':
    unittest.main()
