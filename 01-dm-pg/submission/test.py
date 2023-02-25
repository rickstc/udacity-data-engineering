import unittest
from create_tables import main as create_tables
from etl import main as load_data
import psycopg2


class ProjectTestCases(unittest.TestCase):
    def setUp(self):
        """
        This function cleans the resets the database and loads the data in at the beginning of each test
        to ensure that each test will execute against a fresh copy of the data
        """

        create_tables()
        load_data()

    def test_solution_dataset_size(self):
        """
        This ensures that there is only one row in the songplays table for which the columns song_id and
        artist_id are not null 
        """
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkifydb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM songplays WHERE song_id is NOT NULL and artist_id is NOT NULL LIMIT 5;")
        results = cur.fetchall()
        conn.close()
        self.assertEqual(len(results), 1)


if __name__ == '__main__':
    unittest.main()
