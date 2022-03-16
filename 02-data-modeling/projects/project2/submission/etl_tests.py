"""
File: etl_tests.py
Attribution: Student

Purpose:
The ETL Pipeline code provided by Udacity leverages print statements to ensure
that the csv data file has been created successfully. The student prefers to
create test cases to ensure consistent results, as well as to demonstrate
a functional environment, as the student has elected to split various aspects
of the project into their own files. 
"""

import unittest
import os
import hashlib


class ETLPipelineTests(unittest.TestCase):
    def setUp(self):
        # Get the current directory
        this_dir = os.path.dirname(
            os.path.abspath(__file__)
        )

        self.data_dir = os.path.join(this_dir, 'event_data')
        self.image_dir = os.path.join(this_dir, 'images')

        self.expected_csv_fp = os.path.join(
            this_dir,
            'event_datafile_new.csv'
        )

        self.etl_pipeline_script = os.path.join(
            this_dir,
            'etl_pipeline.py'
        )

        if os.path.exists(self.expected_csv_fp):
            os.remove(self.expected_csv_fp)

    def tearDown(self):
        """ Removes the Datafile Created by Testing the ETL Pipeline """
        if os.path.exists(self.expected_csv_fp):
            os.remove(self.expected_csv_fp)

    def test_project_structure(self):
        """ Ensure the Event Data and Images Exist """
        # Make sure we have the directories
        self.assertTrue(os.path.exists(self.data_dir))
        self.assertTrue(os.path.exists(self.image_dir))

        # Make sure we got the sample image referenced in the notebook file
        self.assertTrue(os.path.exists(
            os.path.join(self.image_dir, 'image_event_datafile_new.jpg')
        ))

        # Ensure the correct number of event csv files exist in the data
        # directory (30) - one for each day in November
        self.assertEqual(len(os.listdir(self.data_dir)), 30)

    def test_etl_pipeline(self):
        """ Ensure that the ETL Pipeline Creates the CSV File Correctly """
        self.assertFalse(os.path.exists(self.expected_csv_fp))
        with open(self.etl_pipeline_script, 'r') as etl_pipeline:
            exec(etl_pipeline.read())
        etl_pipeline.close()
        self.assertTrue(os.path.exists(self.expected_csv_fp))

        # I computed the MD5 hash of the file when I observed
        # it's creation; I'm now using this as a checksum to test that the file
        # creates correctly consistently. The print statements seem to indicate
        # That there are 6821 lines, so this will test that as well
        checksum = '63a64b6f44e717281fdf413feb16d635'
        num_lines = 6821

        chunk_size = 65536

        algo = hashlib.md5()
        line_count = 0

        # A slightly unorthodox method for opening files, this will not
        # run into any memory issues if the file were extremely large.
        # We could simplify the code slightly, but assuming we were working
        # with production data sets, reading the file in chunks might be the
        # best way to avoid crashing the system
        csv_file = open(self.expected_csv_fp, 'rb')
        chunk = csv_file.read(chunk_size)
        while chunk:
            line_count += chunk.count(b'\n')
            algo.update(chunk)
            chunk = csv_file.read(chunk_size)
        csv_file.close()
        self.assertEqual(algo.hexdigest().lower(), checksum)
        self.assertEqual(line_count, num_lines)


if __name__ == '__main__':
    unittest.main()
