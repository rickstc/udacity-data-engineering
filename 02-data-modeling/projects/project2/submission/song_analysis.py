"""
File: song_analysis.py
Attribution: Mixed

Purpose:
This file expands on the boilerplate code provided by udacity in the second
section of the code notebook. That section includes some basic code and prompts,
intended to be completed by the student to accomplish the fictional organization's
data analysis goals.
"""

# This should make a connection to a Cassandra instance your local machine
# (127.0.0.1)

from cassandra.cluster import Cluster
import csv
import os


class UdacityProvided:
    """
    I abstracted all of the Udacity-provided code to this class
    """
    @staticmethod
    def read_datafile():
        """ I converted the CSV reading portion of Udacity's code to a generator """
        csv_file = 'event_datafile_new.csv'

        if os.path.exists(csv_file) is False:
            raise Exception(
                "The event_datafile_new.csv file could not be found in this directory. Have you run your ETL Pipeline?")
        with open(csv_file, encoding='utf8') as f:
            csvreader = csv.reader(f)
            next(csvreader)  # skip header
            for line in csvreader:
                yield line

    @staticmethod
    def open_db_connection():
        """ Returns a cluster and a session """
        cluster = Cluster()

        # To establish connection and begin executing queries, need a session
        session = cluster.connect()

        return cluster, session

    @staticmethod
    def close_db_connection(cluster, session):
        """ Closes a given cluster and session """
        session.shutdown()
        cluster.shutdown()


# Connect to a session in the cluster
cluster, session = UdacityProvided.open_db_connection()

# TO-DO: Create a Keyspace


# TO-DO: Set KEYSPACE to the keyspace specified above


# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#


for line in UdacityProvided.read_datafile():
    print(line)
# # TO-DO: Assign the INSERT statements into the `query` variable
#     query = "<ENTER INSERT STATEMENT HERE>"
#     query = query + "<ASSIGN VALUES HERE>"
#     # TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
#     # For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
#     session.execute(query, (line[  # ], line[#]))

# TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
# sessionId = 338, and itemInSession = 4

# TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
# for userid = 10, sessionid = 182

# TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'


# TO-DO: Drop the table before closing out the sessions

# Close the session and shutdown the cluster
UdacityProvided.close_db_connection(cluster, session)
