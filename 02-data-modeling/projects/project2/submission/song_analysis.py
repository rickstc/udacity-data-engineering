"""
File: song_analysis.py
Attribution: Mixed

Purpose:
This file expands on the boilerplate code provided by udacity in the second
section of the code notebook. That section includes some basic code and prompts,
intended to be completed by the student to accomplish the fictional organization's
data analysis goals.
"""

from cassandra.cluster import Cluster
import csv
import os


class UdacityUtils:
    """
    The student abstracted all of the Udacity-provided code to this class
    """
    @staticmethod
    def read_datafile():
        """ The student converted the CSV reading portion of Udacity's code to a generator """
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


"""
****************************************** Functions ******************************************
The student elected to abstract some logic into functions to achieve the following benefits:
- Consolidating reusable logic allows for error handling with fewer try/except blocks, which can
clutter code
- Abstracting some code to functions and creating a main method should make the code more readable
- Each function could become independently testable
"""


def create_keyspace(session, keyspace_name=None):
    """
    Function: create_keyspace
    Params:
    - session - A cassandra Cluster's Session object
    - keyspace_name - A name for the keyspace; if not provided defaults to 'udacity'

    Returns: keyspace_name if created successfully, otherwise False
    """
    if keyspace_name is None:
        keyspace_name = "udacity"
    if keyspace_name.startswith('system'):
        print("Your chosen keyspace name should not begin with system*")
        return False
    try:
        rep = {'class': 'SimpleStrategy', 'replication_factor': 1}
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH REPLICATION = {rep}")
        return keyspace_name
    except Exception as e:
        print(e)
        return False


def execute_query(session, query, params=None):
    """ 
    Function: execute_query
    Params:
    - session - A cassandra Cluster's Session object
    - query - The query to execute
    - values (optional) - Any values that need to be passed in to the query

    Returns: The response from session.execute if the query was successful, otherwise None

    Purpose:
    Wraps session.execute into a try/except block, allowing for the reuse of exception handling
    """
    try:
        if params is not None:
            return session.execute(query, params)
        return session.execute(query)
    except Exception as ex:
        print(ex)
        return None


def create_table(session, table_name, primary_key):
    """
    Params:
    - session - The Cassandra Cluster.session object
    - primary_key - The primary key that will be used with the table


    This function consolidates the logic to create a table, as the column names and types
    do not change for the data; only the primary key changes for each query
    """

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} 
    (
        session_id int,
        item_in_session int,
        user_id int,
        level text,
        first_name text,
        last_name text,
        gender text,
        location text,
        artist_name text,
        song_name text,
        length float,
        PRIMARY KEY {primary_key}
    )
    """
    execute_query(session, create_table_query)


def query_one(session):
    """
    Params:
    - session - The Cassandra Cluster.session object

    TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
    sessionId = 338, and itemInSession = 4

    The objective of this is to locate the artist, title and length provided a session id and the position in a session

    Therefore, the primary key should contain a partition key of session_id, and a clustering key of item_in_session
    """
    # Prepare our primary key and create the table
    primary_key = "((session_id), item_in_session)"
    create_table(session, 'session_songs', primary_key)

    # Populate the table
    for line in UdacityUtils.read_datafile():
        query = """
        INSERT INTO session_songs (
            session_id,
            item_in_session,
            user_id,
            level,
            first_name,
            last_name,
            gender,
            location,
            artist_name,
            song_name,
            length
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            int(line[8]),
            int(line[3]),
            int(line[10]),
            line[6],
            line[1],
            line[4],
            line[2],
            line[7],
            line[0],
            line[9],
            float(line[5])
        )
        execute_query(session, query, params)

    # Execute the query
    rows = execute_query(session, """
        SELECT artist_name, song_name, length
        FROM session_songs
        WHERE session_id=%s AND item_in_session=%s
    """, (338, 4))

    # Drop table to prepare for next query
    execute_query(session, "DROP TABLE IF EXISTS session_songs")
    return rows


def query_two(session):
    """
    Params:
    - session - The Cassandra Cluster.session object

    TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
    for userid = 10, sessionid = 182

    The objective of this is to locate the artist, song, and the user's first and last name given a user id and session id
    These results are to be sorted by the item_in_session

    Therefore, the primary key should be defined as a composite key with a partion key of session_id and clustering keys of
    user_id and item_in_session

    """
    primary_key = "((session_id), user_id, item_in_session)"
    create_table(session, 'user_sessions', primary_key)

    # Populate the table
    for line in UdacityUtils.read_datafile():
        query = """
        INSERT INTO user_sessions (
            session_id,
            item_in_session,
            user_id,
            level,
            first_name,
            last_name,
            gender,
            location,
            artist_name,
            song_name,
            length
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            int(line[8]),
            int(line[3]),
            int(line[10]),
            line[6],
            line[1],
            line[4],
            line[2],
            line[7],
            line[0],
            line[9],
            float(line[5])
        )
        execute_query(session, query, params)

    # Execute the query
    rows = execute_query(session, """
        SELECT artist_name, song_name, first_name, last_name
        FROM user_sessions
        WHERE session_id=%s AND user_id=%s
        ORDER BY item_in_session
    """, (182, 10))

    # Drop table to prepare for next query
    execute_query(session, "DROP TABLE IF EXISTS user_sessions")
    return rows


def query_three(session):
    """
    Params:
    - session - The Cassandra Cluster.session object

    # TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

    The objective of this is to locate the first and last name of users who listened a given song

    Therefore, the primary key should be a composite containing a partiion key of song_name, with clustering columns
    of first_name and last_name

    While the problem statement did not dictate any ordering, the student chose to order results by last name
    """
    primary_key = "((song_name), last_name, first_name)"
    create_table(session, 'song_plays', primary_key)

    # Populate the table
    for line in UdacityUtils.read_datafile():
        query = """
        INSERT INTO song_plays (
            session_id,
            item_in_session,
            user_id,
            level,
            first_name,
            last_name,
            gender,
            location,
            artist_name,
            song_name,
            length
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            int(line[8]),
            int(line[3]),
            int(line[10]),
            line[6],
            line[1],
            line[4],
            line[2],
            line[7],
            line[0],
            line[9],
            float(line[5])
        )
        execute_query(session, query, params)

    # Execute the query
    rows = execute_query(session, """
        SELECT last_name, first_name
        FROM song_plays
        WHERE song_name=%s
        ORDER BY last_name
    """, ("All Hands Against His Own", ))

    # Drop table to prepare for next query
    execute_query(session, "DROP TABLE IF EXISTS song_plays")
    return rows


def init():
    """
    The execution of the code required was abstracted into it's own function
    so that it could be called when the file is run directly from the terminal
    """
    # Connect to a session in the cluster
    cluster, session = UdacityUtils.open_db_connection()

    # TO-DO: Create a Keyspace
    keyspace = create_keyspace(session)

    # TO-DO: Set KEYSPACE to the keyspace specified above
    session.set_keyspace(keyspace)

    """
    At this point, the project requires the student to perform the following tasks:
    1. Create a table that will be used to store the data from the CSV file
    2. Read the data in from the CSV file and populate the table
    3. Write a query that will provide a certain piece of information
    4. Drop the table after the data has been retrieved

    The student believes that each query might be better abstracted into their its' own
    function for ease of testing and to improve readability
    """

    rows = query_one(session)
    for row in rows:
        print(row)

    rows = query_two(session)
    for row in rows:
        print(row)

    rows = query_three(session)
    for row in rows:
        print(row)

    # Close the session and shutdown the cluster
    UdacityUtils.close_db_connection(cluster, session)


if __name__ == '__main__':
    init()
