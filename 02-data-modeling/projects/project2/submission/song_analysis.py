"""
File: song_analysis.py
Attribution: Mixed

Purpose:
This file expands on the boilerplate code provided by Udacity in the second
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

CSV_POSITIONS = {
    'session_id': 8,
    'item_in_session': 3,
    'user_id': 10,
    'level': 6,
    'first_name': 1,
    'last_name': 4,
    'gender': 2,
    'location': 7,
    'artist_name': 0,
    'song_name': 9,
    'length': 5
}


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


def query_one(session):
    """
    Params:
    - session - The Cassandra Cluster.session object

    TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during
    sessionId = 338, and itemInSession = 4

    The objective of this is to locate the artist, title and length provided a session id and the position in a session

    Therefore, the primary key should contain a partition key of session_id, and a clustering key of item_in_session
    """

    # Variables for the desired lookups
    session_id = 338
    item_in_session = 4

    # Prepare our primary key and create the table
    table_name = 'session_songs'
    primary_key = "((session_id), item_in_session)"

    # Create the table
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} 
    (
        session_id int,
        item_in_session int,
        artist_name text,
        song_name text,
        length float,
        PRIMARY KEY {primary_key}
    )
    """
    execute_query(session, create_table_query)

    # Populate the table
    for line in UdacityUtils.read_datafile():
        query = f"INSERT INTO {table_name} "

        query = query + """
        (
            session_id,
            item_in_session,
            artist_name,
            song_name,
            length
        ) VALUES (%s, %s, %s, %s, %s)
        """
        params = (
            int(line[CSV_POSITIONS['session_id']]),
            int(line[CSV_POSITIONS['item_in_session']]),
            line[CSV_POSITIONS['artist_name']],
            line[CSV_POSITIONS['song_name']],
            float(line[CSV_POSITIONS['length']])
        )
        execute_query(session, query, params)

    # Execute the query
    rows = execute_query(
        session,
        f"""
            SELECT artist_name, song_name, length
            FROM {table_name}
            WHERE session_id={session_id} AND item_in_session={item_in_session}
        """
    )

    # Drop table to prepare for next query
    execute_query(session, f"DROP TABLE IF EXISTS {table_name}")
    return rows


def query_two(session):
    """
    Params:
    - session - The Cassandra Cluster.session object

    TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
    for userid = 10, sessionid = 182

    The objective of this is to locate the artist, song, and the user's first and last name given a user id and session id
    These results are to be sorted by the item_in_session

    Therefore, the primary key should be defined as a composite partion key of session_id and user_id and a 
    clustering key of item_in_session

    """
    # Variables for the desired lookups
    user_id = 10
    session_id = 182

    table_name = 'user_sessions'
    primary_key = "((session_id, user_id), item_in_session)"
    # Create the table
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} 
    (
        session_id int,
        user_id int,
        item_in_session int,
        artist_name text,
        song_name text,
        first_name text,
        last_name text,
        PRIMARY KEY {primary_key}
    )
    """
    execute_query(session, create_table_query)

    # Populate the table
    for line in UdacityUtils.read_datafile():
        query = f"INSERT INTO {table_name} "

        query = query + """
        (
            session_id,
            user_id,
            item_in_session,
            artist_name,
            song_name,
            first_name,
            last_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            int(line[CSV_POSITIONS['session_id']]),
            int(line[CSV_POSITIONS['user_id']]),
            int(line[CSV_POSITIONS['item_in_session']]),
            line[CSV_POSITIONS['artist_name']],
            line[CSV_POSITIONS['song_name']],
            line[CSV_POSITIONS['first_name']],
            line[CSV_POSITIONS['last_name']]
        )
        execute_query(session, query, params)

    # Execute the query
    rows = execute_query(
        session,
        f"""
            SELECT artist_name, song_name, first_name, last_name
            FROM {table_name}
            WHERE session_id={session_id} AND user_id={user_id}
            ORDER BY item_in_session
        """
    )

    # Drop table to prepare for next query
    execute_query(session, f"DROP TABLE IF EXISTS {table_name}")
    return rows


def query_three(session):
    """
    Params:
    - session - The Cassandra Cluster.session object

    # TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

    The objective of this is to locate the first and last name of users who listened a given song

    Therefore, the primary key should be a composite containing a partiion key of song_name, with a clustering column of user_id

    While the problem statement did not specify any ordering, the student chose to order results by user id, to ensure consistent testing
    """
    # Variables for the desired lookups
    song_name = "'All Hands Against His Own'"

    table_name = 'song_plays'
    primary_key = "((song_name), user_id)"

    # Create the table
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} 
    (
        song_name text,
        user_id int,
        first_name text,
        last_name text,
        PRIMARY KEY {primary_key}
    )
    """
    execute_query(session, create_table_query)

    # Populate the table
    for line in UdacityUtils.read_datafile():
        query = f"INSERT INTO {table_name} "

        query = query + """
        (
            song_name,
            user_id,
            first_name,
            last_name
        ) VALUES (%s, %s, %s, %s)
        """
        params = (
            line[CSV_POSITIONS['song_name']],
            int(line[CSV_POSITIONS['user_id']]),
            line[CSV_POSITIONS['first_name']],
            line[CSV_POSITIONS['last_name']]
        )
        execute_query(session, query, params)

    # Execute the query
    rows = execute_query(
        session,
        f"""
            SELECT last_name, first_name
            FROM {table_name}
            WHERE song_name={song_name}
            ORDER BY user_id
        """
    )

    # Drop table to prepare for next query
    execute_query(session, f"DROP TABLE IF EXISTS {table_name}")
    return rows


def init():
    """
    The execution of the code required was abstracted into it's own function
    so that it could be called when the file is run directly from the terminal
    """
    # Connect to a session in the cluster
    cluster, session = UdacityUtils.open_db_connection()

    # Create a Keyspace
    keyspace = create_keyspace(session)

    # Set KEYSPACE to the keyspace specified above
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
