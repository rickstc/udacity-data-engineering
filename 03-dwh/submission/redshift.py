"""
File: redshift.py
Attribution: Mixed
Background:
The code provided by Udacity contained the logic necessary to load
configuration from a config file and make a database connection
based on the configuration information.

Because this connection information was used in at least two places (
etl.py and create_tables.py], the student elected to abstract the logic
to its own class for the following reasons:
1. Reduced code duplication due to the reuse of connections
2. Reduced code complexity in main methods
3. Expanded error handling through the use of logging
"""

import configparser
import psycopg2
import logging
import sql_queries

logging.basicConfig(level=logging.DEBUG, filename='./error.log')


class Redshift:
    """
    This class creates a connection to the Redshift Database and sets
    the connection and cursor attributes on itself

    Key Methods:
    - close - Closes the connection to the Redshift cluster
    - execute - Executes a query
    - create_tables - Replicates functionality provided in create_tables.py
    to create the tables using the create table queries in sql_queries.py
    - drop_tables - Replicates functionality provided in create_tables.py
    to drop the tables using the drop table queries in sql_queries.py

    """

    def __init__(self) -> None:
        """
        Creates an instance of the class, connects to the redshift database
        and sets the connection and cursor properties on itself
        """
        self.conn = self._connect()
        if self.conn is None:
            raise Exception(
                "There was an error connecting to the RedShift cluster. \n \
                    Please check your error.log for more information.")
        self.cur = self.conn.cursor()

    def close(self):
        """
        Closes the connection to the cluster
        """
        self.conn.close()

    def execute(self, query, data=None, fetch=False):
        """
        Executes a given query

        Params:
        - query - The query to execute
        - data (optional) - The data to pass in with the query
        - fetch (optional) - Whether to use the 'fetchall' method defined on the cursor

        This is essentially a wrapper around psycopg2's cursor.execute to provide
        the logging of exception information
        """
        try:
            results = None
            if data is None:
                self.cur.execute(query)
            else:
                self.cur.execute(query, data)

            if fetch:
                results = self.cur.fetchall()
            self.conn.commit()
            return results
        except:
            print("Query execution failed. Please see 'error.log' for more information.")
            logging.exception(f'Query execution failed: \n {query} \n')

    def create_tables(self):
        """
        Creates the tables using the create table queries in sql_queries.py

        To ensure the tables are created, this will first drop the tables
        if they exist
        """
        self.drop_tables()
        for query in sql_queries.create_table_queries:
            self.execute(query)

    def drop_tables(self):
        """
        Drops the tables using the drop table queries in sql_queries.py
        """
        for query in sql_queries.drop_table_queries:
            self.execute(query)

    @staticmethod
    def _connect():
        try:

            config = configparser.ConfigParser()
            config.read('dwh.cfg')

            cluster = config['CLUSTER']
            # The student prefers the keyword argument connection method over
            # the string connection method because it is more explicit
            conn = psycopg2.connect(
                host=cluster['HOST'],
                dbname=cluster['DB_NAME'],
                user=cluster['DB_USER'],
                password=cluster['DB_PASSWORD'],
                port=cluster['DB_PORT']
            )
            return conn
        except Exception as ex:
            logging.exception(
                'Unable to connect to the cluster using credentials in dwh.cfg.')
            return None
