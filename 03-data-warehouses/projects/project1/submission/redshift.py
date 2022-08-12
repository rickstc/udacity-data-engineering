"""
File: redshift.py
Attribution: Mixed
Background:
The code provided by Udacity contained the logic necessary to load
configuration from a config file and make a database connection
based on the configuration information. However, the student
extended this functionality by allowing the configuration information
to be loaded from a JSON file as a fallback if the configuration file
failed.

The student did this so as to hide sensitive configuration information
while also committing the base configuration file so that others could
use it to make a connection using their own information.

Because this connection information was used in at least two places (
etl.py and create_tables.py), the student elected to abstract the logic
to its own class for the following reasons:
1. Reduced code duplication due to the reuse of connections
2. Reduced code complexity in main methods
3. Expanded error handling through the use of logging
"""

import configparser
import psycopg2
import json
import os
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

    def execute(self, query, data=None):
        """
        Executes a given query

        Params:
        - query - The query to execute
        - data (optional) - The data to pass in with the query

        This is essentially a wrapper around psycopg2's cursor.execute to provide
        the logging of exception information
        """
        try:
            if data is None:
                self.cur.execute(query)
            else:
                self.cur.execute(query, data)
            self.conn.commit()
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

            conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
                *config['CLUSTER'].values()))
            return conn
        except Exception as ex:
            try:
                if not os.path.exists('config.json'):
                    logging.exception(
                        'Unable to connect to the cluster using credentials in dwh.cfg.')
                    logging.debug(
                        "You must either configure the 'dwg.cfg' file, or provide a 'config.json' file")
                    return None
                with open('config.json', 'r') as config_file:
                    config = json.load(config_file)

                """
                The student's json configuration file allows for different execution environments
                due to its structure, which is as follows:
                {
                    "local": {
                        "cluster": {
                            ... connection properties ...
                        }
                    },
                    "production": {
                        "cluster": {
                            ... connection properties ...
                        }
                    }
                }

                By setting the 'STACK' environment variable, the student can connect
                to either a local development environment or the production Redshift
                environment hosted within AWS.
                """
                stack = os.environ.get('STACK', 'local')
                cluster = config[stack].get('cluster')

                # The student prefers the keyword argument connection method over
                # the string connection method because it is more explicit
                conn = psycopg2.connect(
                    host=cluster.get('host'),
                    dbname=cluster.get('db_name'),
                    user=cluster.get('db_user'),
                    password=cluster.get('db_password'),
                    port=cluster.get('db_port')
                )
                return conn
            except Exception as ex:
                logging.exception(
                    "Unable to connect to the cluster using credentials in config.json.")
