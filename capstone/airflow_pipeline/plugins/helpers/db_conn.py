import glob
import csv
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.sql import text


class DBHelpers:
    """
    Utility to abstract common logic, like making a database
    connection and running queries
    """

    @staticmethod
    def connect(
        dbname="studentdb",
        host="postgis",
        user="student",
        password="student",
        autocommit=True,
    ):
        """
        Returns a psycopg2 connection object
        """
        connection = psycopg2.connect(
            dbname=dbname,
            host=host,
            user=user,
            password=password,
        )
        if autocommit:
            connection.set_session(autocommit=True)

        return connection

    @staticmethod
    def get_engine(
        dbname="studentdb",
        host="postgis",
        user="student",
        password="student",
    ):
        """
        The student prefers to work with psycopg2, however, the pandas
        implementation seems less complex when using sqlalchemy. This returns
        a sqlalchemy Engine instance.
        """
        return create_engine(f"postgresql://{user}:{password}@{host}/{dbname}")

    @staticmethod
    def clear_table(db_connection, table_name):
        """
        Deletes all records from a table without removing the table.
        """
        cursor = db_connection.cursor()
        cursor.execute(f"DELETE FROM {table_name}")
        cursor.close()

    @staticmethod
    def retrieve_table_df(engine, table_name):
        """
        Selects all records from a given table into a pandas dataframe
        """
        conn = engine.connect()
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df

    @staticmethod
    def insert_table_df(engine, table_name, data_frame):
        """
        Inserts a dataframe into the given table
        """
        conn = engine.connect()
        data_frame.to_sql(con=conn, name=table_name, index=False, if_exists="append")
        conn.close()
