"""
File: etl.py
Attribution: Mixed
Background:
The student is leveraging the Redshift class defined in Redshift.py to ensure
a consisent database state with each run. Consequently, each time this file is run, the 
database tables will be dropped and recreated using the log data stored in S3.

This will essentially 'run' the ETL pipeline by loading, transforming, and then searching
the data set to accomplish the analytical goals of the startup (not provided by Udacity).
"""
from redshift import Redshift
from sql_queries import copy_table_queries, insert_table_queries, analytical_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main method. This will connect to our redshift cluster, load the log data
    into the staging tables from S3, populate analytics tables from the
    staging tables, execute analytical queries, then close the connection to redshift.


    The student has defined four simple analytics queries to get the top artists by
    play count, get the top songs by play count for 'free' level users, get the number
    of songplays by the listener's gender, and get the total number of song plays by
    day of week. The queries are defined in sql_queries.py. See the README.md for expected output.
    """

    # Connection to the database has been moved to redshift.py
    redshift = Redshift()

    # This drops and recreates tables to ensure we're operating on a fresh db
    redshift.create_tables()

    # Load the Staging Tables
    load_staging_tables(redshift.cur, redshift.conn)

    # Populate Analytics Tables
    insert_tables(redshift.cur, redshift.conn)

    # Run Analytics Queries
    for aq in analytical_queries:
        print("\n")
        for results in redshift.execute(aq, fetch=True):
            print(results)

    # Close our connection to Redshift
    redshift.close()


if __name__ == "__main__":
    main()
