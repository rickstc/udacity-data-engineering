"""
File: create_tables.py
Attribution: Udacity
Background:
This file contains the essentials necessary to make a connection to
the Redshift database and drop any tables if necessary, and create 
new tables based on the queries in sql_queries.py

Because the student abstracted connection logic into the Redshift class,
this file became very simple; only providing two methods: drop_tables
and create_tables.

The student elected to replicate the functionality provided here
directly on the Redshift class to drop/create tables.
"""

from redshift import Redshift
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(redshift):
    """
    Deprecated
    Please see drop_tables in redshift.py
    """
    for query in drop_table_queries:
        redshift.execute(query)


def create_tables(redshift):
    """
    Deprecated
    Please see create_tables in redshift.py
    """
    for query in create_table_queries:
        redshift.execute(query)


def main():
    """
    Deprecated
    This file is no longer necessary because this functionality is provided
    by redshift.py, and can be called as necessary from etl.py
    """
    redshift = Redshift()

    drop_tables(redshift)
    create_tables(redshift)

    redshift.close()


if __name__ == "__main__":
    main()
