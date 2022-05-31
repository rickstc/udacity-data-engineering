from redshift import Redshift
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """"""

    # Connection to the database has been moved to redshift.py
    redshift = Redshift()

    # This drops and recreates tables to ensure we're operating on a fresh db
    redshift.create_tables()

    load_staging_tables(redshift.cur, redshift.conn)
    insert_tables(redshift.cur, redshift.conn)

    # Populate Staging Tables

    # Populate Analytics Tables from Staging Tables

    # Run analytic queries
    # TODO: The project didn't define any

    # Close our connection to Redshift
    redshift.close()


if __name__ == "__main__":
    main()
