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
    # redshift.create_tables()

    # load_staging_tables(redshift.cur, redshift.conn)
    # # Check the number of records in the staging tables
    # print(
    #     f'Number of Events: {redshift.execute("SELECT COUNT(*) FROM staging_events;", fetch=True)}')

    # print(
    #     f'Number of Songs: {redshift.execute("SELECT COUNT(*) FROM staging_songs;", fetch=True)}')

    # # Populate Staging Tables
    # insert_tables(redshift.cur, redshift.conn)

    # Populate Analytics Tables from Staging Tables
    print(
        f'Number of SongPlays: {redshift.execute("SELECT COUNT(*) FROM songplays;", fetch=True)}')
    print(
        f'Number of Songs: {redshift.execute("SELECT COUNT(*) FROM songs;", fetch=True)}')
    print(
        f'Number of Users: {redshift.execute("SELECT COUNT(*) FROM users;", fetch=True)}')
    print(
        f'Number of Artists: {redshift.execute("SELECT COUNT(*) FROM artists;", fetch=True)}')
    print(
        f'Number of Time: {redshift.execute("SELECT COUNT(*) FROM time;", fetch=True)}')

    # Run analytic queries
    # TODO: The project didn't define any

    # Close our connection to Redshift
    redshift.close()


if __name__ == "__main__":
    main()
