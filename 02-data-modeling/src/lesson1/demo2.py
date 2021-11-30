import cassandra
from cassandra.cluster import Cluster
from utils import CassUtils


def create_table(session):
    query = "CREATE TABLE IF NOT EXISTS music_library"
    query = query + \
        "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"
    try:
        session.execute(query)
        return True
    except Exception as e:
        print(e)
        return False


def select_count(session):
    query = "select count(*) from music_library"
    try:
        count = session.execute(query)
        return count
    except Exception as e:
        print(e)
        return None


def insert_data(session, year, artist, album):
    query = 'INSERT INTO music_library (year, artist_name, album_name)'
    query = query + ' VALUES(%s, %s, %s)'

    try:
        session.execute(query, (year, artist, album))
        return True
    except Exception as e:
        print(e)
        return False


def get_rows(session):
    query = "select * from music_library WHERE YEAR=1970"
    try:
        rows = session.execute(query)
        return rows
    except Exception as e:
        print(e)
        return None


def drop_table(session):
    query = "drop table music_library"
    try:
        session.execute(query)
        return True
    except Exception as e:
        print(e)
        return False


def init():
    CassUtils.reset_cluster()
    # init()
    session = CassUtils.connect()
    CassUtils.create_keyspace(session, 'udacity')
    session.set_keyspace('udacity')

    if create_table(session) is False:
        return None

    insert_data(session, 1970, "The Beatles", "Let it Be")
    insert_data(session, 1965, "The Beatles", "Rubber Soul")

    count = select_count(session)
    print(count.one())

    rows = get_rows(session)
    if rows is None:
        return None
    for row in rows:
        print(f'{row.year}, {row.album_name}, {row.artist_name}')

    drop_table(session)
    session.shutdown()
    CassUtils.reset_cluster()


if __name__ == "__main__":
    init()
