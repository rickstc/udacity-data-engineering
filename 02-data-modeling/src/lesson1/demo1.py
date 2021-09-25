import psycopg2
from utils import PGUtils


def execute_query(cursor, sql):
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except psycopg2.Error as e:
        print(e)
    return False


def get_cursor(connection):
    connection.set_session(autocommit=True)

    try:
        cur = connection.cursor()
        return cur
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")
        print(e)
        return False


def create_music_lib(cursor):
    sql = "CREATE TABLE IF NOT EXISTS music_library (album_name varchar, artist_name varchar, year int);"
    execute_query(cursor, sql)
    return True


def insert_music_lib(cursor, album_name, artist_name, year):
    try:
        cursor.execute("INSERT INTO music_library \
            VALUES (%s, %s, %s)",
                       (album_name, artist_name, year))
        return True
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)
        return False


if __name__ == "__main__":
    PGUtils.reset_databases()

    connection = PGUtils.connect()
    cur = get_cursor(connection)
    create_music_lib(cur)
    print(execute_query(cur, "SELECT COUNT(*) FROM music_library"))
    print(insert_music_lib(cur, "Let it Be", "The Beatles", 1970))
    print(insert_music_lib(cur, "Rubber Soul", "The Beatles", 1965))
    print(execute_query(cur, "SELECT * FROM music_library"))
    connection.close()
