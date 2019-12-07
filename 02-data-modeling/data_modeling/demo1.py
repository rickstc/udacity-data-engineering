import psycopg2

def execute_query(cursor, sql):
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except psycopg2.Error as e:
        print(e)
    return False

def create_database(cursor, dbname):
    try:
        cursor.execute(f"CREATE DATABASE {dbname}")
        return True
    except psycopg2.Error as e:
        print(e)
    return False

def remove_database(cursor, dbname):
    try:
        cursor.execute(f"DROP DATABASE {dbname}")
        return True
    except psycopg2.Error as e:
        print(e)
    return False

def get_connection(db_name='test'):
    try:
        connection = psycopg2.connect(
            host="localhost",
            dbname=db_name,
            user="someuser",
            password="somepass",
            port=9000
        )
        connection.set_session(autocommit=True)
        return connection
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
        return False

    print("Something else happened.")
    return False

def get_cursor(connection):
    try:
        cur = connection.cursor()
        return cur
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")
        print(e)
        return False
    print("Something else happened.")
    return False

def cleanup(db_name, remove_db=False):
    if remove_db:
        connection = get_connection()
        cursor = get_cursor(connection)
        remove_database(cursor, db_name)
    try:
        connection.close()
    except psycopg2.Error as e:
        print(e)
        return False
    return True

def setup_database(db_name):
    connection = get_connection()
    cur = get_cursor(connection)
    if create_database(cur, db_name):
        connection.close()
        return True
    connection.close()
    return False

def create_music_lib(cursor):
    sql = "CREATE TABLE IF NOT EXISTS music_library (album_name varchar, artist_name varchar, year int);"
    execute_query(cursor, sql)
    return True

def insert_music_lib(cursor, album_name, artist_name, year):
    try:
        cursor.execute("INSERT INTO music_library \
            VALUES (%s, %s, %s)", \
            (album_name, artist_name, year))
        return True
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)
        return False

if __name__ == "__main__":
    db_name = "udacity"

    if setup_database(db_name):
        print("Yay")
        connection = get_connection(db_name)
        cur = get_cursor(connection)
        create_music_lib(cur)
        print(execute_query(cur, "SELECT COUNT(*) FROM music_library"))
        print(insert_music_lib(cur, "Let it Be", "The Beatles", 1970))
        print(insert_music_lib(cur, "Rubber Soul", "The Beatles", 1965))
        print(execute_query(cur, "SELECT * FROM music_library"))
        connection.close()


    cleanup(db_name, True)



    