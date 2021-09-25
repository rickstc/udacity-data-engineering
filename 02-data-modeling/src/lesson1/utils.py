import psycopg2


class DBUtils:
    """
    Utilities to 'shortcut' some operations that the provided online development environment would already provide
    """
    PG_USER = "someuser"
    PG_PASS = "somepass"
    PG_DB = "tmpdb"
    PG_HOST = "127.0.0.1"
    PG_PORT = "9000"

    @classmethod
    def reset_databases(self):
        try:
            conn = self.connect('postgres')
            conn.set_session(autocommit=True)
            cursor = conn.cursor()

            # Delete existing databases
            DBS_TO_PRESERVE = ('postgres', 'template1', 'template0')
            cursor.execute('SELECT datname FROM pg_database')

            dbs = cursor.fetchall()
            for db in dbs:
                db_name = db[0]
                if db_name not in DBS_TO_PRESERVE:
                    cursor.execute(f"DROP DATABASE {db_name}")

            # Create New Database
            cursor.execute(f'CREATE DATABASE {self.PG_DB}')

        except Exception as e:
            print(e)

    @classmethod
    def connect(self, dbname=None):
        if dbname is None:
            dbname = self.PG_DB
        return psycopg2.connect(
            database=dbname,
            user=self.PG_USER,
            password=self.PG_PASS,
            host=self.PG_HOST,
            port=self.PG_PORT
        )
