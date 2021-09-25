import psycopg2


class DBUtils:
    PG_USER = "someuser"
    PG_PASS = "somepass"
    PG_DB = "test"
    PG_HOST = "127.0.0.1"
    PG_PORT = "9000"

    @classmethod
    def reset_database(self):
        try:
            conn = self.connect('postgres')
            conn.set_session(autocommit=True)
            cursor = conn.cursor()
            cursor.execute(f'DROP DATABASE {self.PG_DB}')
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
