import psycopg2
from cassandra.cluster import Cluster


class PGUtils:
    """
    Utilities to 'shortcut' some operations that the classroom's Jupyter notebook would take care of
    My goal is not to circumvent following the course exercises, but to ensure database consistency
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


class CassUtils:
    """
    Utilities for interacting and ensuring consistency within Apache Cassandra
    """
    CASS_HOST = '127.0.0.1'
    DEFAULT_KEYSPACE = "udacity"

    def _list_keyspaces(session):
        keyspaces = set()
        query = session.execute('DESCRIBE keyspaces')
        for row in query.all():
            keyspaces.add(row.keyspace_name)
        return keyspaces

    @classmethod
    def create_keyspace(self, session, keyspace_name=None):
        if keyspace_name is None:
            keyspace_name = self.DEFAULT_KEYSPACE
        if keyspace_name.startswith('system'):
            print("Your chosen keyspace name should not begin with system*")
            return None
        try:
            rep = {'class': 'SimpleStrategy', 'replication_factor': 1}
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH REPLICATION = {rep}")
            return True
        except Exception as e:
            print(e)
            return False

    @classmethod
    def reset_cluster(self):
        session = self.connect()
        keyspaces = self._list_keyspaces(session)
        for keyspace in keyspaces:
            if not keyspace.startswith('system'):
                session.execute(f'DROP KEYSPACE IF EXISTS {keyspace}')

    @classmethod
    def connect(self):
        cluster = Cluster([self.CASS_HOST])
        session = cluster.connect()
        return session
