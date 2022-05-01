import psycopg2
import os
import httpx
from psycopg2.errors import DuplicateDatabase


class PGUtils:
    """
    Utilities to 'shortcut' some operations that the classroom's Jupyter notebook would take care of
    My goal is not to circumvent following the course exercises, but to ensure database consistency
    """
    PG_USER = "postgres"
    PG_PASS = "somepass"
    PG_DB = "test"
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
    def create_db(self, db_name):
        try:
            conn = self.connect('postgres')
            conn.set_session(autocommit=True)
            cursor = conn.cursor()

            # Create New Database
            cursor.execute(f'CREATE DATABASE {db_name}')
            cursor.close()
            conn.close()

        except DuplicateDatabase as ex:
            pass

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

    @classmethod
    def load_pagila(self):
        """
        This will do everything necessary to create a functioning
        'pagila' database
        """

        # Setup variables
        db_directory = os.path.join(
            os.path.abspath(
                os.path.dirname(
                    __file__
                )
            ),
            'data'
        )

        schema_file_url = 'https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql'
        schema_path = os.path.join(
            db_directory,
            schema_file_url.rsplit('/', 1)[1]
        )

        db_file_url = 'https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql'
        db_path = os.path.join(
            db_directory,
            db_file_url.rsplit('/', maxsplit=1)[1]
        )

        # Create the data directory
        if not os.path.exists(db_directory):
            os.makedirs(db_directory, exist_ok=True)

        # Download the DB files
        if not os.path.exists(schema_path):
            r = httpx.get(schema_file_url)
            if r.status_code == 200:
                with open(schema_path, 'w') as the_file:
                    the_file.write(r.text)

        if not os.path.exists(db_path):
            r = httpx.get(db_file_url)
            if r.status_code == 200:
                with open(db_path, 'w') as the_file:
                    the_file.write(r.text)

        # Create the database
        PGUtils.create_db('pagila')

        # Load the DB Files
        os.system(
            'cat data/pagila-schema.sql | docker exec -i src_pg_1 psql -U postgres -d pagila'
        )
        os.system(
            'cat data/pagila-data.sql | docker exec -i src_pg_1 psql -U postgres -d pagila'
        )
