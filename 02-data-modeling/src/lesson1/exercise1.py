import psycopg2
from utils import DBUtils

utils = DBUtils()
utils.reset_database()

try:
    conn = utils.connect()
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)
