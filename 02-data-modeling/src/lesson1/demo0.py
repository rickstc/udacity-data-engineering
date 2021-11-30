import psycopg2

from utils import PGUtils

connection = PGUtils.connect()
connection.set_session(autocommit=True)

cursor = connection.cursor()

# Create the table
cursor.execute("CREATE TABLE test123 (col1 int, col2 int, col3 int);")

# Select and print
cursor.execute("SELECT COUNT(*) FROM test123")
print(cursor.fetchall())

# Remove
cursor.execute("DROP TABLE test123")
