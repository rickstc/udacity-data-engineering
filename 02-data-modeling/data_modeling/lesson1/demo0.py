import psycopg2

connection = psycopg2.connect(
    host="localhost",
    dbname="test",
    user="someuser",
    password="somepass",
    port=9000
)
connection.set_session(autocommit=True)

cursor = connection.cursor()

# Create the table
cursor.execute("CREATE TABLE test123 (col1 int, col2 int, col3 int);")

# Select and print
cursor.execute("SELECT COUNT(*) FROM test123")
print(cursor.fetchall())

# Remove
cursor.execute("DROP TABLE test123")