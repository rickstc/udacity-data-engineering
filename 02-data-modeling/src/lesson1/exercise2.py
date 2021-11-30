import cassandra
from utils import CassUtils

CassUtils.reset_cluster()

# Create Connection to Database
session = CassUtils.connect()

# Create Keyspace
CassUtils.create_keyspace(session, CassUtils.DEFAULT_KEYSPACE)

# Connect to keyspace
session.set_keyspace(CassUtils.DEFAULT_KEYSPACE)

"""
Create a Song Library that contains a list of songs, including the song name, artist name, year, album it was from, and if it was a single.
song_title
artist_name
year
album_name
single

TO-DO: You need to create a table to be able to run the following query:
select * from songs WHERE year=1970 AND artist_name="The Beatles"
"""
# TO-DO: Complete the query below
query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + \
    "(song_title text, artist_name text, year int, album_name text, single boolean, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

"""
TO-DO: Insert the following two rows in your table
First Row:  "Across The Universe", "The Beatles", "1970", "False", "Let It Be"

Second Row: "The Beatles", "Think For Yourself", "False", "1965", "Rubber Soul"
"""

# Add in query and then run the insert statement
query = "INSERT INTO music_library (song_title, artist_name, year, album_name, single)"
query = query + " VALUES (%s, %s, %s, %s, %s)"

try:
    session.execute(query, ("Across The Universe",
                    "The Beatles", 1970, "Let It Be", False))
except Exception as e:
    print(e)

try:
    session.execute(query, ("Think For Yourself",
                    "The Beatles", 1965, "Rubber Soul", False))
except Exception as e:
    print(e)


"""
TO-DO: Validate your data was inserted into the table.
"""
# TO-DO: Complete and then run the select statement to validate the data was inserted into the table
query = 'SELECT * FROM music_library'
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.album_name, row.artist_name)


"""
TO-DO: Validate the Data Model with the original query.
select * from songs WHERE YEAR=1970 AND artist_name="The Beatles"
"""

# TO-DO: Complete the select statement to run the query
query = "SELECT * FROM music_library WHERE YEAR=1970 AND artist_name='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.album_name, row.artist_name)

CassUtils.reset_cluster()
