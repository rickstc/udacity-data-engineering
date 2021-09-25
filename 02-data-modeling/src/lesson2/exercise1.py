import psycopg2
from utils import PGUtils

PGUtils.reset_databases()


"""
Create a connection to the database, get a cursor, and set autocommit to true)

The follow replaces:
try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get cursor to the Database")
    print(e)
conn.set_session(autocommit=True)


"""
conn = PGUtils.connect()
conn.set_session(autocommit=True)
cur = conn.cursor()

"""
Let's imagine we have a table called Music Store.
Table Name: music_store
column 0: Transaction Id
column 1: Customer Name
column 2: Cashier Name
column 3: Year 
column 4: Albums Purchased

Now to translate this information into a CREATE Table Statement and insert the data
(Table Provided in an Image)
"""

# TO-DO: Add the CREATE Table Statement and INSERT statements to add the data in the table

try:
    cur.execute("CREATE TABLE IF NOT EXISTS music_store (transaction_id int, customer_name varchar, cashier_name varchar, year int, albums_purchased varchar[])")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)",
                (1, "Amanda", "Sam", 2000, ['Rubber Soul', 'Let it Be']))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)",
                (2, "Toby", "Sam", 2000, ['My Generation']))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)",
                (3, "Max", "Bob", 2018, ["Meet the Beatles", "Help!"]))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)


try:
    cur.execute("SELECT * FROM music_store;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

"""
Moving to 1st Normal Form (1NF)
TO-DO: This data has not been normalized. To get this data into 1st normal form, you need to remove any collections or list of data and break up the list of songs into individual rows.
"""

# TO-DO: Complete the CREATE table statements and INSERT statements

try:
    cur.execute("CREATE TABLE IF NOT EXISTS music_store2 (transaction_id int, customer_name varchar, cashier_name varchar, year int, album_purchased varchar);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, album_purchased) \
                 VALUES (%s, %s, %s, %s, %s)",
                (1, "Amanda", "Sam", 2000, 'Rubber Soul'))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, album_purchased) \
                 VALUES (%s, %s, %s, %s, %s)",
                (2, "Amanda", "Sam", 2000, 'Let it Be'))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, album_purchased) \
                 VALUES (%s, %s, %s, %s, %s)",
                (3, "Toby", "Sam", 2000, 'My Generation'))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, album_purchased) \
                 VALUES (%s, %s, %s, %s, %s)",
                (4, "Max", "Bob", 2018, "Meet the Beatles"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, album_purchased) \
                 VALUES (%s, %s, %s, %s, %s)",
                ((5, "Max", "Bob", 2018, "Help!")))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("SELECT * FROM music_store2;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()


"""
Moving to 2nd Normal Form (2NF)
You have now moved the data into 1NF, which is the first step in moving to 2nd Normal Form. The table is not yet in 2nd Normal Form. While each of the records in the table is unique, our Primary key (transaction id) is not unique.

TO-DO: Break up the table into two tables, transactions and albums sold.
"""
try:
    cur.execute(
        "CREATE TABLE IF NOT EXISTS transactions (transaction_id int, customer_name varchar, cashier_name varchar, year int);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute(
        "CREATE TABLE IF NOT EXISTS albums_sold (album_id int, album_name varchar, transaction_id int);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)",
                (1, 'Amanda', 'Sam', 2000))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)",
                (2, 'Toby', 'Sam', 2000))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)",
                (3, 'Max', 'Bob', 2018))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, album_name, transaction_id) \
                 VALUES (%s, %s, %s)",
                (1, "Rubber Soul", 1))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, album_name, transaction_id) \
                 VALUES (%s, %s, %s)",
                (2, "Let it Be", 1))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, album_name, transaction_id) \
                 VALUES (%s, %s, %s)",
                (3, "My Generation", 2))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, album_name, transaction_id) \
                 VALUES (%s, %s, %s)",
                (4, "Meet the Beetles", 3))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, album_name, transaction_id) \
                 VALUES (%s, %s, %s)",
                (5, "Help!", 3))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

print("Table: transactions\n")
try:
    cur.execute("SELECT * FROM transactions;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

print("\nTable: albums_sold\n")
try:
    cur.execute("SELECT * FROM albums_sold;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)
row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()


"""
TO-DO: Do a JOIN on these tables to get all the information in the original first Table.
"""
# TO-DO: Complete the join on the transactions and album_sold tables

try:
    cur.execute(
        "SELECT * FROM transactions JOIN albums_sold ON transactions.transaction_id = albums_sold.transaction_id ;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

"""
Moving to 3rd Normal Form (3NF)
Check our table for any transitive dependencies. HINT: Check the table for any transitive dependencies. Transactions can remove Cashier Name to its own table, called Employees, which will leave us with 3 tables.
"""
try:
    cur.execute(
        "CREATE TABLE IF NOT EXISTS transactions2 (transaction_id int, customer_name varchar, employee_id int, year int);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute(
        "CREATE TABLE IF NOT EXISTS employees (employee_id int, employee_name varchar);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, employee_id, year) \
                 VALUES (%s, %s, %s, %s)",
                (1, "Amanda", 1, 2000))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, employee_id, year) \
                 VALUES (%s, %s, %s, %s)",
                (2, "Toby", 1, 2000))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, employee_id, year) \
                 VALUES (%s, %s, %s, %s)",
                (3, "Max", 2, 2018))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO employees (employee_id, employee_name) \
                 VALUES (%s, %s)",
                (1, "Sam"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO employees (employee_id, employee_name) \
                 VALUES (%s, %s)",
                (2, "Bob"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

print("Table: transactions2\n")
try:
    cur.execute("SELECT * FROM transactions2;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

print("\nTable: albums_sold\n")
try:
    cur.execute("SELECT * FROM albums_sold;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

print("\nTable: employees\n")
try:
    cur.execute("SELECT * FROM employees;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

"""
TO-DO: Complete the last two JOIN on these 3 tables so we can get all the information we had in our first Table.
"""
try:
    cur.execute("SELECT * FROM (transactions2 JOIN employees ON \
                               transactions2.employee_id = employees.employee_id) JOIN \
                               albums_sold ON transactions2.transaction_id=albums_sold.transaction_id;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

cur.close()
conn.close()
# Cleanup replaces the table dropping:
PGUtils.reset_databases()
