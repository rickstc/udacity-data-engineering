import psycopg2

""" 
Startup
"""
conn = psycopg2.connect(
        host="localhost",
        dbname="test",
        user="someuser",
        password="somepass",
        port=9000
    )
conn.set_session(autocommit=True)
cur = conn.cursor()
cur.execute("DROP DATABASE IF EXISTS udacity")
cur.execute("CREATE DATABASE udacity")
cur.close()

try: 
    
    conn.close()
    conn = psycopg2.connect(
            host="localhost",
            dbname="udacity",
            user="someuser",
            password="somepass",
            port=9000
        )

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
Exercises
"""

# Create the fact table
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS customer_transactions (customer_id int, store_id int, spent NUMERIC(5, 2))")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
#Insert into all tables 
try: 
    cur.execute(
        "INSERT INTO customer_transactions (customer_id, store_id, spent) \
        VALUES (%s, %s, %s)", \
        (1, 1, 20.50)
    )
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
try: 
    cur.execute(
        "INSERT INTO customer_transactions (customer_id, store_id, spent) \
        VALUES (%s, %s, %s)", \
        (2, 1, 35.21)
    )
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

# Create the dimension tables and insert data
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS customer (customer_id int, customer_name varchar, rewards boolean)")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
   cur.execute(
        "INSERT INTO customer (customer_id, customer_name, rewards) \
        VALUES (%s, %s, %s)", \
        (1, "Amanda", True)
    )
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute(
        "INSERT INTO customer (customer_id, customer_name, rewards) \
        VALUES (%s, %s, %s)", \
        (2, "Toby", False)
    )
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS store (store_id int, state varchar)")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
   cur.execute(
        "INSERT INTO store (store_id, state) \
        VALUES (%s, %s)", \
        (1, "CA")
    )
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute(
        "INSERT INTO store (store_id, state) \
        VALUES (%s, %s)", \
        (2, "WA")
    )
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS items_purchased (customer_id int, item_number int, item_name varchar)")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
   cur.execute(
        "INSERT INTO items_purchased (customer_id, item_number, item_name) \
        VALUES (%s, %s, %s)", \
        (1, 1, "Rubber Soul")
    )
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute(
        "INSERT INTO items_purchased (customer_id, item_number, item_name) \
        VALUES (%s, %s, %s)", \
        (2, 3, "Let It Be")
    )
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

# Query 1: Find all customers that spent more than 30 dollars, who they are, which store, location of store, what they bought, and whether they are a rewards member
# Expected: ('Toby', 1, 'CA', 'Let It Be', False)
try: 
    cur.execute(
        "SELECT c.customer_name, s.store_id, s.state, i.item_name, c.rewards\
        FROM customer_transactions t \
        JOIN store s ON t.store_id = s.store_id \
        JOIN customer c ON t.customer_id = c.customer_id \
        JOIN items_purchased i ON c.customer_id = i.customer_id \
        WHERE spent > 30"
    )
    
    
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

# Query 2: How much did customer 2 spend?
# Expected: (2, 35.21)
try: 
    cur.execute("SELECT customer_id, spent FROM customer_transactions WHERE customer_id=2")
    
    
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


# Drop the tables
table_names = ['customer_transactions', 'customer', 'store', 'items_purchased']
for tname in table_names:
    try:
        cur.execute(f"DROP TABLE {tname}")
        
    except psycopg2.Error as e: 
        print("Error: Dropping table")
        print (e)

"""
Cleanup
"""
## Cleanup
cur.close()
conn.close()