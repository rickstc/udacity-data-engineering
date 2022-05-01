import os
from utils import PGUtils


def print_counts(connection):
    """
    This basically prints a count of all items in some tables. This was a shell command
    provided by Udacity for use in the jupyter notebook that the student has converted
    to use python:

    nStores = %sql select count(*) from store;
    nFilms = %sql select count(*) from film;
    nCustomers = %sql select count(*) from customer;
    nRentals = %sql select count(*) from rental;
    nPayment = %sql select count(*) from payment;
    nStaff = %sql select count(*) from staff;
    nCity = %sql select count(*) from city;
    nCountry = %sql select count(*) from country;

    print("nFilms\t\t=", nFilms[0][0])
    print("nCustomers\t=", nCustomers[0][0])
    print("nRentals\t=", nRentals[0][0])
    print("nPayment\t=", nPayment[0][0])
    print("nStaff\t\t=", nStaff[0][0])
    print("nStores\t\t=", nStores[0][0])
    print("nCities\t\t=", nCity[0][0])
    print("nCountry\t\t=", nCountry[0][0])

    """
    cursor = connection.cursor()
    for table in ['store', 'film', 'customer', 'rental', 'payment', 'staff', 'city', 'country']:
        rows = cursor.execute(f'SELECT COUNT(*) FROM {table}')
        rows = cursor.fetchall()
        print(f"Number of items in the {table} table: {rows[0][0]}")

    cursor.close()


def print_date_range(connection):
    """
    Prints the earliest and latest dates from the payment table

    Replaces the shell command:
    %%sql 
    select min(payment_date) as start, max(payment_date) as end from payment;
    """
    cursor = connection.cursor()
    query = "SELECT MIN(payment_date) AS start, MAX(payment_date) AS end FROM payment"
    cursor.execute(query)
    results = cursor.fetchall()
    for r in results:
        print(r)
    cursor.close()


def addresses_by_district(con):
    """
    TODO: Write a query that displays the number of addresses by district in the address table.
    Limit the table to the top 10 districts. Your results should match the table below.

    district	        n
    Buenos Aires	    10
    California	        9
    Shandong	        9
    West Bengali	    9
    So Paulo	        8
    Uttar Pradesh	    8
    Maharashtra	        7
    England	            7
    Southern Tagalog	6
    Punjab	            5
    """
    cur = con.cursor()

    query = \
        """
            SELECT district, COUNT(*) AS n
            FROM address
            GROUP BY district
            ORDER BY n DESC
            LIMIT 10
        """
    cur.execute(query)
    results = cur.fetchall()

    for r in results:
        print(r)

    cur.close()


def sum_movie_rental_revenue(con):
    """
    3.1.5 sum movie rental revenue
    TODO: Write a query that displays the amount of revenue from each title.
    Limit the results to the top 10 grossing titles. Your results should match the table below.

    Results should match:
    title	            revenue
    TELEGRAPH VOYAGE	231.73
    WIFE TURN	        223.69
    ZORRO ARK	        214.69
    GOODFELLAS SALUTE	209.69
    SATURDAY LAMBS	    204.72
    TITANS JERK	        201.71
    TORQUE BOUND	    198.72
    HARRY IDAHO	        195.70
    INNOCENT USUAL	    191.74
    HUSTLER PARTY	    190.78
    """
    cur = con.cursor()

    query = \
        """
            SELECT title, SUM(amount) AS revenue
            FROM film
            JOIN inventory ON film.film_id = inventory.film_id
            JOIN rental ON inventory.inventory_id = rental.inventory_id
            JOIN payment ON rental.rental_id = payment.rental_id
            GROUP BY title
            ORDER BY revenue DESC
            LIMIT 10
        """
    cur.execute(query)
    results = cur.fetchall()

    print(f"""

        3.1.5 Top Grossing Titles:
    
    """)

    for r in results:
        print(r)

    cur.close()


def top_grossing_cities(con):
    """
    3.2.2 Top grossing cities
    TODO: Write a query that returns the total amount of revenue by city as measured by the amount 
    variable in the payment table.
    Limit the results to the top 10 cities. Your result should match the table below.

    Results should match:
    city	            revenue
    Cape Coral	        221.55
    Saint-Denis	        216.54
    Aurora	            198.50
    Molodetno	        195.58
    Apeldoorn	        194.61
    Santa Brbara dOeste	194.61
    Qomsheh	            186.62
    London	            180.52
    Ourense (Orense)	177.60
    Bijapur	            175.61
    """
    cur = con.cursor()

    query = \
        """
            SELECT city, SUM(amount) AS revenue
            FROM city
            JOIN address ON city.city_id = address.city_id
            JOIN customer ON address.address_id = customer.address_id
            JOIN payment ON customer.customer_id = payment.customer_id
            GROUP BY city
            ORDER BY revenue DESC
            LIMIT 10
        """
    cur.execute(query)
    results = cur.fetchall()

    print(f"""

        3.2.2 Top Grossing Cities:
    
    """)
    for r in results:
        print(r)

    cur.close()


def revenue_by_customer_city_month(con):
    """
    3.3.3 Sum of revenue of each movie by customer city and by month
    TODO: Write a query that returns the total amount of revenue for each movie by customer city and by month.
    Limit the results to the top 10 movies. Your result should match the table below.

    Results should match:
    title	            city	    month	revenue
    SHOW LORD	        Mannheim	1.0	    11.99
    AMERICAN CIRCUS	    Callao	    1.0	    10.99
    CASUALTIES ENCINO	Warren	    1.0	    10.99
    TELEGRAPH VOYAGE	Naala-Porto	1.0	    10.99
    KISSING DOLLS	    Toulon	    1.0	    10.99
    MILLION ACE	        Bergamo	    1.0	    9.99
    TITANS JERK	        Kimberley	1.0	    9.99
    DARKO DORADO	    Bhilwara	1.0	    9.99
    SUNRISE LEAGUE	    Nagareyama	1.0	    9.99
    MILLION ACE	        Gaziantep	1.0	    9.99
    """
    cur = con.cursor()

    query = \
        """
            SELECT title, city, EXTRACT(month FROM p.payment_date) as month, SUM(amount) AS revenue
            FROM film f
            JOIN inventory i ON f.film_id = i.film_id
            JOIN rental r ON i.inventory_id = r.inventory_id
            JOIN payment p ON r.rental_id = p.rental_id
            JOIN customer c ON p.customer_id = c.customer_id
            JOIN address a ON c.address_id = a.address_id
            JOIN city ON city.city_id = a.city_id
            WHERE EXTRACT(month FROM p.payment_date)=1
            GROUP BY title, month, city
            ORDER BY revenue DESC
            LIMIT 10
        """
    cur.execute(query)
    results = cur.fetchall()

    print(f"""

        3.3.3 Revenue By Customer City and Month:
    
    """)
    for r in results:
        print(r)

    cur.close()


def create_first_dimension_table(con):
    """
    TODO: Create the dimDate dimension table with the fields and data types shown in the ERD above.

    Then, check your work with this query:

    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name   = 'dimdate'

    column_name	data_type
    date_key	integer
    date	    date
    year	    smallint
    quarter	    smallint
    month	    smallint
    day	        smallint
    week	    smallint
    is_weekend	boolean
    """

    cur = con.cursor()
    create_table_query = """
        CREATE TABLE IF NOT EXISTS dimDate
        (
            date_key    SERIAL PRIMARY KEY,
            date        date NOT NULL,
            year        smallint NOT NULL,
            quarter     smallint NOT NULL,
            month       smallint NOT NULL,
            day         smallint NOT NULL,
            week        smallint NOT NULL,
            is_weekend  boolean
        )
        
    """
    cur.execute(create_table_query)

    check_query = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name   = 'dimdate'
    """
    cur.execute(check_query)
    print(f"""

        Step 4.a Create the first dimension table
    
    """)
    for r in cur.fetchall():
        print(r)

    cur.close()


def create_other_dimension_tables(con):
    """ These queries were provided by Udacity """
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE dimCustomer
    (
        customer_key SERIAL PRIMARY KEY,
        customer_id  smallint NOT NULL,
        first_name   varchar(45) NOT NULL,
        last_name    varchar(45) NOT NULL,
        email        varchar(50),
        address      varchar(50) NOT NULL,
        address2     varchar(50),
        district     varchar(20) NOT NULL,
        city         varchar(50) NOT NULL,
        country      varchar(50) NOT NULL,
        postal_code  varchar(10),
        phone        varchar(20) NOT NULL,
        active       smallint NOT NULL,
        create_date  timestamp NOT NULL,
        start_date   date NOT NULL,
        end_date     date NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE dimMovie
    (
        movie_key          SERIAL PRIMARY KEY,
        film_id            smallint NOT NULL,
        title              varchar(255) NOT NULL,
        description        text,
        release_year       smallint,
        language           varchar(20) NOT NULL,
        original_language  varchar(20),
        rental_duration    smallint NOT NULL,
        length             smallint NOT NULL,
        rating             varchar(5) NOT NULL,
        special_features   varchar(60) NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE dimStore
    (
        store_key           SERIAL PRIMARY KEY,
        store_id            smallint NOT NULL,
        address             varchar(50) NOT NULL,
        address2            varchar(50),
        district            varchar(20) NOT NULL,
        city                varchar(50) NOT NULL,
        country             varchar(50) NOT NULL,
        postal_code         varchar(10),
        manager_first_name  varchar(45) NOT NULL,
        manager_last_name   varchar(45) NOT NULL,
        start_date          date NOT NULL,
        end_date            date NOT NULL
    )
    """)

    cur.close()


def create_fact_table(con):
    """
    TODO: Create the factSales table with the fields and data types shown in the ERD above.

    then run the following query to check your work:
    %%sql
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name   = 'factSales'

    SHOULD MATCH:
    column_name	data_type
    sales_key	integer
    date_key	integer
    customer_key	integer
    movie_key	integer
    store_key	integer
    sales_amount	numeric
    """
    cursor = con.cursor()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS factSales
        (
            sales_key       SERIAL PRIMARY KEY,
            date_key        integer REFERENCES dimDate (date_key),
            customer_key    integer REFERENCES dimCustomer(customer_key),
            movie_key       integer REFERENCES dimMovie(movie_key),
            store_key       integer REFERENCES dimStore(store_key),
            sales_amount    numeric
        )
        
    """
    cursor.execute(create_table_query)

    check_query = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name   = 'factsales'
    """
    cursor.execute(check_query)
    print(f"""

        Step 4.b Create the fact table
    
    """)
    for r in cursor.fetchall():
        print(r)

    cursor.close()


def populate_date_dimension_table(con):
    """ Code to populate dimension/fact tables that may have been provided by Udacity """
    cur = con.cursor()

    pop_query = """
    INSERT INTO dimDate (date_key, date, year, quarter, month, day, week, is_weekend)
    SELECT DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,
        date(payment_date)                                           AS date,
        EXTRACT(year FROM payment_date)                              AS year,
        EXTRACT(quarter FROM payment_date)                           AS quarter,
        EXTRACT(month FROM payment_date)                             AS month,
        EXTRACT(day FROM payment_date)                               AS day,
        EXTRACT(week FROM payment_date)                              AS week,
        CASE WHEN EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN true ELSE false END AS is_weekend
    FROM payment
    """
    cur.execute(pop_query)

    print("""

        Populate the Date Dimension Table:
        
    """)
    cur.execute('SELECT * FROM dimDate LIMIT 10')
    res = cur.fetchall()
    for r in res:
        print(r)

    cur.close()


def populate_customer_dimension_table(con):
    """
    TODO: Now it's your turn. Populate the dimCustomer table with data from the customer,
    address, city, and country tables. Use the starter code as a guide.
    """
    cur = con.cursor()

    pop_query = """
    INSERT INTO dimCustomer (
        customer_id,
        first_name,
        last_name,
        email,
        address, 
        address2,
        district,
        city,
        country,
        postal_code,
        phone,
        active, 
        create_date,
        start_date,
        end_date
    )
    SELECT
        c.customer_id                                           AS      customer_id,
        c.first_name                                            AS      first_name,
        c.last_name                                             AS      last_name,
        c.email                                                 AS      email,
        a.address                                               AS      address,
        a.address2                                              AS      address2,
        a.district                                              AS      district,
        ci.city                                                 AS      city,
        co.country                                              AS      country,
        a.postal_code                                           AS      postal_code,
        a.phone                                                 AS      phone,
        CASE WHEN c.activebool = false THEN 0 ELSE 1 END        AS      active,
        c.create_date                                           AS      create_date,
        now()                                                   AS      start_date,
        now()                                                   AS      end_date 
    FROM customer c
    JOIN address a  ON (c.address_id = a.address_id)
    JOIN city ci    ON (a.city_id = ci.city_id)
    JOIN country co ON (ci.country_id = co.country_id);
    """
    cur.execute(pop_query)

    print("""

        Populate the Customer Dimension Table:
        
    """)
    cur.execute('SELECT * FROM dimCustomer LIMIT 10')
    res = cur.fetchall()
    for r in res:
        print(r)

    cur.close()


def populate_movie_dimension_table(con):
    """
    TODO: Populate the dimMovie table with data from the film and language tables.
    Use the starter code as a guide.
    """
    cur = con.cursor()

    pop_query = """
    INSERT INTO dimMovie (
        film_id,
        title,
        description,
        release_year,
        language,
        original_language,
        rental_duration,
        length,
        rating,
        special_features
    )
    SELECT
        f.film_id           AS      film,
        f.title             AS      title,
        f.description       AS      description,
        f.release_year      AS      release_year,
        l.name              AS      language,
        orig_lang.name      AS      original_language,
        f.rental_duration   AS      rental_duration,
        f.length            AS      length,
        f.rating            AS      rating,
        f.special_features  AS      special_features
    FROM film f
    JOIN language l              ON (f.language_id=l.language_id)
    LEFT JOIN language orig_lang ON (f.original_language_id = orig_lang.language_id);
    """
    cur.execute(pop_query)

    print("""

        Populate the Movie Dimension Table:
        
    """)
    cur.execute('SELECT * FROM dimMovie LIMIT 10')
    res = cur.fetchall()
    for r in res:
        print(r)

    cur.close()


def populate_store_dimension_table(con):
    """
    TODO: Populate the dimStore table with data from the store, staff, address, city, and country tables.
    This time, there's no guide. You should write the query from scratch. Use the previous queries as a reference..
    """
    cur = con.cursor()

    pop_query = """
    INSERT INTO dimStore (
        store_id,
        address,
        address2,
        district,
        city,
        country,
        postal_code,
        manager_first_name,
        manager_last_name,
        start_date,
        end_date
    )
    SELECT
        s.store_id              AS      store_id,
        a.address               AS      address,
        a.address2              AS      address2,
        a.district              AS      district,
        ci.city                 AS      city,
        co.country              AS      country,
        a.postal_code           AS      postal_code,
        st.first_name           AS      manager_first_name,
        st.last_name            AS      manager_last_name,
        now()                   AS      start_date,
        now()                   AS      end_date
    FROM store s
    JOIN staff st   ON  s.manager_staff_id = st.staff_id
    JOIN address a  ON (s.address_id = a.address_id)
    JOIN city ci    ON (a.city_id = ci.city_id)
    JOIN country co ON (ci.country_id = co.country_id);
    """
    cur.execute(pop_query)

    print("""

        Populate the Store Dimension Table:
        
    """)
    cur.execute('SELECT * FROM dimStore LIMIT 10')
    res = cur.fetchall()
    for r in res:
        print(r)

    cur.close()


def populate_sales_fact_table(con):
    """
    TODO: Populate the factSales table with data from the payment, rental, and inventory tables.
    This time, there's no guide. You should write the query from scratch. Use the previous queries as a reference.
    """
    cur = con.cursor()

    pop_query = """
    INSERT INTO factSales (
        date_key,
        customer_key,
        movie_key,
        store_key,
        sales_amount
    )
    SELECT
        TO_CHAR(p.payment_date :: DATE, 'yyyyMMDD')::integer    AS date_key,
        dc.customer_key                                         AS customer_key,
        dm.movie_key                                            AS movie_key,
        ds.store_key                                            AS store_key,
        p.amount                                                AS sales_amount
        
    FROM payment p
    JOIN rental r           ON      (p.rental_id = r.rental_id)
    JOIN inventory i        ON      (r.inventory_id = i.inventory_id)
    JOIN dimCustomer dc     ON      p.customer_id = dc.customer_id
    JOIN dimStore ds        ON      i.store_id = ds.store_id
    JOIN dimMovie dm        ON      i.film_id = dm.film_id;
    """
    cur.execute(pop_query)

    print("""

        Populate the Sales Fact Table:

    """)
    cur.execute('SELECT * FROM factSales LIMIT 10')
    res = cur.fetchall()
    for r in res:
        print(r)

    cur.close()


def exercise1():
    """ Runs the commands in exercise 1"""
    PGUtils.reset_databases()
    PGUtils.load_pagila()

    # Connect to database
    con = PGUtils.connect('pagila')
    con.set_session(autocommit=True)

    # Print Counts
    print_counts(con)

    # Print date range
    print_date_range(con)

    # TODO: Addresses by District
    addresses_by_district(con)

    # TODO: Movie Rental Revenue
    sum_movie_rental_revenue(con)

    # TODO: Top Grossing Cities
    top_grossing_cities(con)

    # TODO: Revenue of each movie by customer city and month
    revenue_by_customer_city_month(con)

    # TODO: Create first dimension table
    create_first_dimension_table(con)
    create_other_dimension_tables(con)

    # TODO: Create the fact table
    create_fact_table(con)

    populate_date_dimension_table(con)

    # TODO: Populate Customer Dimension Table
    populate_customer_dimension_table(con)

    # TODO: Populate the Movie Dimension Table
    populate_movie_dimension_table(con)

    # TODO: Populate the Store Dimension Table
    populate_store_dimension_table(con)

    # TODO: Populate the Sales Fact Table
    populate_sales_fact_table(con)

    # Cleanup Databases
    con.close()
    PGUtils.reset_databases()


if __name__ == '__main__':
    exercise1()
