import exercise1
from utils import PGUtils


def prepare_star_schema_table(connection):
    """
    Leverages functions from Exercise 1 to Prepare the Database for Queries
    """
    exercise1.create_first_dimension_table(connection)
    exercise1.create_other_dimension_tables(connection)
    exercise1.create_fact_table(connection)
    exercise1.populate_date_dimension_table(connection)
    exercise1.populate_customer_dimension_table(connection)
    exercise1.populate_movie_dimension_table(connection)
    exercise1.populate_store_dimension_table(connection)
    exercise1.populate_sales_fact_table(connection)


def revenue_cube_day_rating_city(connection):
    """
    TODO: Write a query that calculates the revenue (sales_amount) by day, rating, and city.
    Remember to join with the appropriate dimension tables to replace the keys with the dimension labels.
    Sort by revenue in descending order and limit to the first 20 rows.
    The first few rows of your output should match the table below.

    day	rating	city	        revenue
    30	G	    San Bernardino	24.97
    30	NC-17	Apeldoorn	    23.95
    21	NC-17	Belm	        22.97
    30	PG-13	Zanzibar	    21.97
    28	R	    Mwanza	        21.97

    """

    query = """
        SELECT d.day, m.rating, c.city, SUM(f.sales_amount) as revenue
        FROM dimDate d
        JOIN factSales f ON d.date_key = f.date_key
        JOIN dimCustomer c ON f.customer_key = c.customer_key
        JOIN dimMovie m ON f.movie_key = m.movie_key
        GROUP BY d.day, m.rating, c.city
        ORDER BY revenue DESC
        LIMIT 20
    """

    cursor = connection.cursor()
    cursor.execute(query)
    print(f"""

        Revenue by day, rating, city:
    
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def slice_by_rating(connection):
    """
    TODO: Write a query that reduces the dimensionality of the above example by limiting the results to only
    include movies with a rating of "PG-13". Again, sort by revenue in descending order and limit to the
    first 20 rows. The first few rows of your output should match the table below.

    day	rating	city	    revenue
    30	PG-13	Zanzibar	21.97
    28	PG-13	Dhaka	    19.97
    29	PG-13	Shimoga	    18.97
    30	PG-13	Osmaniye	18.97
    21	PG-13	Asuncin	    18.95

    """

    query = """
        SELECT d.day, m.rating, c.city, SUM(f.sales_amount) as revenue
        FROM dimDate d
        JOIN factSales f ON d.date_key = f.date_key
        JOIN dimCustomer c ON f.customer_key = c.customer_key
        JOIN dimMovie m ON f.movie_key = m.movie_key
        WHERE m.rating = 'PG-13'
        GROUP BY d.day, m.rating, c.city
        ORDER BY revenue DESC
        LIMIT 20
    """

    cursor = connection.cursor()
    cursor.execute(query)
    print(f"""

        Slice the cube by rating
    
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def dice_the_cube(connection):
    """
    TODO: Write a query to create a subcube of the initial cube that includes moves with:

    ratings of PG or PG-13
    in the city of Bellevue or Lancaster
    day equal to 1, 15, or 30

    day	rating	city	    revenue
    30	PG	    Lancaster	12.98
    1	PG-13	Lancaster	5.99
    30	PG-13	Bellevue	3.99
    30	PG-13	Lancaster	2.99
    15	PG-13	Bellevue	1.98

    """

    query = """
        SELECT d.day, m.rating, c.city, SUM(f.sales_amount) as revenue
        FROM dimDate d
        JOIN factSales f ON d.date_key = f.date_key
        JOIN dimCustomer c ON f.customer_key = c.customer_key
        JOIN dimMovie m ON f.movie_key = m.movie_key
        WHERE m.rating IN ('PG-13', 'PG')
        AND d.day IN (1, 15, 30)
        AND c.city IN ('Lancaster', 'Bellevue')
        GROUP BY d.day, m.rating, c.city
        ORDER BY revenue DESC
        LIMIT 20
    """

    cursor = connection.cursor()
    cursor.execute(query)
    print(f"""

        Dice up the cube
    
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def exercise2():
    """ Functionality to accomplish exercise 2 """

    # Lesson 1 populated the dimension table that we will be using
    # We can leverage what we've done previously to keep moving forward
    PGUtils.reset_databases()
    PGUtils.load_pagila()
    con = PGUtils.connect('pagila')
    con.set_session(autocommit=True)

    # Load the dimension and fact tables
    prepare_star_schema_table(con)

    # TODO: Calculate Revenue by day, rating, city
    revenue_cube_day_rating_city(con)

    # TODO: Slice by Rating
    slice_by_rating(con)

    # TODO: Dice the cube
    dice_the_cube(con)

    con.close()
    PGUtils.reset_databases()


if __name__ == '__main__':
    exercise2()
