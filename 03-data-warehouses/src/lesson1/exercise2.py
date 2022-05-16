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


def roll_up_day_rating_country(connection):
    """
    TODO: Write a query that calculates revenue (sales_amount) by day, rating, and country. Sort the data by revenue
    in descending order, and limit the data to the top 20 results. The first few rows of your output should match the table below.

    day	rating	country	revenue
    30	G	    China	169.67
    30	PG	    India	156.67
    30	NC-17	India	153.64
    30	PG-13	China	146.67
    30	R	    China	145.66

    """
    query = """
        SELECT d.day, m.rating, c.country, SUM(f.sales_amount) as revenue
        FROM dimDate d
        JOIN factSales f ON d.date_key = f.date_key
        JOIN dimCustomer c ON f.customer_key = c.customer_key
        JOIN dimMovie m ON f.movie_key = m.movie_key
        GROUP BY d.day, m.rating, c.country
        ORDER BY revenue DESC
        LIMIT 20
    """

    cursor = connection.cursor()
    cursor.execute(query)
    print(f"""

        Roll-Up
    
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def drill_down_day_rating_district(connection):
    """
    TODO: Write a query that calculates revenue (sales_amount) by day, rating, and district.
    Sort the data by revenue in descending order, and limit the data to the top 20 results.
    The first few rows of your output should match the table below.

    day	rating	district	        revenue
    30	PG-13	Southern Tagalog	53.88
    30	G	    Inner Mongolia	    38.93
    30	G	    Shandong	        36.93
    30	NC-17	West Bengali	    36.92
    17	PG-13	Shandong	        34.95

    """
    query = """
        SELECT d.day, m.rating, c.district, SUM(f.sales_amount) as revenue
        FROM dimDate d
        JOIN factSales f ON d.date_key = f.date_key
        JOIN dimCustomer c ON f.customer_key = c.customer_key
        JOIN dimMovie m ON f.movie_key = m.movie_key
        GROUP BY d.day, m.rating, c.district
        ORDER BY revenue DESC
        LIMIT 20
    """

    cursor = connection.cursor()
    cursor.execute(query)
    print(f"""

        Drill Down
    
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def total_revenue(connection):
    """
    TODO: Write a query that calculates total revenue (sales_amount)
    """
    query = """
        SELECT SUM(sales_amount) as revenue
        FROM factSales;
    """

    cursor = connection.cursor()
    cursor.execute(query)
    print(f"""

        Total Revenue:
    
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def revenue_by_country(connection):
    """
    TODO: Write a query that calculates total revenue (sales_amount) by country
    """
    query = """
        SELECT s.country, SUM(f.sales_amount) as revenue
        FROM factSales f
        JOIN dimStore s ON f.store_key = s.store_key
        GROUP BY (s.country)
        LIMIT 20;
    """

    cursor = connection.cursor()
    cursor.execute(query)
    print(f"""

        Revenue by Country:
    
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def revenue_by_month(connection):
    """
    TODO: Write a query that calculates total revenue (sales_amount) by month
    """
    query = """
        SELECT d.month, SUM(f.sales_amount) as revenue
        FROM factSales f
        JOIN dimDate d ON f.date_key = d.date_key
        GROUP BY (d.month);
    """

    cursor = connection.cursor()
    cursor.execute(query)
    print(f"""

        Revenue by Month
    
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def revenue_by_month_and_country(connection):
    """
    ???
    TODO: Write a query that calculates total revenue (sales_amount) by month and country.
    Sort the data by month, country, and revenue in descending order.
    The first few rows of your output should match the table below.

    month	country	    revenue
    1	    Australia	2364.19
    1	    Canada	    2460.24
    2	    Australia	4895.10
    2	    Canada	    4736.78
    3	    Australia	12060.33
    """
    query = """
        SELECT d.month, s.country, SUM(f.sales_amount) as revenue
        FROM factSales f
        JOIN dimDate d ON f.date_key = d.date_key
        JOIN dimStore s ON f.store_key = s.store_key
        GROUP BY d.month, s.country
        ORDER BY d.month ASC, s.country ASC
        LIMIT 10;
    """

    cursor = connection.cursor()
    cursor.execute(query)
    print(f"""

        Revenue by Month and Country
    
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def revenue_total_month_country(connection):
    """
    ???
    TODO: Write a query that calculates total revenue at the various grouping levels
    done above (total, by month, by country, by month & country) all at once using
    the grouping sets function. Your output should match the table below.

    month	country	    revenue
    1	    Australia	2364.19
    1	    Canada	    2460.24
    1	    None	    4824.43
    2	    Australia	4895.10
    2	    Canada	    4736.78
    2	    None	    9631.88
    3	    Australia	12060.33
    3	    Canada	    11826.23
    3	    None	    23886.56
    4	    Australia	14136.07
    4	    Canada	    14423.39
    4	    None	    28559.46
    5	    Australia	271.08
    5	    Canada	    243.10
    5	    None	    514.18
    None	None	    67416.51
    None	Australia	33726.77
    None	Canada	    33689.74
    """
    query = """
        SELECT d.month, s.country, SUM(f.sales_amount) as revenue
        FROM factSales f
        JOIN dimDate d ON f.date_key = d.date_key
        JOIN dimStore s ON f.store_key = s.store_key
        GROUP BY GROUPING SETS ((), d.month, s.country, (d.month, s.country))
        ORDER BY d.month ASC, s.country ASC
    """

    cursor = connection.cursor()
    cursor.execute(query)
    print(f"""

        Revenue by Month and Country w/ Subtotals
    
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()


def group_by_cube(connection):
    """
    TODO: Write a query that calculates the various levels of aggregation done in the grouping sets exercise
    (total, by month, by country, by month & country) using the CUBE function.
    Your output should match the table below.


    month	country	    revenue
    1	    Australia	2364.19
    1	    Canada	    2460.24
    1	    None	    4824.43
    2	    Australia	4895.10
    2	    Canada	    4736.78
    2	    None	    9631.88
    3	    Australia	12060.33
    3	    Canada	    11826.23
    3	    None	    23886.56
    4	    Australia	14136.07
    4	    Canada	    14423.39
    4	    None	    28559.46
    5	    Australia	271.08
    5	    Canada	    243.10
    5	    None	    514.18
    None	None	    67416.51
    None	Australia	33726.77
    None	Canada	    33689.74
    """
    query = """
        SELECT d.month, s.country, SUM(f.sales_amount) as revenue
        FROM factSales f
        JOIN dimDate d ON f.date_key = d.date_key
        JOIN dimStore s ON f.store_key = s.store_key
        GROUP BY CUBE (s.country, d.month)
        ORDER BY d.month ASC, s.country ASC
    """

    cursor = connection.cursor()
    cursor.execute(query)
    print(f"""

        Revenue by Month and Country w/ Subtotals Produced by CUBE
    
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

    # TODO: Roll Up
    roll_up_day_rating_country(con)

    # TODO: Drill Down
    drill_down_day_rating_district(con)

    # TODO: Total Revenue
    total_revenue(con)

    # TODO: Revenue by Country
    revenue_by_country(con)

    # TODO: Revenue by Month
    revenue_by_month(con)

    # TODO: Revenue by Month and Country
    revenue_by_month_and_country(con)

    # TODO: Revenue by Month and Country w/ Totals
    revenue_total_month_country(con)

    # TODO: Group by CUBE
    group_by_cube(con)

    con.close()
    PGUtils.reset_databases()


if __name__ == '__main__':
    exercise2()
