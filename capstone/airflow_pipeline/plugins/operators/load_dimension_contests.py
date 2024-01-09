from helpers.db_conn import DBHelpers
from sqlalchemy.sql import text


def load_dimension_contest(table_name, **kwargs):
    """
    This loads data into the 'dimension_contest' table. This corresponds to
    the 'dimension.Contest' model in the dashboard application.
    """
    print(f"Loading table: {table_name}")

    # Connect to the database and get a connection object
    engine = DBHelpers.get_engine()

    """
    This SQL statement populates the dimension table by retrieving data from
    the fact tables. Of particular interest, the contest results are aggregated
    to calculate the average dots scores of all results, as well as the standard
    deviation.
    """
    base_statement = text(
        """
        INSERT INTO dimension_contest (
            fact_contest_id,
            date,
            name,
            federation,
            average_dots,
            standard_deviation,
            location_id
        )
        SELECT
            fc.id,
            fc.date,
            fc.name,
            fc.federation,
            AVG(fcr.dots) as average_dots,
            STDDEV_POP(fcr.dots) AS standard_deviation,
            dl.id AS location_id
        FROM fact_contest AS fc
        JOIN dimension_location dl
        ON fc.location_id=dl.fact_location_id
        JOIN fact_contestresult fcr
        ON fc.id = fcr.contest_id
        GROUP BY fc.id, dl.id;
        """
    )

    with engine.begin() as conn:
        conn.execute(base_statement)
