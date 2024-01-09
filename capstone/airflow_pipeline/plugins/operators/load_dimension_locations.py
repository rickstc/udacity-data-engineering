from helpers.db_conn import DBHelpers
from sqlalchemy.sql import text


def load_dimension_location(table_name, **kwargs):
    """
    This loads data into the 'dimension_location' table. This corresponds to
    the 'dimension.Location' model in the dashboard application.
    """
    print(f"Loading table: {table_name}")

    # Connect to the database and get a connection object
    engine = DBHelpers.get_engine()

    """
    This SQL statement populates the dimension table by retrieving data from
    the fact tables. Of particular interest, the dimension location pulls data
    from the fact location table, but also calculates and stores the nearest
    weather station.

    This leverages the ST_Distance functionality of Postgres with the
    geo spatial extensions loaded; often referred to as PostGIS.

    The distance to the weather station is stored as kilometers.

    The student was able to determine the 'nearest' weather station by
    comparing the coordinates stored in the contest location fact table with
    the coordinates stored in the weather station table using the "CROSS JOIN
    LATERAL" statement.

    The CROSS JOIN performs a cartesian product, joining every single row of
    one table with every single row of another.

    The LATERAL subquery embeds a subquery into the FROM clause while still
    being able to access columns from the preceding tables.

    This query did take a little while to run locally, albeit in a virtualized,
    low performance environment. There is a possibility that queries such as
    this may need to be batched for larger datasets.
    """
    base_statement = text(
        """
        INSERT INTO dimension_location (
            fact_location_id,
            country,
            state,
            town,
            loc_coords,
            weather_station,
            station_coords,
            distance_to_station,
            elevation,
            population
        )
        SELECT fcl.id,
            fcl.country,
            fcl.state,
            fcl.town,
            fcl.location,
            ws.station_name AS weather_station,
            ws.station_location AS station_coords,
            ST_Distance(
                fcl.location::geography,
                ws.station_location::geography
            ) / 1000 AS distance_to_station,
            ws.elevation,
            fcl.population
        FROM fact_contestlocation AS fcl
            CROSS JOIN LATERAL (
                SELECT *
                FROM fact_weatherstation AS ws
                ORDER BY ST_Distance(fcl.location, ws.station_location) ASC
                LIMIT 1
            ) AS ws;
        """
    )

    with engine.begin() as conn:
        conn.execute(base_statement)
