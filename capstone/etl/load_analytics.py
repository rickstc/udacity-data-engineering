import glob
import csv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import re
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text


def build_contests():
    db = create_engine("postgresql://student:student@127.0.0.1/studentdb")
    conn = db.connect()

    sql_query = """
        SELECT
            plc.name as name,
            plc.date as date,
            pll.country as country,
            pll.town as city
        FROM powerlifting_contest plc
        JOIN powerlifting_contestlocation pll
        ON plc.location_id = pll.id;
        """

    df = pd.read_sql(sql_query, conn)

    print(df)


def build_locations():
    connection = psycopg2.connect(
        dbname="studentdb",
        host="127.0.0.1",
        user="student",
        password="student",
    )
    connection.set_session(autocommit=True)

    cursor = connection.cursor()

    cursor.execute(f"DELETE FROM dimension_location;")

    sql_query = """
        INSERT INTO dimension_location (
            country,
            state,
            town,
            weather_station,
            distance_to_station,
            elevation,
            population
        )
        SELECT
            fcl.country,
            fcl.state,
            fcl.town,
            ws.station_name AS weather_station,
            ST_Distance(fcl.location, ws.station_location) / 1000 AS distance_to_station,
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

    cursor.execute(sql_query)

    cursor.close()

    connection.close()


def start():
    # contests = build_contests()
    locations = build_locations()


if __name__ == "__main__":
    start()
