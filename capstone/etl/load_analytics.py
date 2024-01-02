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
    db = create_engine("postgresql://student:student@127.0.0.1/studentdb")
    conn = db.connect()

    sql_query = """
        SELECT
            c.city, c.country, c.lat, c.lng,
            s.name AS station_name,
            ST_DistanceSphere(
                ST_GeogPoint(c.lng, c.lat),
                ST_GeogPoint(s.lng, s.lat)
            ) / 1000 AS distance_km
        FROM location_citylocation AS c
        JOIN location_station AS s
        ON ST_DWithin(
        ST_GeogPoint(c.lng, c.lat),
        ST_GeogPoint(s.lng, s.lat),
        (
            SELECT MIN(ST_DistanceSphere(
            ST_GeogPoint(c.lng, c.lat),
            ST_GeogPoint(s2.lng, s2.lat)
            ))
            FROM location_station AS s2
        )
        );
        """

    sql_query2 = """
    SELECT * FROM location_citylocation AS c;
    """

    df = pd.read_sql(sql_query2, conn)

    print(df)


def start():
    # contests = build_contests()
    locations = build_locations()


if __name__ == "__main__":
    start()
