import glob
import csv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import re
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text


def insert_frame(df, table_name):
    db = create_engine("postgresql://student:student@127.0.0.1/studentdb")
    conn = db.connect()

    # Insert into Database
    df.to_sql(con=conn, name=table_name, index=False, if_exists="append")

    conn.close()


def load_table(table_name):
    db = create_engine("postgresql://student:student@127.0.0.1/studentdb")
    conn = db.connect()

    sql_query = f"SELECT * FROM {table_name};"

    df = pd.read_sql(sql_query, conn)

    return df


# Define the parsing function
def parse_line(line):
    return [
        line[0:11],
        float(line[12:20]),
        float(line[21:30]),
        float(line[31:37]),
        line[38:40].strip(),
        line[41:71].strip(),
        line[72:75],
        line[80:85],
    ]


def handle_stations():
    # Read the data from the text file
    lines = []
    with open("location/stations.txt") as f:
        for line in f:
            lines.append(parse_line(line.strip()))

    # Create a pandas dataframe
    df = pd.DataFrame(
        lines,
        columns=[
            "station_id",
            "lat",
            "lng",
            "elevation",
            "state",
            "name",
            "gsn_flag",
            "wmo_id",
        ],
    )

    # Convert gsn_flag to boolean
    # Replace empty strings with False
    df["gsn_flag"] = df["gsn_flag"].replace("", False)

    # Fill NaN values with False
    df["gsn_flag"] = df["gsn_flag"].fillna(False)

    # Convert all values to boolean
    df["gsn_flag"] = df["gsn_flag"].astype(bool)

    # Replace empty strings in wmo_id
    df["wmo_id"] = df["wmo_id"].replace("", 0)

    # Insert stations into database
    insert_frame(df, "location_station")


def handle_locations():
    df = pd.read_csv("location/cities.csv")
    insert_frame(df, "location_citylocation")


def start():
    dir = "location"

    handle_stations()

    handle_locations()


if __name__ == "__main__":
    start()
