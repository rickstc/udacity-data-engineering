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


def remove_table(table_name):
    db = create_engine("postgresql://student:student@127.0.0.1/studentdb")
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db)
    session = SessionLocal()

    sql_query = f"DELETE FROM {table_name};"

    results = session.execute(text(sql_query))
    session.close()


# Define the parsing function
def parse_line(line):
    return [
        line[0:11],
        float(line[31:37]),
        line[38:40].strip(),
        line[41:71].strip(),
        line[72:75],
        line[80:85],
    ]


def handle_stations():
    # Read the data from the text file
    lines = []
    with open("weather/stations.txt") as f:
        for line in f:
            lines.append(parse_line(line.strip()))

    # Create a pandas dataframe
    df = pd.DataFrame(
        lines,
        columns=[
            "station_id",
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

    # Print the dataframe
    print(df)

    # Insert stations into database
    insert_frame(df, "weather_station")


def start():
    dir = "weather"

    # Insert Weather Stations

    handle_stations()
    stations = load_table("weather_station")

    print(stations)


if __name__ == "__main__":
    start()
