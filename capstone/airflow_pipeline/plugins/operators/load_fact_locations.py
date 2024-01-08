from helpers.db_conn import DBHelpers
import pandas as pd
import os
from sqlalchemy.sql import text


def combine_columns(row):
    return f"{row['country']}.{row['state']}.{row['town']}"


def load_fact_location(file_path, table_name, location_fp, **kwargs):
    print(f"Loading table: {table_name}")
    # Remove data in the table if it exists
    connection = DBHelpers.connect()
    DBHelpers.clear_table(connection, table_name)
    connection.close()

    location_frames = []

    print(f"Looking for CSV at: {file_path}")
    if not os.path.exists(file_path):
        raise ValueError(f"The file_path provided does not exist!")

    # Load the PL data from the csv
    for df in pd.read_csv(file_path, chunksize=5000):
        # Create a locations data frame
        location_frames.append(df.loc[:, ["MeetCountry", "MeetState", "MeetTown"]])

    df = pd.concat(location_frames)

    # Rename fields
    df = df.rename(
        columns={"MeetCountry": "country", "MeetState": "state", "MeetTown": "town"}
    )

    # Fill in Missing Towns and States with Blank Values
    df.town.fillna(value="", inplace=True)
    df.state.fillna(value="", inplace=True)

    # Create a deduplicating column
    df["dedup"] = df.apply(combine_columns, axis=1)

    # Deduplicate Columns
    df = df.drop_duplicates("dedup")

    # Drop deduplicating column
    df = df.drop("dedup", axis=1)

    df["cc_key"] = df["town"] + "." + df["country"]

    # cities_list = load_csv("location/cities.csv")
    city_df = pd.read_csv(location_fp)

    city_df["cc_key"] = city_df["city"] + "." + city_df["country"]

    # Merge dataframes based on country and matching criteria
    merged_df = pd.merge(
        df,
        city_df,
        on="cc_key",
        how="left",
    )

    # Drop unnecessary columns
    merged_df = merged_df.drop("country_y", axis=1)
    merged_df = merged_df.drop("cc_key", axis=1)
    merged_df = merged_df.drop("city", axis=1)
    merged_df.lat.fillna(value=0, inplace=True)
    merged_df.lng.fillna(value=0, inplace=True)
    merged_df.population.fillna(value=0, inplace=True)

    merged_df = merged_df.rename(columns={"country_x": "country"})

    # Create a deduplicating column
    merged_df["dedup"] = merged_df.apply(combine_columns, axis=1)

    # Deduplicate Columns
    merged_df = merged_df.drop_duplicates("dedup")

    # Connect to the database and get a connection object
    engine = DBHelpers.get_engine()

    base_statement = text(
        f"""
            INSERT INTO {table_name} (
                town,
                country,
                state,
                population,
                location
            ) VALUES (
                :town,
                :country,
                :state,
                :population,
                ST_PointFromText('POINT(:lat :lng)', 4326)
            )
        """
    )

    inserts = []

    with engine.begin() as conn:
        # Base statement for insert into `location_station` table
        for record in merged_df.to_dict("records"):
            inserts.append(record)
            if len(inserts) % 500 == 0:
                print(f"Inserting {len(inserts)} into the database.")
                conn.execute(base_statement, inserts)
                inserts = []

        print(f"Inserting {len(inserts)} into the database.")

        conn.execute(base_statement, inserts)
