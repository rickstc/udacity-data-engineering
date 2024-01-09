from helpers.db_conn import DBHelpers
import pandas as pd
import os
from sqlalchemy.sql import text


def combine_columns(row):
    """
    This combines the country, state, and town fields into a single string
    in the following format: country.state.town

    This will be used for deduplication purposes, as records with matching
    values for this column will be considered identical.

    This is not an ideal solution given the data, as not all of the entries
    contain each of these columns. There is potential for merging records
    that shouldn't be merged. Consider the following:

    United States..Portland - could refer to Portland Maine or Portland Oregon

    However, the purpose of this project is to demonstrate the ability to
    create a data pipeline and understand and account for data quality issues.

    The student is willing to accept the potential for inaccurate data in this
    context.
    """
    return f"{row['country']}.{row['state']}.{row['town']}"


def load_fact_location(file_path, table_name, location_fp, **kwargs):
    """
    This loads data into the 'fact_location' table. This corresponds to
    the 'fact.Location' model in the dashboard application.
    """
    print(f"Loading table: {table_name}")

    location_frames = []

    # Ensure the CSV file containing the data exists
    print(f"Looking for CSV at: {file_path}")
    if not os.path.exists(file_path):
        raise ValueError(f"The file_path provided does not exist!")

    # Load the PL data from the csv
    for df in pd.read_csv(file_path, chunksize=5000):
        # Create a locations data frame
        location_frames.append(df.loc[:, ["MeetCountry", "MeetState", "MeetTown"]])

    # Get a dataframe from the list of frames
    df = pd.concat(location_frames)

    # Rename fields
    df = df.rename(
        columns={"MeetCountry": "country", "MeetState": "state", "MeetTown": "town"}
    )

    # Fill in Missing Towns and States with Blank Values
    df.town.fillna(value="", inplace=True)
    df.state.fillna(value="", inplace=True)

    # Create a deduplicating column by calling the combine_columns function
    df["dedup"] = df.apply(combine_columns, axis=1)

    # Deduplicate Columns
    df = df.drop_duplicates("dedup")

    # Drop deduplicating column
    df = df.drop("dedup", axis=1)

    # Create another key for deduplicating against the location data
    df["cc_key"] = df["town"] + "." + df["country"]

    # Load the list of locations into a new dataframe and create a column
    # to use for deduplicating with the locationd data from the powerlifting
    # data
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

    # Clean some additional data
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

    """
    This SQL statement populates the table. In other instances, the student
    opted to use the pandas dataframe 'to_sql' method, however, it did not
    play nicely with the geospatial fields without additioanl configuration
    or libraries such as https://geopandas.org/en/stable/.

    Rather than bring in another dependency, it seemed relevant to demonstrate
    the abilty to work with SQL directly.

    Of particular interest in the following statement is the use of the
    ST_PointFromText function, which inserts a Point field from a given
    latitude and longitude.
    """
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

    # Open a connection to the database
    with engine.begin() as conn:
        # Base statement for insert into `fact_location` table
        for record in merged_df.to_dict("records"):
            inserts.append(record)

            # Execute inserts in batches of 500 and then reset the inserts list
            if len(inserts) % 500 == 0:
                print(f"Inserting {len(inserts)} into the database.")
                conn.execute(base_statement, inserts)
                inserts = []

        # Insert any remaining records after all lines have been processed
        print(f"Inserting {len(inserts)} into the database.")
        conn.execute(base_statement, inserts)
