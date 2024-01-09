from helpers.db_conn import DBHelpers
import pandas as pd
import os


def load_fact_contest(file_path, table_name, location_table_name, **kwargs):
    """
    This loads data into the 'fact_contest' table. This corresponds to the
    'fact.Contest' model in the dashboard application.
    """
    print(f"Loading table: {table_name}")
    # Remove data in the table if it exists
    connection = DBHelpers.connect()
    DBHelpers.clear_table(connection, table_name)
    connection.close()

    contest_frames = []

    print(f"Looking for CSV at: {file_path}")
    if not os.path.exists(file_path):
        raise ValueError(f"The file_path provided does not exist!")

    # Load the PL data from the csv
    for df in pd.read_csv(file_path, chunksize=5000):
        # Create an athletes data frame
        contest_frames.append(
            df.loc[
                :,
                [
                    "MeetName",
                    "Federation",
                    "ParentFederation",
                    "Date",
                    "MeetCountry",
                    "MeetState",
                    "MeetTown",
                ],
            ]
        )

    # Get a data frame from the list of frames
    df = pd.concat(contest_frames)

    # Rename columns to match the database column names
    df = df.rename(
        columns={
            "MeetCountry": "country",
            "MeetState": "state",
            "MeetTown": "town",
            "MeetName": "name",
            "Federation": "federation",
            "ParentFederation": "parent_federation",
            "Date": "date",
        }
    )

    # Deduplicate
    df = df.drop_duplicates(["name", "date"])

    # Fill in Missing Towns and States with Blank Values
    df.town.fillna(value="", inplace=True)
    df.state.fillna(value="", inplace=True)

    # Rename locations
    engine = DBHelpers.get_engine()
    locations_df = DBHelpers.retrieve_table_df(engine, location_table_name)
    locations_df = locations_df.rename(columns={"id": "location_id"})

    df = df.merge(locations_df, on=["country", "town", "state"], how="left")

    # Remove unnecessary fields post merge
    df = df.drop(["country", "state", "town", "location", "population"], axis=1)

    # Fill in missing values
    df.federation.fillna(value="", inplace=True)
    df.parent_federation.fillna(value="", inplace=True)

    # Insert the data frame into the database
    DBHelpers.insert_table_df(engine, table_name, df)
    return True
