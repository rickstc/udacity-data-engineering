from helpers.db_conn import DBHelpers
import pandas as pd
import os


def load_fact_athlete(file_path, table_name, **kwargs):
    print(f"Loading table: {table_name}")
    # Remove data in the table if it exists
    connection = DBHelpers.connect()
    DBHelpers.clear_table(connection, table_name)
    connection.close()

    athletes_frames = []

    print(f"Looking for CSV at: {file_path}")
    if not os.path.exists(file_path):
        raise ValueError(f"The file_path provided does not exist!")

    # Load the PL data from the csv
    for df in pd.read_csv(file_path, chunksize=5000):
        # Create an athletes data frame
        athletes_frames.append(df.loc[:, ["Name", "Sex"]])

    athletes_df = pd.concat(athletes_frames)

    # 'Name' column is deduplicated via # symbol, so, we can drop based on name
    athletes_df = athletes_df.drop_duplicates("Name")

    # Rename Name to name and Sex to gender to conform with database fields
    athletes_df = athletes_df.rename(columns={"Name": "name", "Sex": "gender"})

    # Split "name" column into "name" and "deduplication_number"
    try:
        athletes_df[["name", "deduplication_number"]] = athletes_df.name.str.split(
            " #", expand=True
        )
    except:
        athletes_df["deduplication_number"] = 0

    # Transform 'gender' field, replacing "Mx" with "X", to conform with API Schema
    athletes_df["gender"].replace("Mx", "X", inplace=True)

    # If there are 'deduplication_number' fields with no value, replace with 0
    athletes_df.deduplication_number.fillna(value=0, inplace=True)

    engine = DBHelpers.get_engine()
    DBHelpers.insert_table_df(engine, table_name, athletes_df)
    return True
