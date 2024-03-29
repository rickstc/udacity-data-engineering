from helpers.db_conn import DBHelpers
import pandas as pd
import os


def lookup_contest_id(row, lookup):
    """This function accepts a data frame row and a dictionary lookup. It
    calculates a key and then returns the corresponding entry in the dictionary
    at that key."""
    key = f"{row['name']}.{row['date']}"
    return lookup[key]


def load_fact_result(
    file_path,
    table_name,
    location_table_name,
    athletes_table_name,
    contests_table_name,
    **kwargs,
):
    """
    This loads data into the 'fact_contestresult' table. This corresponds to
    the 'fact.ContestResult' model in the dashboard application.
    """
    print(f"Loading table: {table_name}")

    result_frames = []

    # Ensure the CSV file containing the data exists
    print(f"Looking for CSV at: {file_path}")
    if not os.path.exists(file_path):
        raise ValueError(f"The file_path provided does not exist!")

    # Load the PL data from the csv
    for df in pd.read_csv(file_path, chunksize=5000):
        # Create an athletes data frame
        result_frames.append(
            df.loc[
                :,
                [
                    "MeetName",
                    "Date",
                    "Name",
                    "Event",
                    "Equipment",
                    "Tested",
                    "Division",
                    "Age",
                    "AgeClass",
                    "BirthYearClass",
                    "BodyweightKg",
                    "WeightClassKg",
                    "Best3DeadliftKg",
                    "Best3SquatKg",
                    "Best3BenchKg",
                    "Place",
                    "TotalKg",
                    "Dots",
                ],
            ]
        )
    # Get a dataframe from the list of frames
    df = pd.concat(result_frames)

    # Connect to the database
    engine = DBHelpers.get_engine()

    # Pull locations into a new dataframe
    locations_df = DBHelpers.retrieve_table_df(engine, location_table_name)
    locations_df = locations_df.rename(columns={"id": "location_id"})

    # Pull athletes into a new dataframe
    athletes = DBHelpers.retrieve_table_df(engine, athletes_table_name)
    athletes = athletes.rename(columns={"id": "athlete_id"})

    # Pull contests into a new dataframe
    contests = DBHelpers.retrieve_table_df(engine, contests_table_name)
    contests = contests.rename(columns={"id": "contest_id"})

    """
    Build a lookup (dictionary) to assist in merging the contests dataframe
    with the results dataframe.

    This concatenates the contest's 'name' and 'date' to build a unique key
    that can be used to lookup the foreign key of the contest.

    The access time in a python dictionary is O1, so while this approach is not
    ideal, it shouldn't be a big drain on performance either.    
    """
    contest_id_lookup = {}
    for index, row in contests.iterrows():
        contest_id_lookup[f"{row['name']}.{row['date']}"] = row["contest_id"]

    df = df.rename(columns={"MeetName": "name", "Date": "date"})
    df["contest_id"] = df.apply(lookup_contest_id, axis=1, lookup=contest_id_lookup)
    df = df.drop(["name", "date"], axis=1)

    # Rename for athletes
    df = df.rename(columns={"Name": "name"})
    df[["name", "deduplication_number"]] = df.name.str.split(" #", expand=True)
    df.deduplication_number.fillna(value=0, inplace=True)

    # Merge Athletes
    df = df.merge(athletes, on=["name", "deduplication_number"], how="left")

    # Drop Merge Fields
    df = df.drop(["name", "deduplication_number", "gender"], axis=1)

    # Rename contest fields
    df = df.rename(
        columns={
            "Event": "event",
            "Equipment": "equipment",
            "Division": "division",
            "Age": "age",
            "AgeClass": "age_class",
            "BirthYearClass": "birth_year_class",
            "BodyweightKg": "bodyweight",
            "WeightClassKg": "weight_class",
            "Best3SquatKg": "squat",
            "Best3BenchKg": "bench_press",
            "Best3DeadliftKg": "deadlift",
            "Place": "place",
            "TotalKg": "meet_total",
            "Dots": "dots",
            "Tested": "drug_tested",
        }
    )

    # Normalize Event
    df["event"].replace("SBD", "FP", inplace=True)
    df["event"].replace("BD", "BD", inplace=True)
    df["event"].replace("SD", "SD", inplace=True)
    df["event"].replace("SB", "SB", inplace=True)
    df["event"].replace("S", "SQ", inplace=True)
    df["event"].replace("B", "BP", inplace=True)
    df["event"].replace("D", "DL", inplace=True)

    # Normalize Equipment
    df["equipment"].replace("Raw", "R", inplace=True)
    df["equipment"].replace("Wraps", "W", inplace=True)
    df["equipment"].replace("Single-ply", "S", inplace=True)
    df["equipment"].replace("Multi-ply", "M", inplace=True)
    df["equipment"].replace("Unlimited", "U", inplace=True)
    df["equipment"].replace("Straps", "T", inplace=True)

    # Remove any results without a numeric 'place' - they were either disqualified,
    # no shows, guest lifters, etc.
    df["place"] = pd.to_numeric(df["place"], errors="coerce")
    df = df.dropna(subset=["place"])

    # Weight class field includes + symbol representing anything over the weight.
    df["weight_class"] = pd.to_numeric(df["weight_class"], errors="coerce")

    df["drug_tested"].replace("Yes", "True", inplace=True)
    df["drug_tested"].replace("No", "False", inplace=True)
    df["drug_tested"].fillna(value="False", inplace=True)

    # Drop any rows with null values for athlete
    df = df.dropna(subset=["athlete_id"])

    # Replace some null values to match desired table defaults
    df["drug_tested"].fillna(value=False, inplace=True)
    df["division"].fillna(value="", inplace=True)

    df["deadlift"].fillna(value=0, inplace=True)
    df["squat"].fillna(value=0, inplace=True)
    df["bench_press"].fillna(value=0, inplace=True)

    df["bodyweight"].fillna(value=0, inplace=True)
    df["weight_class"].fillna(value=0, inplace=True)

    df["age"].fillna(value=0, inplace=True)
    df["age_class"].fillna(value="", inplace=True)
    df["birth_year_class"].fillna(value="", inplace=True)

    df["place"].fillna(value=0, inplace=True)
    df["meet_total"].fillna(value=0, inplace=True)
    df["dots"].fillna(value=0, inplace=True)

    # Insert the dataframe into the database
    DBHelpers.insert_table_df(engine, table_name, df)
    return True
