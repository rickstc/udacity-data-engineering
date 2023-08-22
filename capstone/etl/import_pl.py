import glob
import csv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def combine_columns(row):
    return f"{row['country']}.{row['state']}.{row['town']}"


def insert_frame(df, table_name):
    db = create_engine("postgresql://student:student@127.0.0.1/studentdb")
    conn = db.connect()

    # Insert into Database
    df.to_sql(con=conn, name=table_name, index=False, if_exists="append")

    conn.close()


def load_table(table_name):
    db = create_engine("postgresql://student:student@127.0.0.1/studentdb")
    conn = db.connect()

    sql_query = f"SELECT * FROM {table_name}"

    df = pd.read_sql(sql_query, conn)

    return df


def lookup_contest_id(row, lookup):
    key = f"{row['name']}.{row['date']}"
    return lookup[key]


def handle_contest_results(df, athletes, contests):
    # Rename ids to support foreign keys
    athletes = athletes.rename(columns={"id": "athlete_id"})
    contests = contests.rename(columns={"id": "contest_id"})

    """
    I spent hours trying to merge the df data frame with the contests data frame but I
    kept getting contest_id = NaN for all records. I decided to, instead, go with the 
    less elegant but still pretty quick python dictionary to act as a hash map, which
    still has pretty decent speed. Compute power is cheap, developer time is not.

    df = df.rename(columns={"MeetName": "name", "Date": "date"})
    # Merge Contests
    df = df.merge(contests, on=["name", "date"], how="left")
    
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

    print("Pre:")
    print(df)

    # Remove any results without a numeric 'place' - they were either disqualified,
    # no shows, guest lifters, etc.
    df["place"] = pd.to_numeric(df["place"], errors="coerce")
    df = df.dropna(subset=["place"])

    # Weight class field includes + symbol representing anything over the weight.
    df["weight_class"] = pd.to_numeric(df["weight_class"], errors="coerce")

    df["drug_tested"].replace("Yes", "True", inplace=True)
    df["drug_tested"].replace("No", "False", inplace=True)
    df["drug_tested"].fillna(value="False", inplace=True)

    print("Post:")
    print(df)

    # Update database
    insert_frame(df, "powerlifting_contestresult")


def handle_contests(df, locations_df):
    # Rename Fields
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
    locations_df = locations_df.rename(columns={"id": "location_id"})

    df = df.merge(locations_df, on=["country", "town", "state"], how="left")

    # Remove unnecessary fields post merge
    df = df.drop(["country", "state", "town"], axis=1)

    # Fill in missing values
    df.federation.fillna(value="", inplace=True)
    df.parent_federation.fillna(value="", inplace=True)

    # Insert into database
    insert_frame(df, "powerlifting_contest")


def handle_athletes(df):
    """Transform dataframe containing athlete information and insert into database"""
    # 'Name' column is deduplicated via # symbol, so, we can drop based on name
    athletes = df.drop_duplicates("Name")

    # Rename Name to name and Sex to gender to conform with database fields
    athletes = athletes.rename(columns={"Name": "name", "Sex": "gender"})

    # Split "name" column into "name" and "deduplication_number"
    try:
        athletes[["name", "deduplication_number"]] = athletes.name.str.split(
            " #", expand=True
        )
    except:
        athletes["deduplication_number"] = 0

    # Transform 'gender' field, replacing "Mx" with "X", to conform with API Schema
    athletes["gender"].replace("Mx", "X", inplace=True)

    # If there are 'deduplication_number' fields with no value, replace with 0
    athletes.deduplication_number.fillna(value=0, inplace=True)

    insert_frame(athletes, "powerlifting_athlete")
    return athletes


def handle_locations(df):
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

    # Insert locations into database
    insert_frame(df, "powerlifting_contestlocation")


def start():
    dir = "opl"
    csv_files = glob.glob(f"{dir}/**/*.csv")

    # print(csv_files)

    athletes_frames = []
    location_frames = []
    contest_frames = []
    contest_result_frames = []
    for csv_fp in csv_files:
        for df in pd.read_csv(csv_fp, chunksize=5000):
            # Create an athletes data frame
            athletes_frames.append(df.loc[:, ["Name", "Sex"]])

            # Create a locations data frame
            location_frames.append(df.loc[:, ["MeetCountry", "MeetState", "MeetTown"]])

            # Create a contests data frame
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

            # Contest Results
            contest_result_frames.append(
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

    # Insert Athletes
    # handle_athletes(pd.concat(athletes_frames))
    athletes = load_table("powerlifting_athlete")

    # Insert Locations
    # handle_locations(pd.concat(location_frames))
    locations = load_table("powerlifting_contestlocation")

    # Insert Contests
    # handle_contests(pd.concat(contest_frames), locations)
    contests = load_table("powerlifting_contest")

    # Insert Contest Results
    handle_contest_results(pd.concat(contest_result_frames), athletes, contests)


def test():
    df1 = [
        ["name", "date"],
        ["Open Tournament", "2019-05-11"],
        ["World Open Championships", "2016-11-14"],
    ]


if __name__ == "__main__":
    start()
    # test()
