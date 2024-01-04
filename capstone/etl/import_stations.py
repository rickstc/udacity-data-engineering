from sqlalchemy import create_engine
from sqlalchemy.sql import text


def clean_gsn_flag(value):
    """
    Return True if 'GSN' is a substring of the value provided
    """
    return True if "GSN" in str(value) else False


def clean_wmo_id(value):
    """
    The values should either be integers or return 0 for a default case
    """
    try:
        return int(value.strip())
    except Exception:
        return 0


def clean_state(value):
    """
    Return null if value is empty else wrap in ""
    """
    return "" if len(value) == 0 else value


# Define the parsing function
def parse_line(line):
    cleaned_data = {
        "station_id": line[0:11].strip(),
        "elevation": float(line[31:37]),
        "station_state": clean_state(line[38:40].strip()),
        "station_name": line[41:71].strip().replace("'", ""),
        "gsn_flag": clean_gsn_flag(line[72:75]),
        "wmo_id": clean_wmo_id(line[80:85]),
        "lat": float(line[21:30].strip()),
        "lng": float(line[12:20].strip()),
    }

    return cleaned_data


def import_stations():
    # Connect to the database and get a connection object
    db = create_engine("postgresql://student:student@127.0.0.1/studentdb")
    conn = db.connect()

    # Base statement for insert into `fact_weatherstation` table
    base_statement = text(
        """
        INSERT INTO fact_weatherstation (
            station_id,
            station_location,
            elevation,
            station_state,
            station_name,
            gsn_flag,
            wmo_id
        ) VALUES (
            :station_id,
            ST_PointFromText('POINT(:lat :lng)', 4326),
            :elevation,
            :station_state,
            :station_name,
            :gsn_flag,
            :wmo_id
        ) ON CONFLICT DO NOTHING
    """
    )

    # List to hold data that will be put into the database
    data_list = []

    # Open the stations file
    with open("location/stations.txt") as file:
        # Each line in the file represents a weather station
        # For each line (station)
        for line in file:
            # Parse the station data in from the line and add that parsed data
            # to the data_list defined above
            data_list.append(parse_line(line))

            # Execute inserts in batches of 500 and then reset the data_list
            if len(data_list) % 500 == 0:
                print(f"Inserting {len(data_list)} into the database.")
                conn.execute(base_statement, data_list)
                data_list = []

        # Insert any remaining records after all lines have been processed
        print(f"Inserting {len(data_list)} into the database.")
        conn.execute(base_statement, data_list)

    conn.commit()

    conn.close()


def start():
    import_stations()


if __name__ == "__main__":
    start()
