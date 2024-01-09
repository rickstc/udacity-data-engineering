from helpers.db_conn import DBHelpers

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


def parse_line(line):
    """
    This builds a dictionary of values based on a given line in the stations
    file.

    According to the weather station's documentation here:
    https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt

    The data is in the following format:
        ------------------------------
        Variable   Columns   Type
        ------------------------------
        ID            1-11   Character
        LATITUDE     13-20   Real
        LONGITUDE    22-30   Real
        ELEVATION    32-37   Real
        STATE        39-40   Character
        NAME         42-71   Character
        GSN FLAG     73-75   Character
        HCN/CRN FLAG 77-79   Character
        WMO ID       81-85   Character
        ------------------------------

    """
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


def load_fact_station(file_path, table_name, **kwargs):
    """
    This loads data into the 'fact_weatherstation' table. This corresponds to
    the 'fact.WeatherStation' model in the dashboard application.
    """

    # Connect to the database and get a connection object
    engine = DBHelpers.get_engine()

    # Base statement for insert into `fact_weatherstation` table
    """
    This SQL statement populates the table. In other instances, the student
    opted to use the pandas data frame 'to_sql' method, however, it did not
    play nicely with the geo spatial fields without additional configuration
    or libraries such as https://geopandas.org/en/stable/.

    Rather than bring in another dependency, it seemed relevant to demonstrate
    the ability to work with SQL directly.

    Of particular interest in the following statement is the use of the
    ST_PointFromText function, which inserts a Point field from a given
    latitude and longitude.
    """
    base_statement = text(
        f"""
        INSERT INTO {table_name} (
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

    with engine.begin() as conn:
        # Open the stations file
        with open(file_path, "r") as file:
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
