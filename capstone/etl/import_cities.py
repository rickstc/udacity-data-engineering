from sqlalchemy import create_engine
from sqlalchemy.sql import text
import csv


# Define the parsing function
def clean_data(city_dictionary):
    """
    Clean some data fields on a city dictionary
    """
    city_dictionary["lat"] = float(city_dictionary["lat"])
    city_dictionary["lng"] = float(city_dictionary["lng"])
    city_dictionary["population"] = int(city_dictionary["population"])

    return city_dictionary


def load_csv(file_path):
    """
    Converts a CSV with headers into a list of dictionaries with the header row
    as keys
    """

    # List to hold the dictionaries
    data = []

    # Open the file
    with open(file_path, "r") as csvfile:
        # Instantiate a reader
        reader = csv.reader(csvfile)
        # Read the header row
        headers = next(reader)
        for row in reader:
            # Create a dictionary from the row data
            data.append(clean_data(dict(zip(headers, row))))
    return data


def import_cities():
    # Connect to the database and get a connection object
    db = create_engine("postgresql://student:student@127.0.0.1/studentdb")
    conn = db.connect()

    # Base statement for insert into `location_station` table
    base_statement = text(
        """
        INSERT INTO location_citylocation (
            city,
            country,
            population,
            city_location
        ) VALUES (
            :city,
            :country,
            :population,
            ST_PointFromText('POINT(:lat :lng)', 4326)
        )
    """
    )

    # List to hold data that will be put into the database
    data_list = load_csv("location/cities.csv")

    conn.execute(base_statement, data_list)
    conn.commit()

    conn.close()


def start():
    import_cities()


if __name__ == "__main__":
    start()
