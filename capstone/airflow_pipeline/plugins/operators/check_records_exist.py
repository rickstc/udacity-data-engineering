from helpers.db_conn import DBHelpers


def check_records_in_database(table_name):
    """
    This will check the number of records in a table, raising a ValueError if
    the table is empty

    Useful for checking to ensure that data is getting loaded into the table,
    serving as a quality check on the pipeline
    """
    connection = DBHelpers.connect()

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) > 0 FROM {table_name};")
        result = cursor.fetchone()[0]
        if not result:
            raise ValueError(f"No records found in the table {table_name}")
