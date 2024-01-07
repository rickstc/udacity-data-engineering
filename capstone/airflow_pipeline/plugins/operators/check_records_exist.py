from helpers.db_conn import DBHelpers


def check_records_in_database(table_name):
    connection = DBHelpers.connect()

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) > 0 FROM {table_name};")  # Execute the check
        result = cursor.fetchone()[0]
        if not result:
            raise ValueError(f"No records found in the table {table_name}")
