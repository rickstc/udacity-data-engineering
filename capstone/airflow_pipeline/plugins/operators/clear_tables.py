from helpers.db_conn import DBHelpers


def clear_table(table_name):
    """Empties a table"""
    connection = DBHelpers.connect()
    DBHelpers.clear_table(connection, table_name)
    connection.close()
