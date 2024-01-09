from helpers.db_conn import DBHelpers


def clear_table(table_name):
    """
    Clear table operator deletes all records in a given table; useful for
    rerunning inserts while developing the pipeline, or cleaning data between
    imports
    """
    connection = DBHelpers.connect()
    DBHelpers.clear_table(connection, table_name)
    connection.close()
