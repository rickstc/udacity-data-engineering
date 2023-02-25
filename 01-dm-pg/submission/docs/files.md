# Project Files

[<- Return to Overview](../README.md)

This table contains a listing of core project files and a brief description of each file.

| File             | Description                                                                                                                                                                                                      |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| create_tables.py | This file provides methods to prepare the database and tables, and is meant to be run directly without modification prior to data being loaded into the database.                                                |
| etl.ipynb        | Jupyter (iPython) Notebook file that provides the ability to process a single file song and log entry and populate the tables with that data.                                                                    |
| etl.py           | This file provides the functionality necessary to load data into the database from the json data files.                                                                                                          |
| README.md        | Provides basic project information including author, quickstart, and links to documentation.                                                                                                                     |
| sql_queries.py   | This file defines the basic sql queries necessary to perform all necessary interactions with the database, including dropping and creating tables, inserting records into the tables, and querying the database. |
| test.ipynb       | Jupyter (iPython) Notebook file that selects and displays the first few rows of each database table.                                                                                                             |
| test.py          | Unit Test to assess the state of the database after the database has been instantiated and the log files have been processed and data inserted into the database.                                                |

Additional documentation is available in the `docs/` directory.

[<- Return to Overview](../README.md)
