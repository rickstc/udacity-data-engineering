# Data Modeling with Postgres

## Submission Information

Author: Timothy Ricks

Submission Date: 2021-11-29 (first)
Submission Date: 2021-12-04 (second)

External Repository: [GitHub](https://github.com/rickstc/udacity-data-engineering)

---

## Project Abstract

The project narrative instructs the student that they have been tasked by a music streaming startup called Sparkify to develop a database schema and ETL pipeline for their streaming platform. This schema and pipeline would be developed utilizing the platform's user logs (JSON) and song metadata (JSON), with the end goal of being able to better understand user listening preferences.

The student is to complete the boilerplate project code to define fact and definition tables representing a star schema and populate the tables using the JSON provided in the [data directory](docs/data.md).

---

## Additional Documentation

Additional documentation can be found in the `/docs` directory. Additional documentation includes:

- [data.md](docs/data.md) - Overview of the data format that the application will be processing
- [files.md](docs/files.md) - File structure for the application
- [tables.md](docs/tables.md) - Table schemas and descriptions

---

## Quickstart

### Install the Dependencies

To run this application locally, the dependencies must be installed (preferably in a virtual environment).

#### Using Pipenv (preferred)

```
pipenv install
pipenv shell
```

#### Using Pip

```
pip install -r requirements.txt
```

### Ensure Database Access

The project assumes that there is a postgres database server running on the default port (5432) accessible from `127.0.0.1`. Additionally, the database should have a database named `studentdb`, accessible by a username/password combination of `student/student`.

While Udacity provides a code execution environment, the author prefers local development, and has accomplished this using Docker. Database setup is outside the scope of this project and has not been included in the submission, however, a `docker-compose.yml` file for this specific project is available from the author's GitHub repository here: https://github.com/rickstc/udacity-data-engineering

### Initialize the Database

With the appropriate virtual environment activated (optional if required packages are available globally), run the following command to clean the database and populate the necessary tables:
`python create_tables.py`.

### Run the ETL Pipeline

Run the ETL Pipeline to parse the source data and populate the database tables by running the following command:
`python etl.py`
