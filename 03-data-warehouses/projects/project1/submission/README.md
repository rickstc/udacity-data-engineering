# Cloud Data Warehouses: Project

## Submission Information

Author: Timothy Ricks

Submission Date: 2022-06-20

External Repository: [GitHub](https://github.com/rickstc/udacity-data-engineering)

---

## Project Abstract

The project narrative states that the student has been tasked with assisting Sparkify, a music streaming startup, in building an ETL pipeline. This pipeline needs to extract song and log data from S3, stage them in Redshift, and transform the data into a set of dimensional tables that Sparkify's analytics team can use.

---

## Documentation

The following is a list of files contained in the repository:

| File             | Description                                                                                                                                                                                                                                                                                                                                                                          |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| create_tables.py | This file contains the necessary functionality to drop and create tables using the queries defined in `sql_queries.py`. The student created a Redshift class to handle other interactions with the Redshift database, and built this functionality into that class. Therefore, this file is provided for context, and is fully functional but not necessary to run the ETL pipeline. |
| dwg.cfg          | This configuration file allows the user to configure their redshift connection parameters and S3 buckets where the song data is stored. This file is a template meant to be filled in by the user, and is blank to protect the student's private connection information.                                                                                                             |
| iac.py           | This file provides a consistent means for creating, removing, and getting the status of a Redshift cluster within AWS.                                                                                                                                                                                                                                                               |

---

# TODO:

## Quickstart

---

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

---

### Copy Song Data into Project Folder

Two folders have been omitted from this repository that contain the data provided to the student by Udacity. The omission was intended to keep the repository slim and guard against potential data exfiltration. The scripts contained in this repository operate on that data; therefore, anyone who clones this repository must place two folders (`event_data` and `images`) directly into the folder that contains this file.

---

### Ensure Database Access

The project assumes that there is a cassandra database server running on the default port (9042) accessible from `127.0.0.1`.

While Udacity provides a code execution environment, the author prefers local development, and has accomplished this using Docker. Database setup is outside the scope of this project and has not been included in the submission, however, a `docker-compose.yml` file for this specific project is available from the author's GitHub repository here: https://github.com/rickstc/udacity-data-engineering

---

### Run the ETL Pipeline

Generate the CSV data file by running the ETL pipeline with: `python etl_pipeline.py`. Correct generation can be tested using `python etl_tests.py`.

### Run the Analysis

Run the data analysis script to view the results of the data analysis with `python song_analysis.py`. Correct execution can be verified using the student-provided test file with `python analysis_tests.py`.
