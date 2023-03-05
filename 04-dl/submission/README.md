# Data Lakes with Spark: Project

## Submission Information

Author: Timothy Ricks

Submission Date: 2023-03-05

External Repository: [GitHub](https://github.com/rickstc/udacity-data-engineering)

---

## Project Abstract

The project narrative states that the student has been tasked with assisting Sparkify, a music streaming startup, in building an ETL pipeline. This pipeline needs to extract song and log data from JSON files stored in S3, process them using Spark and load the data into dimensional tables back into S3.

---

## Quickstart

---

### Install the Dependencies

To run this application locally, the dependencies must be installed (preferably in a virtual environment).

#### Using Pipenv (preferred)

```bash
pipenv install
pipenv shell
```

#### Using Pip (not preferred)

```bash
pip install -r requirements.txt
```

### Configure the Environment

Rename the "dl.cfg-sample" file to "dl.cfg" and fill in the required configuration information.

```bash
mv dl.cfg-sample dl.cfg
```

### Run the ETL Pipeline
Run the ETL pipeline by using the `etl.py` file.


## Documentation

### Files

The following is a list of files contained in the repository:
| File             | Description                                                                                                                                                                                                                                                                                                                                                                          |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| dl.cfg-sample   | This configuration file allows the user to explictly define their AWS credentials. This file is a template meant to be filled in by the user, and renamed to `dwh.cfg`. |
| etl.py | The logic required to process song and log data, transforming it into dimensional tables that can be reused for analysis. |
| Pipfile and Pipefile.lock | These files define the package requirements for the project, using pipenv. |
| README.md | This file; documents the project. |
| sql_queries.py | The file provided by Udacity for the student to fill in to populate sql queries necessary to accomplish Sparkify's objective. Queries contained include loading the event data into Redshift staging tables, building the data tables from the staging tables, and defining the analytical queries for Sparkify's analytics team. |