# Data Modeling with Postgres

## Submission Information

Author: Timothy Ricks

Submission Date: 2022-04-23

External Repository: [GitHub](https://github.com/rickstc/udacity-data-engineering)

---

## Project Abstract

The project narrative instructs the student that they have been tasked to assist in analyzing song paly data recorded by Sparkify, a startup music streaming service. Sparkify's data analysis team would like the student to import data from CSV files into an Apache Cassandra database to address their specific analysis needs.

---

## Documentation

The following is a list of files contained in the repository:

| File                              | Description                                                                                                                                                                                                                                                              |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| etl_pipeline.py                   | This contains the ETL pipeline code provided by Udacity. It's purpose is to process the data located in the /event_data/ folder (not included) in order to generate a CSV file which the student will use to load data into the Cassandra database.                      |
| etl_tests.py                      | This file contains tests written by the student to ensure that the ETL pipeline correctly generates the CSV file.                                                                                                                                                        |
| song_analysis.py                  | This file contains the code that accomplishes the goals set forth by the project's requirements. The student abstracted the code provided by Udacity into it's own class so as to make clear what code was provided by Udacity and what code was written by the student. |
| analysis_tests.py                 | This contains the student's tests for the actual analysis queries. While not part of the assignment, the student prefers to leverage unit tests to ensure correct and consistent functionality over print statements.                                                    |
| Pipfile and Pipfile.lock          | These files are managed through the use of the `pipenv` python package. The student is using this tool for requirements management as an alternative to `pip` as it handles installing requirements into a python virtual environment.                                   |
| requirements.txt                  | This file contains the project's requirements.                                                                                                                                                                                                                           |
| README.md                         | This file                                                                                                                                                                                                                                                                |
| Project_1B_Project_Template.ipynb | Jupyter (iPython) Notebook file provided by Udacity that contains all boilerplate code. This file was not completed by the student.                                                                                                                                      |

---

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
