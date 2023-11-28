# Capstone Project - Powerlifting Climate Data Explorer

# Project Introduction and Scope
<!-- TODO: Introduce the project -->
The world of competitive powerlifting is filled with atheletes doing their very best to control and account for every variable in order to perform at their very best. In this project, I will build an ETL pipeline that loads geographic elevation data and powerlifting competition results into a dashboard that will allow the user to analyze any potential impacts that altitude at the competition venue may have on the athlete's performance.

## Data Sources

### Powerlifting Dataset

OpenPowerlifting maintains a data service that aggregates lift data from powerlifting competitions and provides a downloadable CSV file with results from powerlifting meets all over the world. Additionally, OpenPowerlifting maintains good documentation about their data here: [Open Powerlifting Data Documentation](https://openpowerlifting.gitlab.io/opl-csv/bulk-csv-docs.html). As of 8/19/2023, this CSV contains 2,960,122 records, which satisfies the requirements for at least one million records.

### Elevation Dataset

The weather data used in this project is coming from The Global Historical Climatology Network - Daily (GHCN-Daily), courtesy of the National Centers for Environment Information, a US agency managed by the National Oceanic and Atmospheric Administration (NOAA). This data is organized into multiple CSV files. The data directory provides a folder for each year starting with 1901. Within each folder for a given year, there is a CSV file per weather station. Within each CSV file, there is a line for each hour of each day. Therefore, a single year's worth of data could contain as many as 306,600,000 (35,000 stations *365 days* 24 hours/day) records. Documentation for this data set can be found at the [GHCN-Daily Dataset Overview Page](https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C00861/html).

## Data Dashboard

I will be building an ETL pipeline that loads data into a Postgres database, a REST API that will expose the data, and a visual dashboard that consumes the API that can be used to explore and search the data.

### ETL Pipeline
<!-- TODO: Information on the pipeline -->

### REST API and Visual Dashboard

I will be leveraging [Django](https://www.djangoproject.com/) and [Django REST Framework](https://www.django-rest-framework.org/) to build the API and dashboard. Django is python web framework that provides an ORM (Object Relational Mapper) that makes working with objects in a database easy. Django REST Framework is also a python web framework that provides tools for building REST APIs, as well as a "Browsable API" built using Twitter Bootstrap that allows users to view and search data using HTML.

# Explore and Access the Data

<!-- 
Explore the data to identify data quality issues, like missing values, duplicate data, etc.
Document steps necessary to clean the data
-->

# Define the Data Model

<!-- 
Map out the conceptual data model and explain why you chose that model
List the steps necessary to pipeline the data into the chosen data model
-->

# Run ETL to Model the Data

<!-- 

Create the data pipelines and the data model
Include a data dictionary
Run data quality checks to ensure the pipeline ran as expected
Integrity constraints on the relational database (e.g., unique key, data type, etc.)
Unit tests for the scripts to ensure they are doing the right thing
Source/count checks to ensure completeness
-->

# Complete Project Write Up

<!-- 
What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
Clearly state the rationale for the choice of tools and technologies for the project.
Document the steps of the process.
Propose how often the data should be updated and why.
Post your write-up and final data model in a GitHub repo.
Include a description of how you would approach the problem differently under the following scenarios:
    If the data was increased by 100x.
    If the pipelines were run on a daily basis by 7am.
    If the database needed to be accessed by 100+ people.
-->

<!-- Rubric

Scoping the Project

The write up includes an outline of the steps taken in the project. The purpose of the final data model is made explicit.

Addressing Other Scenarios

The write up describes a logical approach to this project under the following scenarios:

The data was increased by 100x.
The pipelines would be run on a daily basis by 7 am every day.
The database needed to be accessed by 100+ people.
Defending Decisions

The choice of tools, technologies, and data model are justified well.

Project code is clean and modular.

All coding scripts have an intuitive, easy-to-follow structure with code separated into logical functions. Naming for variables and functions follows the PEP8 style guidelines. The code should run without errors.

Quality Checks

The project includes at least two data quality checks.

Data Model

The ETL processes result in the data model outlined in the write-up.
A data dictionary for the final data model is included.
The data model is appropriate for the identified purpose.
Datasets

The project includes:

At least 2 data sources
More than 1 million lines of data.
At least two data sources/formats (csv, api, json)

-->