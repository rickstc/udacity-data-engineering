# Data Exploration

The project instructions broke the project into a series of steps. This document aims to cover the first two steps:

- Step 1: Scope the Project and Gather Data (this document)
- Step 2: Explore and Access the Data (this document)
- [Step 3: Define the Data Model](data_models.md)
- [Step 4: Run ETL to Model the Data](etl_pipeline.md)
- [Step 5: Complete Project Write-Up](write_up.md)

The student opted to combine them because these two were completed together, where data quality issues identified (step two) prompted the student to seek additional data sources (step 1).

## Udacity Step Definitions

These are the instructions given to the student, provided here for context.

### Step 1: Scope the Project and Gather Data

Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step, you’ll:

- Identify and gather the data you'll be using for your project (at least two sources and more than 1 million rows). See Project Resources for ideas of what data you can use.
- Explain what end use cases you'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)

### Step 2: Explore and Assess the Data

- Explore the data to identify data quality issues, like missing values, duplicate data, etc.
- Document steps necessary to clean the data

## Scope the Project and Gather Data

### Open Powerlifting Data

The student knew he wanted to work with the [Open Powerlifting Data](https://openpowerlifting.gitlab.io/opl-csv/bulk-csv-docs.html). as it is a topic of interest for him. Additionally, with more than 3 million entries in the CSV file, this meet's the project's requirement of one million rows in a data set. Each record in the data set is a record of an athlete's performance at a given competition.

This data included the athlete's "DOTS" score for a given competition, which provides a means for comparing the total strength of an athlete taking into account gender and weight class. It also contains the location (town, state, country) of the powerlifting competitions, which seemed like a good starting point for integrating additional data sources.

The student set out to find additional data sources that could be compared against this powerlifting dataset to enrich and enable interesting insights.

### Weather Station Data

The [GHCN-Daily Dataset](https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C00861/html) contains a list of weather stations around the world, with the elevation of each weather station. Elevation has the potential to impact athlete performance, and some elite athletes have anecdotally commented on the impact that changes in elevation have had on their performance, with some going so far as to train in higher elevations to improve their performance in their respective sports.

### Integration Issues

However, the weather stations data could not be correlated back to the powerlifting data because the location of each station was recorded by coordinates (latitude and longitude).

Therefore, the student needed to find some way to associate a powerlifting competition's location given as country, state, and town with the coordinates of a weather station. Additionally, each location may not have a nearby weather station, so the distance between the contest venue and weather station would have to be taken into account when determining how relevant the 'elevation' would be to the contest location. Obviously, if the nearest weather station is 200 KM from the contest venue, the elevation of the weather station will be less reliable than a station that is 1KM from a contest venue.

If this had been a production application, the student likely would have looked into geocoding the locations of each powerlifting competition using a third party service like [Google Maps](https://developers.google.com/maps/documentation) or [OpenStreetMaps](https://wiki.openstreetmap.org/wiki/API_v0.6). However, the student is not aware of any data service that allows for geocoding that also allows for the dataset to be downloaded manually (versus an API being called for each address). Calling the API would either be costly or time consuming, and would not be particularly conducive to developing an ETL pipeline that can be run over and over.

### City Export

In looking for a downloadable dataset of global cities that included location, the student discovered an excerpt from a dataset hosted on GitHub by [GitHub User curran](https://gist.github.com/curran/13d30e855d48cdd6f22acdf0afe27286/). This dataset contains city, country, lat, lng, and population for cities with more than 50,000 people globally. While not comprehensive, nor a direct match, it provided enough information for the student to loosely draw a correlation between the town, state, and country data in the powerlifting dataset with the town and country in this dataset to obtain a latitude and longitude value, which could then be compared with the location data in the weather station data set.

## Explore and Access the Data

### Powerlifting Data Set

The [OpenPowerlifting Data Service](https://openpowerlifting.gitlab.io/opl-csv/bulk-csv-docs.html) contains excellent documentation about the data. The fields included in the CSV include:

- Name
- Sex
- Event
- Equipment
- Age
- AgeClass
- BirthYearClass
- Division
- BodyweightKg
- WeightClassKg
- Squat1Kg
- Squat2Kg
- Squat3Kg
- Squat4Kg
- Best3SquatKg
- Bench1Kg
- Bench2Kg
- Bench3Kg
- Bench4Kg
- Best3BenchKg
- Deadlift1Kg
- Deadlift2Kg
- Deadlift3Kg
- Deadlift4Kg
- Best3DeadliftKg
- TotalKg
- Place
- Dots
- Wilks
- Glossbrenner
- Goodlift
- Tested
- Country
- State
- Federation
- ParentFederation
- Date
- MeetCountry
- MeetState
- MeetTown
- MeetName

A sample of this data is located at [opl-sample.csv](sample_data/opl-sample.csv).

#### PL Data Quality Issues

How these fields mapped into a data model is covered in the [Data Model Documentation](./data_models.md), however, here are some of the data quality issues that had to be addressed with this data set:

- Athletes in the data set are deduplicated by a pound symbol (#) and a number after their name. The student had to string split based on the presence of the pound symbol and create a new entry for each athlete.
- MeetTown and MeetState were often blank. Some competitions were just listed as taking place in a country, which made geocoding infeasible. Null values were filled with empty strings.
- Some fields that represent boolean values were represented by "Yes" or "No". These were converted to proper booleans, with null values converted to a sensible default. For example, the "Tested" field indicated whether an athlete was drug tested. If no value was provided, the student assumed the athlete was not drug tested.
- Because the data export is a flat CSV file, many of the fields were duplicated. Each row represents an athlete's performance in a contest, so all of the fields for a contest were duplicated for every athlete that competed: (State, Federation, ParentFederation, Date, MeetCountry, MeetState, MeetTown, MeetName). The student had to deduplicate to distinguish between unique contests during the process of normalizing the data.

### Weather Station Data Set

The weather station data obtained from the [GHCN-Daily Dataset](https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C00861/html) came in the format of a flat text file, with each row representing a weather station, and different data points about the station being contained within certain positions on the line (it was not delineated by any special character).

Fortunately, the following data dictionary was provided:

    ------------------------------
    Variable   Columns   Type
    ------------------------------
    ID            1-11   Character
    LATITUDE     13-20   Real
    LONGITUDE    22-30   Real
    ELEVATION    32-37   Real
    STATE        39-40   Character
    NAME         42-71   Character
    GSN FLAG     73-75   Character
    HCN/CRN FLAG 77-79   Character
    WMO ID       81-85   Character
    ------------------------------

A sample of this data is located at [stations-sample.txt](sample_data/stations-sample.txt).

### Weather Data Quality Issues

The first challenge was parsing this format. The student ended up pulling different fields out using string slicing, passing the values to functions where necessary to clean the data:

```python
cleaned_data = {
    "station_id": line[0:11].strip(),
    "elevation": float(line[31:37]),
    "station_state": clean_state(line[38:40].strip()),
    "station_name": line[41:71].strip().replace("'", ""),
    "gsn_flag": clean_gsn_flag(line[72:75]),
    "wmo_id": clean_wmo_id(line[80:85]),
    "lat": float(line[21:30].strip()),
    "lng": float(line[12:20].strip()),
}
```

These string values had to be parsed into integers or float values in some cases, and spaces had to be trimmed from fields to ensure data consistency.

### City Location/Population Data Set

The dataset obtained from [GitHub User curran](https://gist.github.com/curran/13d30e855d48cdd6f22acdf0afe27286/) was the most straightfoward to parse. It was a simple CSV file with the following fields:

- city
- lat
- lng
- country
- population

A sample of this data is located at [cities-sample.csv](sample_data/cities-sample.csv).

#### City Location Data Quality Issues

The simplicity of the data set didn't mean it was without issues, however. City and Country were the only fields that could be compared to the data in the Open Powerlifting Data Set. Because this data didn't contain "state", it would have been likely that two or more cities from the data set would match with a location in the powerlifting data.

To prevent this, the student simply dropped any records where city and country were duplicated. An example of this would be "Portland" "United States", where that could have matched on powerlifting competitions held in Portland, Oregon or Portland, Maine.
