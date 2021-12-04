# Database Table Implementation

[<- Return to Overview](../README.md)

The project instructions indicated that the following tables should be created:
| Table | Type | Comment |
| ----- | -------- | --- |
| songplays | fact | Record information about a song play |
| users | dimension | Basic information about an application user |
| songs | dimension | Basic information about an available song |
| artists | dimension | Basic information about an artist |
| time | dimension | Timestamps of `songplays` broken down into specific time units |

## Disclaimers

In a production environment, care would be taken to identify primary and foreign keys, and to set up database constraints. However, due to a note in the project rubric, no such attempts have been made in this implementation. The dataset provided includes many `null` values in fields that would have been identified as primary key values. As such, attempts to enforce key constraints would produce multiple failures.

In lieu of placing such constraints in the create tables, these fields have been annotated in the following table definitions.

Abbreviations used in comments are:

- pk: Primary Key
- fk(<table.column>): Foreign Key to a given field in a table

In addition to key constraints, attempts should be made to identify maximum field lengths and limit where appropriate. While outside the scope of this use case, location-specific data such as lat/lng could benefit from the Postgres GIS extension to leverage it's geospatial calculation capabilities.

## Songplays

| Field       | Type      | Comments             |
| ----------- | --------- | -------------------- |
| songplay_id | serial    | pk                   |
| start_time  | timestamp | not null             |
| user_id     | varchar   | fk(Users.user_id)    |
| level       | varchar   |                      |
| song_id     | varchar   | fk(Songs.song_id)    |
| artist_id   | varchar   | fk(Artist.artist_id) |
| session_id  | int       | not null             |
| location    | varchar   |                      |
| user_agent  | varchar   |                      |

## Users

| Field      | Type    | Comments |
| ---------- | ------- | -------- |
| user_id    | int     | pk       |
| first_name | varchar | not null |
| last_name  | varchar | not null |
| gender     | varchar |          |
| level      | varchar |          |

## Songs

| Field     | Type    | Comments              |
| --------- | ------- | --------------------- |
| song_id   | varchar | pk                    |
| title     | varchar | unique                |
| artist_id | varchar | fk(Artists.artist_id) |
| year      | int     |                       |
| duration  | decimal |                       |

## Artists

| Field     | Type    | Comments |
| --------- | ------- | -------- |
| artist_id | varchar | pk       |
| name      | varchar | unique   |
| location  | varchar |          |
| latitude  | int     |          |
| longitude | int     |          |

## Time

| Field      | Type      | Comments |
| ---------- | --------- | -------- |
| start_time | timestamp | pk       |
| hour       | int       | not null |
| day        | int       | not null |
| week       | int       | not null |
| month      | int       | not null |
| year       | int       | not null |
| weekday    | varchar   | not null |

[<- Return to Overview](../README.md)
