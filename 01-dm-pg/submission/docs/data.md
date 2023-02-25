# Project Source Data

[<- Return to Overview](../README.md)

The data was provided by Udacity for this project, and has been omitted from this repository. The data directory can be dropped directly into the application root folder, which would result in the following folder/file structure:

## Directory Structure

- /data
  - /log_data/11
    - 2018-11-01-events.json
    - 2018-11-02-events.json
    - ...
  - /song_data/A
    - /A
      - /A
        - TRAAAAW128F429D538.json
        - ...
      - /B
        - TRAABCL128F4286650.json
        - ...
      - /C
        - TRAACCG128F92E8A55.json
        - ...
    - /B
      - /A
        - TRABACN128F425B784.json
        - ...
      - /B
        - TRABBAM128F429D223.json
        - ...
      - /C
        - TRABCAJ12903CDFCC2.json
        - ...

## Data Formats

### Event

The basic format of an event is as follows:

```
{
    "artist": null,
    "auth": "Logged In",
    "firstName": "Walter",
    "gender": "M",
    "itemInSession": 0,
    "lastName": "Frye",
    "length": null,
    "level": "free",
    "location": "San Francisco-Oakland-Hayward, CA",
    "method": "GET",
    "page": "Home",
    "registration": 1540919166796.0,
    "sessionId": 38,
    "song": null,
    "status": 200,
    "ts": 1541105830796,
    "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId": "39"
}
```

### Song

The basic format of a song is:

```
{
    "num_songs": 1,
    "artist_id": "ARD7TVE1187B99BFB1",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "California - LA",
    "artist_name": "Casual",
    "song_id": "SOMZWCG12A8C13C480",
    "title": "I Didn't Mean To",
    "duration": 218.93179,
    "year": 0
}
```

[<- Return to Overview](../README.md)
