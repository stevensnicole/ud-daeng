# Project: Data Warehouse

## Project Introduction

The Sparkify startup have been collecting data on how their new music streaming app is being used. Sparkify are monitoring which songs their users are listening to by reporting against their log files. There has recently been an increase in the Sparkify user base and the song database and therefore the data to the log files is increasing. This has prompted Sparkify to investigate other options for storing log files and reporting data and they have decided to move to the cloud.

The log files are stored in JSON format, there are also metadata files of the songs in JSON format. The files are stored in an Amazon S3 bucket, the data is to be staged in Amazon Redshift and then transformed into a star schema optimized for queries on song play analysis. The process to ingest and then transform the data will use an ETL pipeline.

## File Structure

Each file type has the same structure, which will be used to load the data into the staging tables using an ETL process. The sample data displayed will be used to design the tables so that the data is loaded into the correct column type.

### Log Files

``` json
{
    "artist": "Infected Mushroom",
    "auth": "Logged In",
    "firstName": "Kaylee",
    "gender": "F",
    "itemInSession": 6,
    "lastName": "Summers",
    "length": 440.2673,
    "level": "free",
    "location": "Phoenix-Mesa-Scottsdale, AZ",
    "method": "PUT",
    "page": "NextSong",
    "registration": 1540344794796.0,
    "sessionId": 139,
    "song": "Becoming Insane",
    "status": 200,
    "ts": 1541107053796,
    "userAgent": "\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"",
    "userId": "8"
}
```

### Song file JSON structure

``` json
{
    "num_songs": 1,
    "artist_id": "ARJIE2Y1187B994AB7",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "",
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0
}
```

## Staging Schema

The staging schema will be optimized for quick loading of the data directly into tables with identical formats to that of the files being loaded. There will be no primary or foreign key or constraints that could slow the insert of the data.

### Staging table - staging_events

The log data associated with song play events.

|   Column    |               Type             |
| ----------- | ------------------------------ |
| artist | text variable unlimited length |
| auth  | text variable unlimited length |
| first_name     | text variable unlimited length |
| gender       | text variable unlimited length |
| item_in_session     | text variable unlimited length |
| last_name   | text variable unlimited length |
| length  | numeric                        |
| level    | text variable unlimited length |
| location  | text variable unlimited length |
| page  | text variable unlimited length |
| registration  | numeric |
| session_id  | integer |
| song  | text variable unlimited length |
| status  | integer |
| ts  | bigint |
| user_agent  | text variable unlimited length |
| user_id  | integer |

### Staging table - staging_songs

The metadata associated with songs.

|   Column    |               Type             |
| ----------- | ------------------------------ |
| num_songs | integer |
| artist_id  | text variable unlimited length |
| artist_latitude     | numeric |
| artist_longitude       | numeric |
| artist_location     | text variable unlimited length |
| artist_name   | text variable unlimited length |
| song_id  | text variable unlimited length |
| title    | numeric |
| duration  | text variable unlimited length |
| year  | integer |
## Analytics Schema

The analytics schema will be optimized for analysis of song plays, with the business data being stored in a fact table of songplays. The attributes for the song play data, artists, users, songs and time will be stored in dimension tables. Dimension tables contain descriptive information about the data in the fact table. This design is called a star schema, star schema's have the following characteristics:

1. Less complex queries:

    Join logic of star schema is simple in comparison to logic required to fetch data from a transactional schema that is highly normalized.

2. Less complex Business Reporting Logic:

    Compares to a highly normalized transaction schema, a star schema has simpler common business reporting logic, such as as-of reporting and period-over-period.

These two characteristics lend themselves well to the requirement, which is why they have been chosen.

### Fact table

#### Songplays

The log data associated with song plays i.e. records with `page` from the log file JSON set to `NextSong`.

|   Column    |               Type             | PK |
| ----------- | ------------------------------ | -- |
| songplay_id | integer                        | Y  |
| start_time  | timestamp without time zone    |    |
| user_id     | integer                        |    |
| level       | text variable unlimited length |    |
| song_id     | text variable unlimited length |    |
| artist_id   | text variable unlimited length |    |
| session_id  | integer                        |    |
| location    | text variable unlimited length |    |
| user_agent  | text variable unlimited length |    |


### Dimension tables

#### Users

Users taken from the log files with a unique row for each user.

|   Column   |              Type              | PK |
| ---------- | ------------------------------ | -- |
| user_id    | integer                        | Y  |
| first_name | text variable unlimited length |    |
| last_name  | text variable unlimited length |    |
| gender     | text variable unlimited length |    |
| level      | text variable unlimited length |    |

#### Songs

Songs from the song metadata files.

|  Column   |               Type             | PK |
| --------- | ------------------------------ | -- |
| song_id   | text variable unlimited length | Y  |
| title     | text variable unlimited length |    |
| artist_id | text variable unlimited length |    |
| year      | integer                        |    |
| duration  | float                          |    |

#### Artists

Unique artists loaded from the song metadata files.

|  Column   |               Type             | PK |
| --------- | ------------------------------ | -- |
| artist_id | text variable unlimited length | Y  |
| name      | text variable unlimited length |    |
| location  | text variable unlimited length |    |
| latitude  | float                          |    |
| longitude | float                          |    |


#### Time

Timestamps of records in songplays broken down into units ready for analysis, makes it easy to group by items such as year.

|   Column   |            Type             | PK |
| ---------- | --------------------------- | -- |
| start_time | timestamp without time zone | Y  |
| hour       | integer                     |    |
| day        | integer                     |    |
| week       | integer                     |    |
| month      | integer                     |    |
| year       | integer                     |    |
| weekday    | integer                     |    |

## File listing and execution

### File List

* README.md - this file describing the project, schema and ETL process.

* create_redshift.py - Python file to create the Amazon Redshift instance.

* drop_redshift.py - Python file used to delete the Amazon Redshift instance.

* dwh.cfg - setup values for the redshift instance.

* create_tables.py - Python file which creates sparkifydb, drops all the tables if they exist, then loads creates the schema.

* etl.py - Python script to extract data from the .json files, load it into the redshift database staging tables then transform it into the dimension and fact tables.

* sql_queries.py - Python file to build the drop, create, insert and select statements executed by create_tables.py and etl.py.
