# Project: Data Modeling with Postgres

## Project Introduction

The Sparkify startup have been collecting data on how their new music streaming app is being used. Sparkify would like to know which songs users are listening to by reporting against their log files.

The log files are stored in JSON format, there are also metadata files of the songs in JSON format. The requirement is to take the data from these files and create a database. The database must be designed to optimize queries for songplay analysis. Once the schema has been designed and created, the JSON files will be loaded into the database via an ETL pipeline.

The Sparkify analytics team will issue queries based on the schema design for results comparison.

## File Structure

Each file type has the same structure, which will be used to load the data into the schema using an ETL process. The sample data displayed will be used to design the tables so that the data is loaded into the correct column type.

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

## Schema
The schema will be optimized for analysis of song plays, with the business data being stored in a fact table of songplays. The attributes for the song play data, artists, users, songs and time will be stored in dimension tables. Dimension tables contain descriptive information about the data in the fact table. This design is called a star schema, star schema's have the following characteristics:
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

* create_tables.py - Python file which creates sparkifydb, drops all the tables if they exist, then loads creates the schema.

* etl.ipnyb - Jupiter notebook with exploratory python snippets checking how to maniputlate and load the data.

* etl.py - Python script to extract data from the .json files, transform it and load into the schema.

* README.md - this file describing the project, schema and ETL process.

* sql_queries.py - Python file to build the drop, create, insert and select statements executed by create_tables.py and etl.py.

* test.ipynb - Jupyter notebook to explore the data in the schema once it has loaded.


### Executing the data load

Requires Python 3, Postgres running locally with a login of student/student to a db named studentdb.

Execute the following at a python command prompt to create the schema:

``` sh
./create_tables.py
```

Once the schema is created execute the following at a python command prompt to load the data:

``` sh
./etl.py
```

Use the `test.ipynb` jupyter notebook to check the data, lauch jupyter notbooks with:

``` sh
jupyter notebook
```

## Example Analytic Outputs

