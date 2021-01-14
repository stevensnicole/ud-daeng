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

Distribution of the data throughout the cluster nodes will be taken into consideration dependant on the type of data within the table and whether a field is used for joins.  

No specific requirement for reporting has been given, therefore the sorting requirements of the data is difficult to predict. Example sort keys have been given with respect to the example SQL queries at the end of this file.

### Fact table

#### Songplays

The log data associated with song plays i.e. records with `page` from the log file JSON set to `NextSong`. This table is most likely to be joined to the dimenaion table "songs" therefore a distribution key has been added to the join column song_id. After songs, the most likely table to be used and sorted by is time joiend to start_time, so a sortkey has been added to this column.

|   Column    |               Type             | PK | DISTKEY | SORTKEY |
| ----------- | ------------------------------ | -- | ------- | ------- |
| songplay_id | integer                        | Y  |         |         |
| start_time  | timestamp without time zone    |    |         |    Y    |
| user_id     | integer                        |    |         |         |
| level       | text variable unlimited length |    |         |         |
| song_id     | text variable unlimited length |    |    Y    |    Y    |
| artist_id   | text variable unlimited length |    |         |         |
| session_id  | integer                        |    |         |         |
| location    | text variable unlimited length |    |         |         |
| user_agent  | text variable unlimited length |    |         |         |


### Dimension tables

Distribuition key - Rows having similar values are placed in the same slice.
Distribution style - distributing both facts and dimensions on the joining KEYs eliminates shuffling
sorting key - used for columns that are used frequently in sorting like the date dimension and it's corresponding foreign key in the fact table. It maximises the query time since each node already has contiguous ranges of rows based on the sorting key.

#### Users

Users taken from the log files with a unique row for each user. The Log files contain the user information and will be duplicated for every songplay and login. Therefore when a new user is added from a log file, the most recent entry by timestamp contains the user's current setup. The ETL process must take this into account as distinct cannot be used due to a duplication when a user switches from free to paid or vice versa.

This table is likely to be fairly small and can be distributed across all nodes for query performance using a distribution style of all.

|   Column   |              Type              | PK | DISTKEY | SORTKEY |
| ---------- | ------------------------------ | -- | ------- | ------- |
| user_id    | integer                        | Y  |         |         |
| first_name | text variable unlimited length |    |         |         |
| last_name  | text variable unlimited length |    |         |         |
| gender     | text variable unlimited length |    |         |         |
| level      | text variable unlimited length |    |         |         |

#### Songs

Songs from the song metadata files, it is likely that data from this table could be sorted by title or year, therefore sortkeys have been added to these fields.

|  Column   |               Type             | PK | DISTKEY | SORTKEY |
| --------- | ------------------------------ | -- | ------- | ------- |
| song_id   | text variable unlimited length | Y  |    Y    |         |
| title     | text variable unlimited length |    |         |    Y    |
| artist_id | text variable unlimited length |    |         |         |
| year      | integer                        |    |         |    Y    |
| duration  | float                          |    |         |         |

#### Artists

Unique artists loaded from the song metadata files, sorted by the join key of artist_id and then the name, which is likely to be an order by for searching.

This table is likely to be fairly small and can be distributed across all nodes for query performance using a distribution style of all.

|  Column   |               Type             | PK | DISTKEY | SORTKEY |
| --------- | ------------------------------ | -- | ------- | ------- |
| artist_id | text variable unlimited length | Y  |         |    Y    |
| name      | text variable unlimited length |    |         |    Y    |
| location  | text variable unlimited length |    |         |         |
| latitude  | float                          |    |         |         |
| longitude | float                          |    |         |         |

This table is likely to be fairly small and can be distributed across all nodes for query performance using a distribution style of all.

#### Time

Timestamps of records in songplays broken down into units ready for analysis, makes it easy to group by items such as year. Ordered by it's join key of start_time as song plays are likely to be ordered by time.

|   Column   |            Type             | PK | DISTKEY | SORTKEY |
| ---------- | --------------------------- | -- | ------- | ------- |
| start_time | timestamp without time zone | Y  |         |    Y    |
| hour       | integer                     |    |         |         |
| day        | integer                     |    |         |         |
| week       | integer                     |    |         |         |
| month      | integer                     |    |         |         |
| year       | integer                     |    |         |         |
| weekday    | integer                     |    |         |         |

## File listing and execution

### File List

* README.md - this file describing the project, schema and ETL process.

* create_redshift.py - Python file to create the Amazon Redshift instance.

* drop_redshift.py - Python file used to delete the Amazon Redshift instance.

* dwh.cfg - setup values for the redshift instance.

* amzn-keys.cfg - key and secret for AWS, added to .gitignore so it is not commited to project.

* create_tables.py - Python file which creates sparkifydb, drops all the tables if they exist, then loads creates the schema.

* etl.py - Python script to extract data from the .json files, load it into the redshift database staging tables then transform it into the dimension and fact tables.

* sql_queries.py - Python file to build the drop, create, insert and select statements executed by create_tables.py and etl.py.

### Execution

1. create_redshift.py - creates the Redshift cluster and grants it permission to read from S3. Adds the cluster details to dwh.cfg

2. create_tables.py - creates the staging tables and star schema

3. etl.py - extracts the data from the S3 buckets into the staging table then trasnforms the raw staging table data into the star schema

4. drop_redshift.py - clean up process to drop the Redshift cluster and remove the cluster details from dwh.cfg

## Example queries

There are alot of rows in this data set which makes it difficult to get a good grasp of the kind of queries that would be helpful to the business.  Here are some example "top" queries where the top 10 are displayed.

### Top browsers

```sql
select top 10 user_agent, count(user_agent) from songplays
group by user_agent
order by count(user_agent) desc;
```

### Month when website most used
This would normally be used to help spot trends but there's only one month of data.

```sql
select month, count(month) from time
join songplays on time.start_time = songplays.start_time
group by month
order by count(month) desc;
```

### Most played artists

```sql
select top 10 name, count(name) from artists
join songplays on artists.artist_id = songplays.artist_id
group by name
order by count(name) desc;
```


### Most active users
```sql
select top 10 last_name || ',' || first_name as "User Name", count(last_name || ',' || first_name) from users
join songplays on users.user_id = songplays.user_id
group by last_name || ',' || first_name
order by count(last_name || ',' || first_name) desc;
```

