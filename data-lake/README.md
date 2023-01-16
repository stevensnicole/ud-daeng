# Data Lake with AWS EMR

## Project summary

Sparkify is a music streaming startup that stores it's data in Amazon S3.  The data comprises of user and song databases, and now the user base has grown, analysis on the data is becoming difficult.

Sparkify require the data to be extracted from the S3 bucket and cleanse and transform the data into dimensional tables, allowing the Sparkify analytics team to find insights from the dataset.

## Files in GitHub repo

This github repo contains the following files

1. .gitignore - stops some files from being uploaded to git

2. etl.py - python file for the ETL pipe

3. dg.cfg - configuration file for AWS secret keys

## Datasets

### Song Dataset

### Log Dataset

## Resultant table structure

Once processed, the data will be stored in a star schema held within an S3 bucket with a fact table and four dimensions tables

### Fact table

**songplays**

All records with page set to "NextSong" - this is log data associated with song plays

| Column Name | Data Type | Note                                     |
|-------------|-----------|------------------------------------------|
| songplay_id | String    | Unique ID for the song play (GUID)       |
| start_time  | Integer   | Timestamp of the song play               |
| user_id     | Integer   | ID of the user who played the song       |
| level       | String    | Paid or free                             |
| song_id     | String    | ID for the song played                   |
| artist_id   | String    | ID for the song's artist                 |
| session_id  | Integer   | ID of the sparkify session               |
| location    | String    | Location the song was played             |
| user_agent  | String    | The method of play, such as browser type |


### Dimension tables

**users**

Users in the app

| Column Name | Data Type | Note                               |
|-------------|-----------|------------------------------------|
| user_id     | Integer   | Unique ID for the user             |
| first_name  | String    | Timestamp of the song play         |
| last_name   | String    | ID of the user who played the song |
| gender      | String    | Gender                             |
| level       | String    | Paid or free                       |

**songs**

Songs in music database

| Column Name | Data Type | Note                   |
|-------------|-----------|------------------------|
| song_id     | Integer   | Unique ID for the song |
| title       | String    | Title of the song      |
| artist_id   | String    | Artist ID for the song |
| year        | Integer   | Year of song release   |
| duration    | Float     | Duration of song       |

**artists**

Atists - artists in music database

| Column Name | Data Type | Note                     |
|-------------|-----------|--------------------------|
| artist_id   | String    | Unique ID for the Artist |
| name        | String    | Name of the artist       |
| location    | String    | Location of Artist       |
| lattitude   | Float     | Artist Lattitude         |
| longitude   | Float     | Artist Longitude         |

**time**

Timestamps of records in songplays broken down into specific units

| Column Name | Data Type | Note                                   |
|-------------|-----------|----------------------------------------|
| start_time  | Integer   | Timestamp                              |
| hour        | Integer   | Hour of timestamp                      |
| day         | Integer   | Day of timestamp                       |
| week        | Integer   | Week number of timestamp               |
| month       | Integer   | Month of timestamp                     |
| year        | Integer   | Year of timestamp                      |
| weekday     | String    | Three letter day of week for timestamp |

## How to run ETL pipeline

1. Add AWS credentials that have access to both buckets into the dl.cgf file

2. Update the etl.py file with the output S3 bucket

3. Create an EMR cluster on EC2, once the cluster has completed assign an ssh key to ssh to the cluster

4. sftp dl.cfg and etl.py to the cluster

5. Execute the command `spark-submit etl.py` to run