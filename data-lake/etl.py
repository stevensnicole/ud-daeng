import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['secretkey']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['secretkey']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates a spark session

    Args:
        None

    Returns:
        spark - object for the spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Reads the songs json files and creates parquet files of songs and artists

    Args:
        spark - object for the spark session
        input_data - S3 bucket endpoint for the input data
        output_data - S3 bucket endpoint for the output data

    Returns:
        None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song-data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data, encoding='UTF-8')

    print(df.count())

    # extract columns to create songs table
    songs_table = df.select(
        col("song_id").alias("song_id"),
        col("title").alias("title"),
        col("artist_id").alias("artist_id"),
        col("year").cast("int").alias("year"),
        col("duration").cast("float").alias("duration")
    )
    
    # write songs table to parquet files partitioned by year and artist
    songs_table\
    .write\
    .partitionBy(["year", "artist_id"])\
    .mode('overwrite')\
    .parquet(os.path.join(output_data, "songs"))
    
    # extract columns to create artists table
    artists_table = df.select(
        col("artist_id").alias("artist_id"),
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").cast("float").alias("latitude"),
        col("artist_longitude").cast("float").alias("longitude")
    ).drop_duplicates()
    
    # write artists table to parquet files
    artists_table\
    .write\
    .mode('overwrite')\
    .parquet(os.path.join(output_data, "artists"))

def process_log_data(spark, input_data, output_data):
    """Reads the songs json files and creates parquet files for time, users
    and the fact table songplays

    Args:
        spark - object for the spark session
        input_data - S3 bucket endpoint for the input data
        output_data - S3 bucket endpoint for the output data

    Returns:
        None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data, encoding='UTF-8')
    
    # filter by actions for song plays
    df = df.filter(df["page"] == "NextSong")

    # extract columns for users table    
    users_table = df.select(
        col("userId").cast("int").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("gender"),
        col("level")
    ).drop_duplicates() 
    
    # write users table to parquet files
    users_table\
    .write\
    .mode('overwrite')\
    .parquet(os.path.join(output_data, "users"))

    # create date_time column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn("date_time", get_datetime("ts"))
  
    # extract columns to create time table
    time_table = df\
    .select(col("ts"),col("date_time"))\
    .withColumnRenamed("ts", "start_time")\
    .withColumn("hour", hour(col("date_time")).cast("int"))\
    .withColumn("day", dayofmonth(col("date_time")).cast("int"))\
    .withColumn("week", weekofyear(col("date_time")).cast("int"))\
    .withColumn("month", month(col("date_time")).cast("int"))\
    .withColumn("year", year(col("date_time")).cast("int"))\
    .withColumn("weekday", date_format(col("date_time"), "E"))\
    .drop_duplicates()

    # Remove date_time from the dataframe
    time_table = time_table.drop(time_table.date_time)
   
    # write time table to parquet files partitioned by year and month
    time_table\
    .write\
    .partitionBy(["year", "month"])\
    .mode('overwrite')\
    .parquet(os.path.join(output_data, "time"))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs"))

    # Add a unique id
    df = df.withColumn("songplay_id", expr("uuid()"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df\
        .join(
           song_df,
            on=((df.length == song_df.duration) & (df.song == song_df.title) & (df.artist == song_df.artist_id)),
            how='left_outer'
        )\
        .join(
            time_table,
            on=(df.ts == time_table.start_time),
            how='left_outer'
        )\
        .select(
            df["songplay_id"],
            df["ts"].alias('start_time'),
            df["userId"].alias('user_id'),
            df["level"],
            song_df["song_id"],
            song_df["artist_id"],
            df["sessionId"].alias("session_id"),
            df["location"],
            df["useragent"].alias("user_agent"),
            time_table["year"],
            time_table["month"]
        ).drop_duplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table\
    .write\
    .partitionBy(["year", "month"])\
    .mode('overwrite')\
    .parquet(os.path.join(output_data, "song_plays"))

def main():
    """Sets up the input and output data paths
    Calls process_song_data and process_log_data to read
    the song and log files, clean the data, transform to requirements
    and write as parquet files to the output S3 bucket

    Args:
        None

    Returns:
        None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://ud-daeng-output/"
    
    songs_table = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
