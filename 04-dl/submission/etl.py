"""
File: etl.py
Attribution: Mixed
Background:
The boilerplate of this file, along with instructional inline comments, was provided by Udacity.
The student was to fill in code, where required, to accomplish the stated goals of the project,
conforming to the prompts in the inline comments.

This script contains all logic necessary for the ETL pipeline. The script will create a spark session,
load data from the Udacity-provided S3 bucket, create dimensional tables based on the raw log data,
and then output those as a series of parquet files.
"""

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import shutil
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session() -> SparkSession:
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes song data, creating song and artist tables.

    Parameters:
        spark : SparkSession
        input_data : The base path where the log files are stored
        output_data : The base path where the parquet files will be stored
    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration'
    )

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        f"{output_data}/songs/",
        partitionBy=['year', 'artist_id']
    )

    # extract columns to create artists table
    artists_table = df.selectExpr(
        'artist_id',
        'artist_name as name',
        'artist_location as location',
        'artist_latitude as latitude',
        'artist_longitude as longitude'
    )

    # write artists table to parquet files
    artists_table.write.parquet(
        f"{output_data}/artists/"
    )


def process_log_data(spark, input_data, output_data):
    """
    Processes log data, creating user, time, and songplay tables

    Parameters:
        spark : SparkSession
        input_data : The base path where the log files are stored
        output_data : The base path where the parquet files will be stored
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table
    users_table = df.selectExpr(
        'userId as user_id',
        'firstName as first_name',
        'lastName as last_name',
        'gender',
        'level'
    ).dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(
        f"{output_data}/users/"
    )

    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('start_time', get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select('start_time') \
        .dropDuplicates() \
        .withColumn('hour', hour(col('start_time'))) \
        .withColumn('day', dayofmonth(col('start_time'))) \
        .withColumn('week', weekofyear(col('start_time'))) \
        .withColumn('month', month(col('start_time'))) \
        .withColumn('year', year(col('start_time'))) \
        .withColumn('weekday', date_format(col('start_time'), 'E'))

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
        f"{output_data}/time/",
        partitionBy=['year', 'month']
    )

    # read in song data to use for songplays table
    song_data = spark.read.format(
        'parquet'
    ).option(
        'basePath', os.path.join(output_data, "songs/")
    ).load(
        os.path.join(output_data, 'songs/*/*/')
    )

    sp_df = df.join(
        song_data, df.song == song_data.title, how='inner'
    )

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = sp_df.withColumn(
        'songplay_id', monotonically_increasing_id()
    ).selectExpr(
        'songplay_id',
        'start_time',
        'userId as user_id',
        'level',
        'song_id',
        'artist_id',
        'sessionId as session_id',
        'location',
        'userAgent as user_agent',
    ).withColumn(
        'month', month(col('start_time'))).withColumn(
        'year', year(col('start_time'))
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        f"{output_data}/songplays/",
        partitionBy=['year', 'month']
    )


def main():
    """
    Main method, begins the processing of data
    """
    spark = create_spark_session()

    input_data = "s3://udacity-dend/"
    output_data = "s3://rickstc-ude/dl/"

    # This block of code allowed the student to first test locally
    # before deploying to AWS EMR
    data_loc = os.environ.get('data_location', 'remote')
    if data_loc == 'local':

        input_data = './data/'
        output_data = "./output/"

        if os.path.exists(output_data):
            shutil.rmtree(output_data)

    # Process the log data
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
