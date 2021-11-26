import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS_CREDS_ADMIN', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS_CREDS_ADMIN', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Returns a spark session.
    :return: spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Downloads song data, creates new tables and saves them in S3.
    :param spark: spark session
    :param input_data: s3 filepath of log data
    :param output_data: s3 filepath of new files
    :return:
    """
    # get filepath to song data file
    song_data = '{}{}'.format(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration'].drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet('{}songs.parquet'.format(output_data), mode='overwrite', partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'].drop_duplicates()

    # write artists table to parquet files
    artists_table.write.parquet('{}artists.parquet'.format(output_data), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Downloads log data, creates new tables and saves them in S3.
    :param spark: spark session
    :param input_data: s3 filepath of log data
    :param output_data: s3 filepath of new files
    :return:
    """
    # get filepath to log data file
    log_data = '{}{}'.format(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(col("song").isNotNull()).drop_duplicates()

    # extract columns for users table
    users_table = df['userId', 'firstName', 'lastName', 'level', 'gender'].drop_duplicates()

    # write users table to parquet files
    users_table.write.parquet('{}users.parquet'.format(output_data), mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(int(x) / 1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))

    # NOTE: I don't think creating a separate
    # datetime column is necessary. We can just use the newly created
    # start_time column to extract the necessary data.
    # # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = None

    # extract columns to create time table
    time_table = df.select(['start_time', 'ts'])
    time_table = time_table.withColumn('hour', hour('start_time')) \
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', dayofweek('start_time'))

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet('{}time.parquet'.format(output_data), mode='overwrite', partitionBy=['year', 'month'])


def create_and_save_songplays_table(spark, input_data, output_data):
    """
    Creates songplays table and saves it in S3.
    :param spark: spark session
    :param input_data: s3 filepath of log data
    :param output_data: s3 filepath of new files
    :return:
    """
    # get filepath to log data file
    log_data = '{}{}'.format(input_data, 'log_data/*/*/*.json')

    # read log data file
    log_df = spark.read.json(log_data)

    # read in song data to use for songplays table
    song_df = spark.read.parquet('{}songs.parquet'.format(output_data))

    # read in time table data to be able to map ts to start_time
    time_df = spark.read.parquet('{}time.parquet'.format(output_data))

    # create temporary views to be able to use sql to join the data more easily
    log_df.createOrReplaceTempView('log_data_view')
    song_df.createOrReplaceTempView('song_data_view')
    time_df.createOrReplaceTempView('time_data_view')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql('''
            SELECT DISTINCT
                monotonically_increasing_id() as songplay_id,
                ts.start_time,
                l.userId AS user_id,
                l.level AS level,
                s.song_id AS song_id,
                s.artist_id AS artist_id,
                l.sessionId  AS session_id,
                l.location AS location,
                l.userAgent AS user_agent
            FROM log_data_view AS l
            JOIN song_data_view AS s ON s.title=l.song AND s.artist_name=l.artist 
            JOIN time_data_view AS t ON l.ts=t.ts
            ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet('{}songplays.parquet'.format(output_data), mode='overwrite', partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config.get('S3', 'BUCKET_ID')
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    create_and_save_songplays_table(spark, input_data, output_data)


if __name__ == "__main__":
    main()
