import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F#year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
import pyspark.sql.types as Spark_DT

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('INFO','AWS_ACCESS_KEY_ID')#['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('INFO','AWS_SECRET_ACCESS_KEY')#config['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    '''
    Creates and returs a spark session.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Processes the artist and song table.
        -Input
            spark - Spark Session
            input_data - s3 bucket with json data
        -Output
            output_data - s3 bucket for data lake
    '''
    # get filepath to song data file
    song_data = input_data + 'song_data' + '/*/*/*/*.json'  #*will read and append any json data to the data frame.

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id',\
        'artist_id',\
        'title',\
        'year',\
        'duration')\
            .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite") \
            .partitionBy("year", "artist_id")\
            .parquet(output_data+'/song_table')

    # extract columns to create artists table
    artists_table = df.select('artist_id',\
        'artist_name', \
        'artist_location',\
        'artist_latitude', \
        'artist_longitude')\
            .dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite")\
            .parquet(output_data+'/artists_table')


def process_log_data(spark, input_data, output_data):
    '''
    Processes the artist and song table.
        -Input
            spark - Spark Session
            input_data - s3 bucket with json data
        -Output
            output_data - s3 bucket for data lake
    '''
    # get filepath to log data file
    log_data = input_data + 'log_data' + '/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    user_table = df.select('userId',\
        'firstName',\
        'lastName', \
        'gender',\
        'level', )\
            .repartition(2).dropDuplicates()

    # write users table to parquet files
    user_table.write.mode('overwrite')\
            .parquet(output_data + '/user_table')

    # create timestamp column from original timestamp column

    def get_ts (ts):
        return datetime.fromtimestamp(int(ts) / 1000.0)

    get_dt = udf(lambda z: get_ts(z), TimestampType())


    # create datetime column from original timestamp column

    df_time = df.select('ts')\
            .dropDuplicates()\
            .withColumn('datetime', get_dt('ts'))

    # extract columns to create time table
    time_table = df_time.select(F.col('ts').alias('starttime'),\
        F.hour('datetime').alias('hour'),\
        F.dayofmonth('datetime').alias('day'),\
        F.weekofyear('datetime').alias('week'),\
        F.month('datetime').alias('month'),\
        F.year('datetime').alias('year'),\
        F.dayofweek('datetime').alias('weekday'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite")\
                    .partitionBy("year", "month")\
                    .parquet(output_data + '/time_table')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data' + '/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df.artist == song_df.artist_name) \
            .select('ts',\
                    'userId',\
                    'song_id',\
                    'artist_id',\
                    'level',\
                    'sessionId',\
                    'location',\
                    'userAgent')\
                    .withColumn('datetime', get_dt('ts'))\
                    .select(F.col('ts').alias('starttime'),\
                        F.col('userId'),\
                        F.col('artist_id'),\
                        F.col('level'),\
                        F.col('sessionId'),\
                        F.col('location'),\
                        F.col('userAgent'),\
                        F.month('datetime').alias('month'),\
                        F.year('datetime').alias('year'),\
                        F.dayofweek('datetime').alias('weekday'))\
                        .withColumn('songplay_id', F.monotonically_increasing_id())\
                        .dropDuplicates()



    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite")\
                    .partitionBy("year", "month")\
                    .parquet(output_data + '/songplays_table')


def main():
    '''
    runs all funtions above and terminates spark job at the end.
        Output:
            s3a://s3-for-emr-cluster2 contains all processed tables.
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://s3-for-emr-cluster2"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    print('\nThe Sript\nIs Now\nDone\n') # use for command line feedback!

    spark.stop()#prevents cluster from hanging


if __name__ == "__main__":
    main()
