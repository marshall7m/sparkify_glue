from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from configparser import ConfigParser
from pyspark import SparkConf
config = ConfigParser()
config.read_file(open('/app/config/aws.cfg'))

conf = (
    SparkConf()
        .set('spark.hadoop.fs.s3a.access.key', config.get('AWS', 'KEY'))
        .set('spark.hadoop.fs.s3a.secret.key', config.get('AWS', 'SECRET'))
        .set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)

sc = SparkContext(conf=conf)

spark = SparkSession(sc)

glueContext = GlueContext(spark)
glueContext.createOrReplaceTempView("songplay")

sonplay = spark.sql("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
        INTO 
            songplay
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
             * FROM staging_events WHERE page='NextSong') events
        LEFT JOIN 
            staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)


