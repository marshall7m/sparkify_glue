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

# RUN:    
# gluesparksubmit app/glue_etl_scripts/log_data.py --JOB-NAME test


# sc = SparkContext.getOrCreate()
# hadoop_conf = sc._jsc.hadoopConfiguration()

# hadoop_conf.set('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
# hadoop_conf.set("fs.s3.awsAccessKeyId", config.get('AWS', 'KEY'))
# hadoop_conf.set("fs.s3.awsSecretAccessKey", config.get('AWS', 'SECRET'))
# hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# hadoop_conf.set("fs.s3a.access.key", config.get('AWS', 'KEY'))
# hadoop_conf.set("fs.s3a.secret.key", config.get('AWS', 'SECRET'))

# hadoop_conf.set("fs.s3a.awsAccessKeyId", config.get('AWS', 'KEY'))
# hadoop_conf.set("fs.s3a.awsSecretAccessKey", config.get('AWS', 'SECRET'))
# hadoop_conf.set("fs.s3a.endpoint", "s3a.us-west-2.amazonaws.com")

# hadoop_conf.set('fs.s3.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf = (
    SparkConf()
        .set('spark.hadoop.fs.s3a.access.key', config.get('AWS', 'KEY'))
        .set('spark.hadoop.fs.s3a.secret.key', config.get('AWS', 'SECRET'))
        .set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)

sc = SparkContext(conf=conf)

spark = SparkSession(sc)

try:
    print('Attempt 1: spark.read.json')
    url = 's3a://sparkify-dend-analytics/song_data/A/A/A/TRAAAAW128F429D538.json'
    spark.read.json(url).show(1)
except Exception as e:
    print(e)

glueContext = GlueContext(spark)
try:
    print('Attempt 2: create_dynamic_frame.from_options')
    song_df = glueContext.create_dynamic_frame.from_options(
            connection_type='s3',
            connection_options={"paths": [ "s3a://sparkify-dend-analytics"]},
            format='json')

    print ('Count: ', song_df.count())
    print('Schema: ')
    song_df.printSchema()
except Exception as e:
    print(e)

try:
    print('Attempt 3: create_dynamic_frame.from_catalog')
    song_df = glueContext.create_dynamic_frame.from_catalog(
            database='sparkify',
            table_name='song_data')

    print ('Count: ', song_df.count())
    print('Schema: ')
    song_df.printSchema()
except Exception as e:
    print(e)

try:
    print('Attempt 3: create_dynamic_frame_from_catalog')
    song_df = glueContext.create_dynamic_frame_from_catalog(
            database='sparkify',
            table_name='song_data')

    print ('Count: ', song_df.count())
    print('Schema: ')
    song_df.printSchema()
except Exception as e:
    print(e)



# l_history = Join.apply(orgs,
#                        Join.apply(persons, memberships, 'id', 'person_id'),
#                        'org_id', 'organization_id').drop_fields(['person_id', 'org_id'])
# print "Count: ", l_history.count()
# l_history.printSchema()