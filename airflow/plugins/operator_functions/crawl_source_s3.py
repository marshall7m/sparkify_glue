import boto3
import configparser
from airflow.contrib.hooks.aws_hook import AwsHook
import time

def crawl_source_s3(crawler_name, bucket, s3_key_format, region, aws_conn_id, **kwargs):
    aws = AwsHook(aws_conn_id)
    session = aws.get_session()
    glue = session.client(service_name='glue', region_name=region)

    # crawler = glue.get_crawler(Name=crawler_name)

    year = kwargs['execution_date'].year
    month = kwargs['execution_date'].month
    day = kwargs['execution_date'].day

    s3_key_dict = {
        'bucket': bucket,
        'year': year,
        'month': month,
        'day': day
    }

    crawl_path = s3_key_format.format(**s3_key_dict)
    print('Updating S3Targets path to: ', crawl_path)
    response = glue.update_crawler(Name=crawler_name,
                           Targets={'S3Targets': [{'Path':crawl_path}]})

    print('Starting Crawler')
    response = glue.start_crawler(Name=crawler_name)

    crawler = glue.get_crawler(Name=crawler_name)
    crawler_state = crawler['Crawler']['State']
    sleep_secs = 100

    while crawler_state != 'READY':
        time.sleep(sleep_secs)
        metrics = glue.get_crawler_metrics(CrawlerNameList=[crawler_name])['CrawlerMetricsList'][0]
        if metrics['StillEstimating'] == True:
            pass
        else:
            time_left = int(metrics['TimeLeftSeconds'])
            if time_left > 0:
                print('Estimated Time Left: ', time_left)
                sleep_secs = time_left
            else:
                print('Crawler should finish soon')
                crawler = glue.get_crawler(Name=crawler_name)
        # refresh crawler state
        crawler = glue.get_crawler(Name=crawler_name)
        crawler_state = crawler['Crawler']['State']

    metrics = glue.get_crawler_metrics(CrawlerNameList=[crawler_name])['CrawlerMetricsList'][0]
    print('Table Metrics')
    print('Number of Tables Created: ', metrics['TablesCreated'])
    print('Number of Tables Updated: ', metrics['TablesUpdated'])
    print('Number of Tables Deleted: ', metrics['TablesDeleted'])

    print('Crawler is done')