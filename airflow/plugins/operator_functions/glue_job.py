import boto3
from botocore.exceptions import ClientError
import time

def glue_job(job_name, job_type, role, local_path, s3_key, bucket, overwrite=False):
    """
    job_type -- Environment used to execute job (see: https://docs.aws.amazon.com/glue/latest/dg/add-job.html)
    """
    job_type_dict = {
        'python': 'pythonshell',
        'spark': 'glueetl',
        'streaming': 'gluestreaming'
    }
    s3 = boto3.resource('s3')

    if overwrite == True:
        try:
            response = s3.Object(bucket, s3_key).put(Body=open(local_path, 'rb'))
            print(f'Overwritting current ETL script under {s3_key}')
        except Exception as e:
            print(e)
    else:
        try:
            file = s3.Object(bucket, s3_key).load()
            print(f'ETL script already exists under: {s3_key}')
        except ClientError as e:
            try:
                response = s3.Object(bucket, s3_key).put(Body=open(local_path, 'rb'))
                print(f'ETL script successfully added under: {s3_key}')
            except Exception as e:
                print(e)
    command_name = job_type_dict[job_type]
    job = glue.create_job(Name=job_name, Role=role,
                        Command={'Name': command_name,
                                'ScriptLocation': f's3://{bucket}/{s3_key}'})

    job_run = glue.start_job_run(JobName=job['Name'])
    status = glue.get_job_run(JobName=job['Name'], RunId=job_run['JobRunId'])['JobRun']['JobRunState']

    while status != 'SUCCEEDED' or status != 'STOPPED':
        time.sleep(20)
        status = glue.get_job_run(JobName=job['Name'], RunId=job_run['JobRunId'])['JobRun']['JobRunState']
        
    print(f'Job status: {status}')
