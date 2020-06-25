from datetime import datetime, timedelta
import os
from configparser import ConfigParser

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from operator_functions.crawl_source_s3 import crawl_source_s3
from operator_functions.glue_job import glue_job


config = ConfigParser()
config.read_file(open('/config/aws.cfg'))

default_args = {
    'owner': config.get('DL', 'DL_DB_USER'),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(dag_id='sparkify_analytics',
          default_args=default_args,
          description='Loads data from S3 to Athena',
          schedule_interval='0 0 * * *',
          start_date=datetime(year=2018, month=11, day=11),
          end_date=datetime(year=2018, month=11, day=13),
          max_active_runs=1)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

crawl_source_s3_operator = PythonOperator(
        task_id='crawl_source_s3',
        python_callable=crawl_source_s3,
        op_kwargs={
            'crawler_name': config.get('AWS', 'CRAWLER'), 
            'bucket': config.get('AWS', 'BUCKET'),
            's3_key_format': 's3://{bucket}/log_data/{year}/{month}', 
            'region': config.get('AWS', 'REGION'),
            'aws_conn_id':config.get('AWS', 'AWS_CONN_ID')},
        dag=dag,
        provide_context=True)

load_songplays_fact_table = PythonOperator(
    task_id='load_songplay',
    python_callable=glue_job,
    op_kwargs={
        'job_name': 'load_songplay', 
        'job_type': 'spark', 
        'role': config.get('DL', 'DL_IAM_ROLE_NAME'), 
        'local_path':'glue_etl_scripts/load_songplay.py', 
        's3_key': 'glue_etl_scripts/load_songplay.py', 
        'bucket': config.get('AWS', 'BUCKET'), 
        'overwrite'=True
    },
    dag=dag)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql=SqlQueries.users_table_insert)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql=SqlQueries.songs_table_insert
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql=SqlQueries.artists_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql=SqlQueries.time_table_insert
)


start_operator >> crawl_source_s3_operator

# crawl_source_s3_operator = AwsGlueJobOperator(
#     task_id='crawl_source_s3',
#     job_name='test_job',
#     job_desc='AWS Glue Job with Airflow',
#     script_location='plugins/operator_functions/crawl_source_s3.py',
#     concurrent_run_limit=2,
#     script_args={
#             'crawler_name': config.get('AWS', 'CRAWLER'), 
#             'bucket': config.get('AWS', 'BUCKET'),
#             's3_key_format': 's3://{bucket}/log_data/{year}/{month}', 
#             'region': config.get('AWS', 'REGION'),
#             'aws_conn_id':config.get('AWS', 'AWS_CONN_ID')},
#     retry_limit=3,
#     num_of_dpus=6,
#     aws_conn_id=config.get('AWS', 'AWS_CONN_ID'),
#     region_name=config.get('AWS', 'REGION'),
#     s3_bucket=config.get('AWS', 'BUCKET'),
#     iam_role_name=config.get('AWS', 'DL_IAM_ROLE_NAME'),
#     dag=dag,
#     provide_context=True
# )

# stage_events = SubDagOperator(
#     subdag=stage_s3_to_redshift(
#         parent_dag_name=main_dag.dag_id,
#         child_dag_name='stage_events',
#         start_date=main_dag.start_date,
#         end_date=main_dag.end_date,
#         schedule_interval=main_dag.schedule_interval,
#         redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
#         table='stage_events',
#         error_table='stage_events_errors',
#         create_sql=SqlQueries.create_stage_events,
#         s3_bucket=config.get('AWS','BUCKET'),
#         s3_key='s3://{bucket}/{file_path}.{file_format}',
#         iam_role=config.get('DWH', 'DWH_ROLE_ARN'),
#         region=config.get('DWH', 'REGION'),
#         file_format='CSV'),
#     task_id='staging_events',
#     dag=dag)

# stage_songs = SubDagOperator(
#     subdag=stage_s3_to_redshift(
#         parent_dag_name=main_dag.dag_id,
#         child_dag_name='stage_songs',
#         start_date=main_dag.start_date,
#         end_date=main_dag.end_date,
#         schedule_interval=main_dag.schedule_interval,
#         redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
#         table='stage_songs',
#         error_table='stage_songs_errors',
#         create_sql=SqlQueries.create_stage_songs,
#         s3_bucket=config.get('AWS','BUCKET'),
#         s3_key='s3://{bucket}/{file_path}.{file_format}',
#         iam_role=config.get('DWH', 'DWH_ROLE_ARN'),
#         region=config.get('DWH', 'REGION'),
#         file_format='CSV'),
#     task_id='staging_events',
#     dag=dag)


# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag,
#     redshift_conn_id='redshift',
#     table_list=['songplays', 'artists', 'songs', 'users', 'time']
# )

# end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# start_operator >> table_created_check

# table_created_check >> [stage_events_to_redshift, stage_songs_to_redshift]

# stage_events_to_redshift >> [load_songplays_fact_table, load_users_dimension_table]
# stage_songs_to_redshift >> [load_songplays_fact_table, load_artists_dimension_table, load_songs_dimension_table]

# load_songplays_fact_table >> load_time_dimension_table
# [load_artists_dimension_table, load_songs_dimension_table, load_users_dimension_table, load_time_dimension_table] >> run_quality_checks

# run_quality_checks >> end_operator



