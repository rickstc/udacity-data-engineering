from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


"""
Define the dag with the following default parameters:
    - The DAG does not have dependencies on past runs
    - On failure, the task are retried 3 times
    - Retries happen every 5 minutes
    - Catchup is turned off
    - Do not email on retry

References:
    - DAG: https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html#airflow.models.dag.DAG 
    - Operator: https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html
"""

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False
}

""" Define the DAG """

dag = DAG(
    'udacity_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)

""" Define Operators """

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    conn_id='redshift',
    aws_creds='aws_credentials',
    table_name='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    reload=False
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    conn_id='redshift',
    aws_creds='aws_credentials',
    table_name='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    reload=False
)

load_songplays_table = LoadDimensionOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    query=SqlQueries.songplay_table_insert,
    conn_id='redshift',
    table_name='songplays',
    reload=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dimension_table',
    dag=dag,
    query=SqlQueries.user_table_insert,
    conn_id='redshift',
    table_name='users',
    reload=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dimension_table',
    dag=dag,
    query=SqlQueries.song_table_insert,
    conn_id='redshift',
    table_name='songs',
    reload=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dimension_table',
    dag=dag,
    query=SqlQueries.artist_table_insert,
    conn_id='redshift',
    table_name='artists',
    reload=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dimension_table',
    dag=dag,
    query=SqlQueries.time_table_insert,
    conn_id='redshift',
    table_name='time',
    reload=True
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    conn_id='redshift',
    tables=['artists', 'songplays', 'songs', 'time', 'users']
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

""" Define the Task Execution Order """

# Begin Execution by Staging Songs and Events
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# Populate the Fact Tables
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# Populate Dimension Tables
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# Run Data Quality Checks
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# End Execution
run_quality_checks >> end_operator
