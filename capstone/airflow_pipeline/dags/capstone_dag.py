from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from operators.acquire_pl_data import download_pl_data
from operators.load_fact_athletes import load_fact_athlete
from operators.check_records_exist import check_records_in_database
import os


# Create the DAG
with DAG(
    dag_id="capstone_dag",
    start_date=datetime.today(),
    schedule_interval="@daily",
) as dag:
    # Create operators for each data source
    powerlifting_csv = PythonOperator(
        task_id="acquire_pl_data",
        python_callable=download_pl_data,
    )

    fact_athlete = PythonOperator(
        task_id="load_fact_athlete",
        python_callable=load_fact_athlete,
        op_kwargs={
            "table_name": "fact_athlete",
            "file_path": "{{ task_instance.xcom_pull(task_ids='acquire_pl_data', key='powerlifting_csv_file_path') }}",
        },
    )

    check_fact_athlete = PythonOperator(
        task_id="check_athletes_loaded",
        python_callable=check_records_in_database,
        op_kwargs={
            "table_name": "fact_athlete",
        },
    )

    powerlifting_csv >> fact_athlete >> check_fact_athlete
