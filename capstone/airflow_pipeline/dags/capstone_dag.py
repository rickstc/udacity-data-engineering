from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from operators.acquire_location_data import download_location_data
from operators.acquire_pl_data import download_pl_data
from operators.acquire_weather_station_data import download_weather_station_data
from operators.check_records_exist import check_records_in_database
from operators.load_fact_athletes import load_fact_athlete
from operators.load_fact_stations import load_fact_station
import os


# Create the DAG
with DAG(
    dag_id="capstone_dag",
    start_date=datetime.today(),
    schedule_interval="@daily",
) as dag:
    # Acquire Powerlifting Data
    powerlifting_acquire = PythonOperator(
        task_id="acquire_powerlifting_data",
        python_callable=download_pl_data,
    )

    # Acquire Location Data
    location_acquire = PythonOperator(
        task_id="acquire_location_data",
        python_callable=download_location_data,
    )

    # Acquire Weather Station Data
    weather_station_acquire = PythonOperator(
        task_id="acquire_station_data",
        python_callable=download_weather_station_data,
    )

    # Athlete
    fact_athlete = PythonOperator(
        task_id="load_fact_athlete",
        python_callable=load_fact_athlete,
        op_kwargs={
            "table_name": "fact_athlete",
            "file_path": "{{ task_instance.xcom_pull(task_ids='acquire_powerlifting_data', key='powerlifting_csv_file_path') }}",
        },
    )

    # Check Athlete Loaded
    check_fact_athlete = PythonOperator(
        task_id="check_athletes_loaded",
        python_callable=check_records_in_database,
        op_kwargs={
            "table_name": "fact_athlete",
        },
    )

    # Weather Station
    fact_station = PythonOperator(
        task_id="load_fact_station",
        python_callable=load_fact_station,
        op_kwargs={
            "table_name": "fact_weatherstation",
            "file_path": "{{ task_instance.xcom_pull(task_ids='acquire_station_data', key='weather_station_text_file_path') }}",
        },
    )

    # Check Weather Station Loaded
    check_fact_station = PythonOperator(
        task_id="check_stations_loaded",
        python_callable=check_records_in_database,
        op_kwargs={
            "table_name": "fact_weatherstation",
        },
    )

    powerlifting_acquire >> fact_athlete >> check_fact_athlete
    weather_station_acquire >> fact_station >> check_fact_station
