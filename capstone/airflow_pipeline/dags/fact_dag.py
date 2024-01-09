from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from operators.acquire_location_data import download_location_data
from operators.acquire_pl_data import download_pl_data
from operators.acquire_weather_station_data import download_weather_station_data
from operators.check_records_exist import check_records_in_database
from operators.clear_tables import clear_table
from operators.load_fact_athletes import load_fact_athlete
from operators.load_fact_contests import load_fact_contest
from operators.load_fact_locations import load_fact_location
from operators.load_fact_results import load_fact_result
from operators.load_fact_stations import load_fact_station


# Create the DAG
with DAG(
    dag_id="fact_dag",
    start_date=datetime.today(),
    schedule_interval="@monthly",
) as dag:
    # Acquire Powerlifting Data
    clear_station = PythonOperator(
        task_id="clear_weather_stations",
        python_callable=clear_table,
        op_kwargs={"table_name": "fact_weatherstation"},
    )

    clear_athlete = PythonOperator(
        task_id="clear_athletes",
        python_callable=clear_table,
        op_kwargs={"table_name": "fact_athlete"},
    )

    clear_location = PythonOperator(
        task_id="clear_locations",
        python_callable=clear_table,
        op_kwargs={"table_name": "fact_contestlocation"},
    )

    clear_contest = PythonOperator(
        task_id="clear_contests",
        python_callable=clear_table,
        op_kwargs={"table_name": "fact_contest"},
    )

    clear_result = PythonOperator(
        task_id="clear_results",
        python_callable=clear_table,
        op_kwargs={"table_name": "fact_contestresult"},
    )

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

    # Contest Location
    fact_location = PythonOperator(
        task_id="load_fact_location",
        python_callable=load_fact_location,
        op_kwargs={
            "table_name": "fact_contestlocation",
            "file_path": "{{ task_instance.xcom_pull(task_ids='acquire_powerlifting_data', key='powerlifting_csv_file_path') }}",
            "location_fp": "{{ task_instance.xcom_pull(task_ids='acquire_location_data', key='location_csv_file_path') }}",
        },
    )

    # Check Contest Location Loaded
    check_fact_location = PythonOperator(
        task_id="check_location_loaded",
        python_callable=check_records_in_database,
        op_kwargs={
            "table_name": "fact_contestlocation",
        },
    )

    # Contests
    fact_contest = PythonOperator(
        task_id="load_fact_contest",
        python_callable=load_fact_contest,
        op_kwargs={
            "table_name": "fact_contest",
            "file_path": "{{ task_instance.xcom_pull(task_ids='acquire_powerlifting_data', key='powerlifting_csv_file_path') }}",
            "location_table_name": "fact_contestlocation",
        },
    )

    # Check Contests Loaded
    check_fact_contest = PythonOperator(
        task_id="check_contest_loaded",
        python_callable=check_records_in_database,
        op_kwargs={
            "table_name": "fact_contest",
        },
    )

    # Contest Results
    fact_result = PythonOperator(
        task_id="load_fact_result",
        python_callable=load_fact_result,
        op_kwargs={
            "table_name": "fact_contestresult",
            "file_path": "{{ task_instance.xcom_pull(task_ids='acquire_powerlifting_data', key='powerlifting_csv_file_path') }}",
            "location_table_name": "fact_contestlocation",
            "athletes_table_name": "fact_athlete",
            "contests_table_name": "fact_contest",
        },
    )

    # Check Contest Result Loaded
    check_fact_result = PythonOperator(
        task_id="check_result_loaded",
        python_callable=check_records_in_database,
        op_kwargs={
            "table_name": "fact_contestresult",
        },
    )

    # Clear Tables
    clear_athlete >> fact_athlete
    clear_location >> fact_location
    clear_contest >> fact_contest
    clear_result >> fact_result
    clear_station >> fact_station

    # Contest Results Must be deleted first
    clear_result >> clear_contest
    clear_contest >> clear_location

    # Data Acquisition Dependencies
    powerlifting_acquire >> fact_athlete
    powerlifting_acquire >> fact_location
    powerlifting_acquire >> fact_contest
    powerlifting_acquire >> fact_result

    weather_station_acquire >> fact_station
    location_acquire >> fact_location

    # Data Load Checks
    fact_athlete >> check_fact_athlete
    fact_station >> check_fact_station
    fact_location >> check_fact_location
    fact_result >> check_fact_result
    fact_contest >> check_fact_contest

    # Tables Depending on Tables
    check_fact_location >> fact_contest

    # Contest Results needs athletes, locations, contests
    check_fact_athlete >> fact_result
    check_fact_location >> fact_result
    check_fact_contest >> fact_result
