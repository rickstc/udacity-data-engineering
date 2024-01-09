from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from operators.check_records_exist import check_records_in_database
from operators.clear_tables import clear_table
from operators.load_dimension_contests import load_dimension_contest
from operators.load_dimension_locations import load_dimension_location


# Create the DAG
with DAG(
    dag_id="dimension_dag",
    start_date=datetime.today(),
    schedule_interval="@monthly",
) as dag:
    clear_dim_loc = PythonOperator(
        task_id="clear_dimension_loc",
        python_callable=clear_table,
        op_kwargs={"table_name": "dimension_location"},
    )

    clear_dim_contest = PythonOperator(
        task_id="clear_dimension_contest",
        python_callable=clear_table,
        op_kwargs={"table_name": "dimension_contest"},
    )

    # Dimension Location
    dim_location = PythonOperator(
        task_id="load_dim_location",
        python_callable=load_dimension_location,
        op_kwargs={
            "table_name": "dimension_location",
        },
    )

    # Check Location Result Loaded
    check_dim_location = PythonOperator(
        task_id="check_dim_location_loaded",
        python_callable=check_records_in_database,
        op_kwargs={
            "table_name": "dimension_location",
        },
    )

    # Dimension Contest
    dim_contest = PythonOperator(
        task_id="load_dim_contest",
        python_callable=load_dimension_contest,
        op_kwargs={
            "table_name": "dimension_contest",
        },
    )

    # Check Contest Result Loaded
    check_dim_contest = PythonOperator(
        task_id="check_dim_contest_loaded",
        python_callable=check_records_in_database,
        op_kwargs={
            "table_name": "dimension_contest",
        },
    )

    # Dimension Location
    clear_dim_loc >> dim_location >> check_dim_location

    check_dim_location >> dim_contest

    dim_contest >> check_dim_contest
