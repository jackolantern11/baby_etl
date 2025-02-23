from datetime import datetime, timedelta
import pendulum
import os
from pathlib import Path
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.providers.http.hooks.http import HttpHook


# Define default arguments
default_args = {
    'owner': 'zfreeze',
    'retries': 2,
    'retry_delay': timedelta(minutes=30)
}

# DAG definition using @dag decorator
@dag(
    start_date=datetime(2024, 5, 1, 14, 0, 0, tzinfo=pendulum.timezone("US/Central")),
    end_date=None,
    schedule=None, # '0 9 20 5 *',
    dag_id="baby_name_etl",
    dagrun_timeout=timedelta(minutes=5),
    catchup=False,
    description="baby name etl from SSA records",
    max_active_runs=3,
    tags=['Baby Names'],
    default_args=default_args
)
def etl_baby_names():

    # Config
    work_dir_str: str = '/home/zfreeze/airflow/dags/baby_etl'
    os.chdir(work_dir_str)

    @task
    def extract_baby_name_data():
        # Download Baby Name Data to Central Location
        response = HttpHook(method='GET', http_conn_id='AIRFLOW_CONN_SSA').run(endpoint='oact/babynames/names.zip')

        # Write Response Content to File System
        with open("names.zip", "wb") as temp_file:
            temp_file.write(response.content)


    clean_zip_files = BashOperator(
        task_id='clean_zip_files',
        bash_command=f'cd {work_dir_str} && unzip names.zip -d names && find ./names -type f -not -name "*.txt" -delete'
    )

    @task
    def upload_baby_names():
        # Upload CSV -> tuples -> insert rows to Postgres using Python
        print(f"To be implemented!")
        pass

    clean_working_files = BashOperator(
        task_id='clean_working_files',
        bash_command=f'cd {work_dir_str} && rm -R names && rm names.zip'
    )
    
    # Define task dependencies
    extract_baby_name_data() >> clean_zip_files >> upload_baby_names() >> clean_working_files

# Instantiate DAG
etl_baby_names()
