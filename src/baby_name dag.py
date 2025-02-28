from datetime import datetime, timedelta
import pendulum
import logging
import os
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


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
        from airflow.providers.http.hooks.http import HttpHook

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
        import glob
        import csv
        import re
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # Define CSV Directory & Table Name
        TABLE_NAME: str = "baby_names.names"

        # Define static column names (since CSV files have no headers)
        COLUMN_NAMES: list[str] = ["year", "name", "gender", "count"]

        # Connect to PostgreSQL
        conn = PostgresHook(postgres_conn_id='AIRFLOW_CONN_POSTGRES_ANALYTICS')
        csv_file_paths: list[str] = glob.glob(os.path.join(work_dir_str + '/names', "*.txt"))
        csv_file_names: list[str] = [os.path.basename(file_name) for file_name in csv_file_paths]

        # Use glob to get all files:
        for path, name in zip(csv_file_paths, csv_file_names):
            print(f"Processing {path=} -- {name=}...")
            year = re.findall('\d+', name)[0]

            batch: list[list[any]] = []
            with open(path, "r") as f:
                reader = csv.reader(f)
                for row in reader:
                    row.insert(0, year)
                    batch.append(row)

            print(f"{batch=}")

            conn.insert_rows(table=TABLE_NAME, rows=batch, target_fields=COLUMN_NAMES, commit_every=0)
            print(f"Finished processing {path=} -- {name=}")

    clean_working_files = BashOperator(
        task_id='clean_working_files',
        bash_command=f'cd {work_dir_str} && rm -R names && rm names.zip'
    )
    
    # Define task dependencies
    extract_baby_name_data() >> clean_zip_files >> upload_baby_names() >> clean_working_files

# Instantiate DAG
etl_baby_names()
