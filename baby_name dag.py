from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    tags=['demo'],
    default_args=default_args
)
def etl_baby_names():

    @task
    def extract_baby_name_data():
        
        # Download Baby Name Data to Central Location
        response = HttpHook(method='GET', http_conn_id='AIRFLOW_CONN_SSA').run(endpoint='oact/babynames/names.zip')

        # Write Response Content to File System
        with open("/home/zfreeze/airflow/dags/names.zip", "wb") as temp_file:
            temp_file.write(response.content)

    # Bash to extract files from zip
    # unzip names.zip -d names

    # Bash to remove non-txt files
    # find ./names -type f -not -name "*.txt" -delete

    # Bash task remove files not matching the desired year

    @task
    def upload_baby_names():
        # Upload CSV -> tuples -> insert rows to Postgres using Python
        pass

    # Bash to clean up all files in the directory
    # rm -R names && rm names.zip
    
    # Define task dependencies
    extract_baby_name_data()

# Instantiate DAG
etl_baby_names()
