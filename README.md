# baby_etl
ETL DAG for SSA baby names data

## Steps
1. Extract baby names zip from SSA API
2. Unzip data files and remove unwanted files
3. Add records to postgres table if current years records have not yet been added to postgres
4. Clean up extracted txt files

## Quick Deploy to Airflow
* `scp -r -P 277 ../baby_etl/src zfreeze@rasp-pi:~/airflow/dags/baby_etl`
