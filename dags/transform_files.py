from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook as AzDLHook
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from include.utils import *
from datetime import datetime, timedelta
import os
from include import loading_scripts_sql


default_args = {
    'owner': 'Emmanuel',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='transform_files_on_databricks',
    default_args=default_args,
    start_date=datetime(2023, 10, 2),
    schedule_interval='@once',
    template_searchpath="/usr/local/airflow/include"
) as dag:
    json = {
    "new_cluster": {"spark_version": "2.1.0-db3-scala2.11", "num_workers": 2},
    "notebook_task": {
        "notebook_path": "/Users/airflow@example.com/PrepareData",
    },
}
    # notebook_transform_user_reviews = DatabricksRunNowOperator(
    #     task_id = 'notebook_transform_user_reviews',
    #     databricks_conn_id = 'databricks_conn',
    #     job_id = DATABRICKS_JOB_ID
    # )

    create_data_warehouse = PostgresOperator(
        task_id = 'create_data_warehouse',
        postgres_conn_id = POSTGRES_CONN_ID,
        sql="create_data_warehouse.sql"
    )

    get_name_of_dim_device_file = PythonOperator(
        task_id= 'get_name_of_dim_device_file',
        python_callable=get_latest_dim_csv_file_name,
        op_kwargs={
            'container_name': 'results',
            'dim_name': 'device'
        }
    )

    load_dim_device_table = PythonOperator(
        task_id = 'load_dim_device_table',
        python_callable=load_csv_dim_data_from_staging_area,
        op_kwargs={
            'table_name': 'dw.dim_devices',
            'data_lake_folder': 'device',
            'container_name': "results",
        }
    )

    get_name_of_dim_os_file = PythonOperator(
        task_id= 'get_name_of_dim_os_file',
        python_callable=get_latest_dim_csv_file_name,
        op_kwargs={
            'container_name': 'results',
            'dim_name': 'os'
        }
    )

    load_dim_os_table = PythonOperator(
        task_id = 'load_dim_os_table',
        python_callable=load_csv_dim_data_from_staging_area,
        op_kwargs={
            'table_name': 'dw.dim_os',
            'data_lake_folder': 'os',
            'container_name': "results",
        }
    )

    
    get_name_of_dim_location_file = PythonOperator(
        task_id= 'get_name_of_dim_location_file',
        python_callable=get_latest_dim_csv_file_name,
        op_kwargs={
            'container_name': 'results',
            'dim_name': 'location'
        }
    )

    load_dim_location_table = PythonOperator(
        task_id = 'load_dim_location_table',
        python_callable=load_csv_dim_data_from_staging_area,
        op_kwargs={
            'table_name': 'dw.dim_location',
            'data_lake_folder': 'location',
            'container_name': "results",
        }
    )
    
    get_name_of_dim_date_file = PythonOperator(
        task_id= 'get_name_of_dim_date_file',
        python_callable=get_latest_dim_csv_file_name,
        op_kwargs={
            'container_name': 'results',
            'dim_name': 'date'
        }
    )

    load_dim_date_table = PythonOperator(
        task_id = 'load_dim_date_table',
        python_callable=load_csv_dim_data_from_staging_area,
        op_kwargs={
            'table_name': 'dw.dim_date',
            'data_lake_folder': 'date',
            'container_name': "results",
        }
    )
    
    
    # # Write script to load data into dimension table
    # load_dimension_device = PostgresOperator(
    #     task_id = 'load_dimension_device',
    #     postgres_conn_id = POSTGRES_CONN_ID,
    #     sql=loading_scripts_sql.load_data_into_dim_device
    # )
    
    # notebook_transform_user_reviews >> create_data_warehouse 

    create_data_warehouse >> (get_name_of_dim_device_file, get_name_of_dim_os_file, get_name_of_dim_location_file, get_name_of_dim_date_file) 
    get_name_of_dim_device_file >> load_dim_device_table
    get_name_of_dim_os_file >> load_dim_os_table
    get_name_of_dim_location_file >> load_dim_location_table
    get_name_of_dim_date_file >> load_dim_date_table

