from airflow.decorators import dag , task
from airflow import DAG
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
from tempfile import NamedTemporaryFile
from include.utils import *

from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'Emmanuel',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='connect_load_data_etl',
    default_args=default_args,
    start_date=datetime(2023, 10, 2),
    schedule_interval='@once',
    tags=['loading']
) as dag:
    
    # Start Workflow
    start_workflow = DummyOperator(task_id="start_workflow")
    
    # Check to ensure that the file exists in the azure storage bucket
    confirm_file_existence_in_azure = ShortCircuitOperator(
        task_id='confirm_file_existence_in_azure',
        python_callable=check_if_file_exists,
        op_kwargs={
            'file_name': AZ_FILE_NAME,
            'container_name': AZURE_CONTAINER_NAME
        }
    )

    # Create the postgres Table.
    create_postgres_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f""" CREATE SCHEMA IF NOT EXISTS user_purchase_schema;
                CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
                    invoice_number varchar(10),
                    stock_code varchar(20),
                    detail varchar(1000),
                    quantity int,
                    invoice_date timestamp,
                    unit_price numeric(8,3),
                    customer_id int,
                    country varchar(20)
                    );
                """
    )

#     # This task loads a sample data to postgres for testing purposes
#     load_sample_data = PostgresOperator(
#         task_id='load_sample_data',
#         postgres_conn_id=POSTGRES_CONN_ID,
#         sql=f"""
#                 INSERT INTO {POSTGRES_TABLE} 
#                 VALUES(536365, '85123A', 'WHITE HANGING HEART T-LIGHT HOLDER', 6, '12/1/2010  8:26:00 AM', 2.55, 17850, 'United Kingdom'); 
        
# """
#     )

    continue_process = DummyOperator(task_id='continue_process')

    clear_table = PostgresOperator(
        task_id="clear_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""DELETE FROM {POSTGRES_TABLE}""",
    )


    # Transfer the data from azure to postgres
    load_azure_data_to_postgres = PythonOperator(
        task_id="load_azure_data_to_postgres",
        python_callable=load_data_from_azure_bucket,
        op_kwargs={
            'az_container_name': AZURE_CONTAINER_NAME,
            'az_object_name': AZ_FILE_NAME,
            'postgres_table': POSTGRES_TABLE
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    validate_table_is_empty = BranchSQLOperator(
        task_id='validate_data',
        conn_id=POSTGRES_CONN_ID,
        sql=f"SELECT COUNT(*) AS total_rows FROM {POSTGRES_TABLE}",
        follow_task_ids_if_false=[continue_process.task_id],
        follow_task_ids_if_true=[clear_table.task_id],

    )
    end_workflow = DummyOperator(task_id="end_workflow")

    # create_postgres_table >> add_sample_data_to_postgres >> 
    
    
    (
        start_workflow 
        >> confirm_file_existence_in_azure 
        >> create_postgres_table
        >> validate_table_is_empty
        )
    
    validate_table_is_empty >> [continue_process, clear_table ]  >> load_azure_data_to_postgres 
    load_azure_data_to_postgres >> end_workflow

    dag.doc_md = __doc__