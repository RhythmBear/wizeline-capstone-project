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
from datetime import datetime, timedelta
import os

# Defining Constants
AZ_CONN_ID = 'azure_storage'
POSTGRES_CONN_ID = 'azure_database'
POSTGRES_TABLE = 'user_purchase_schema.user_purchase'
AZURE_CONTAINER_NAME = 'csv-files'
AZ_FILE_NAME = 'user_purchase_updated.csv'


def check_if_file_exists(
        container_name,
        file_name
):
    az_hook = WasbHook(wasb_conn_id="azure_storage")

    # Check if the file exists 
    file_exists = az_hook.check_for_blob(container_name=container_name, blob_name=file_name)

    return file_exists

# Defining Functions
def load_data_from_azure_bucket(
        az_container_name: str,
        az_object_name: str,
        postgres_table: str,
        az_conn_id: str = AZ_CONN_ID,
        postgres_conn_id: str = POSTGRES_CONN_ID):
    
    az_hook = WasbHook(wasb_conn_id="azure_storage")
    psql_hook = PostgresHook(postgres_conn_id)
    
    with NamedTemporaryFile() as tmp:
        # Add logging or print statements to track the download progress
        az_hook.get_file(file_path=tmp.name, container_name=az_container_name, blob_name=az_object_name)

        # psql_hook.bulk_load(table=postgres_table, 
        #                     tmp_file=tmp.name)
        psql_hook.copy_expert(f"COPY {postgres_table} FROM STDIN DELIMITER ',' CSV HEADER;", tmp.name)