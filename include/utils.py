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
import pyarrow.parquet as pq

# Defining Constants
AZ_CONN_ID = 'az_data_lake'
POSTGRES_CONN_ID = 'azure_database'
POSTGRES_TABLE = 'user_purchase_schema.user_purchase'
AZURE_CONTAINER_NAME = 'csv-files'
AZ_FILE_NAME = 'user_purchase_updated.csv'
DATABRICKS_JOB_ID = 841473727185472


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


def get_latest_dim_csv_file_name(ti, container_name, dim_name):
   """
   Get the name of the latest file that was written to the azure datalake using the container name and the name of the dimension
   """

   wasb_hook = WasbHook(wasb_conn_id=AZ_CONN_ID)
   file_list = wasb_hook.get_blobs_list_recursive(container_name=container_name, endswith='.csv')
   print(file_list)
   file_names = [file for file in file_list if dim_name in file]

   file_name = file_names[0] 
   ti.xcom_push(key=f'{dim_name}_filename', value=file_name)


def load_csv_dim_data_from_staging_area(
        table_name,
        data_lake_folder,
        container_name
):
    az_hook = WasbHook(wasb_conn_id="az_data_lake")
    psql_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Retrieve The File Name of the dim file to be stored
    file_list = az_hook.get_blobs_list_recursive(container_name=container_name, endswith='.csv')
    print(file_list)
    file_names = [file for file in file_list if f"log_reviews_result/{data_lake_folder}.csv/" in file]

    file_name = file_names[0] 
#     ti.xcom_push(key=f'{dim_name}_filename', value=file_name)

#     # Fetch actual file path
#     file_path = ti.xcom_pull(task_ids='get_name_of_dim_device_file', key=f"{data_lake_folder}_filename")   
    print(f"file path gotten is --> {file_name}")

    with NamedTemporaryFile() as tmp:
        az_hook.get_file(file_path=tmp.name, container_name=container_name, blob_name=file_name)

        # psql_hook.bulk_load(table=postgres_table, 
        #                     tmp_file=tmp.name)
        psql_hook.copy_expert(f"""COPY {table_name}
                              FROM STDIN 
                              WITH (FORMAT CSV,
                                    DELIMITER ',' 
                                    );
                              """, tmp.name)
        
def load_fact_table_from_staging_area(
        ti, 
        table_name,
        container_name
):
    az_hook = WasbHook(wasb_conn_id="az_data_lake")
    psql_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # get file name o
    file_list = az_hook.get_blobs_list_recursive(container_name=container_name, endswith='.csv')
    file_names = [file for file in file_list if 'fact/fact_movie_analytics.csv/part' in file]

    file_name = file_names[0] 
    print(f"file path gotten is --> {file_name}")

    # Load the Data into the Fact Table
    with NamedTemporaryFile() as tmp:
        az_hook.get_file(file_path=tmp.name, container_name=container_name, blob_name=file_name)

        # psql_hook.bulk_load(table=postgres_table, 
        #                     tmp_file=tmp.name)
        psql_hook.copy_expert(f"""COPY {table_name}
                              FROM STDIN 
                              WITH (FORMAT CSV,
                                    DELIMITER ',' 
                                    );
                              """, tmp.name)



def load_dim_data_from_staging_area(
        ti,
        data_lake_file_name,
        container_name,
):
    az_hook = WasbHook(wasb_conn_id="az_data_lake")

    # Create a temporary file
    tmp = NamedTemporaryFile(delete=False)

    
    # Add logging or print statements to track the download progress
    az_hook.get_file(file_path=tmp.name, container_name=container_name, blob_name=data_lake_file_name)

    # Read the file with pyarrow 
    df = pq.read_table(tmp.name)
    
     # Extract the columns as lists
    log_id_list = df['log_id'].to_pylist()
    device_list = df['device'].to_pylist()

    # Load data into PostgreSQL using PostgresHook
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    connection = hook.get_conn()
    cursor = connection.cursor()

    # Zip the lists and pass them to cursor.execute
    data = zip(log_id_list, device_list)
    cursor.executemany("INSERT INTO dw.dim_devices(id_dim_devices, device) VALUES (%s, %s)", data)

    # Commit changes and close the connection
    connection.commit()
    cursor.close()
    connection.close()
