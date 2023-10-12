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
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator


from datetime import datetime, timedelta
import os


default_args = {
    'owner': 'Emmanuel',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='transform_files_on_databricks',
    default_args=default_args,
    start_date=datetime(2023, 10, 2),
    schedule_interval='@once'
) as dag:
    json = {
    "new_cluster": {"spark_version": "2.1.0-db3-scala2.11", "num_workers": 2},
    "notebook_task": {
        "notebook_path": "/Users/airflow@example.com/PrepareData",
    },
}
    notebook_transform_user_reviews = DatabricksSubmitRunOperator(task_id="notebook_transform_user_reviews", json=json)

    notebook_transform_sumbit_postgres_data = DatabricksSubmitRunOperator(task_id="notebook_transform_sumbit_postgres_data", json=json)
    
    notebook_transform_log_reviews = DatabricksSubmitRunOperator(task_id="notebook_transform_log_reviews", json=json)

    notebook_transform_user_reviews >> notebook_transform_sumbit_postgres_data >> notebook_transform_log_reviews