from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# Define connection ID for PostgreSQL
postgres_conn_id = "postgres_conn"
az_hook = WasbHook(wasb_conn_id="azure_storage")
file = ""

# Define SQL statements for each dimension table
load_data_into_dim_device = """
    INSERT INTO dw.dim_devices(id_dim_devices, device)
    SELECT id, device
    FROM {{ti.xcom_pull(key='tmp_file_name')}}
"""

load_data_into_dim_os = """
INSERT INTO dw.dim_os(id_dim_devices, os)
SELECT id, os
FROM %(file_path)s
"""
load_data_into_dim_location = """
INSERT INTO dw.dim_location(id_dim_location, location)
SELECT id, location
FROM %(file_path)s
"""
load_data_into_dim_date = """
INSERT INTO dim_d(id_dim_date, log_date)
SELECT id, log_date
FROM %(file_path)s
"""

