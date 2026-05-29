#imports
import os
import datetime

from airflow import DAG

# pylint: disable=g-import-not-at-top
try:
  from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
  from airflow.operators.bash_operator import BashOperator

try:
  from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
  from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
# fucntions

# pylint: enable=g-import-not-at-top
# default args
default_args = {
    'start_date': datetime.datetime(2000, 1, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# dag definitions
dag = DAG(
    'gcs-to-bigquery-etl-pipeline',
    default_args=default_args,
    description='pipeline default',
    schedule=None,
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10),
)

check_file = GCSObjectExistenceSensor(
    task_id='check_file',
    bucket='tt-landing-00',
    object='employee_data.csv',
    timeout=600,
    poke_interval=10,
    dag=dag,
)

# task definitions
load_csv= GCSToBigQueryOperator(
    task_id='load_csv',
    bucket='tt-landing-00',
    source_objects=['employee_data.csv'],
    destination_project_dataset_table='project-9ee83310-902f-4273-a72.airflow_demo.employee_data',
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,
    dag=dag,
)

move_file = GCSToGCSOperator(
    task_id='move_file',
    source_bucket='tt-landing-00',
    source_object='employee_data.csv',
    destination_bucket='tt-landing-00',
    destination_object='processed/employee_data.csv',
    move_object=True,
    dag=dag,
)

# define dependencies
check_file >> load_csv >> move_file

