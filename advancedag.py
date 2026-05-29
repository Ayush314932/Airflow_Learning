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

# fucntions
def start_task():
    print("Dag execution has been started")

def create_file():
    file_path = "/tmp/airflow_sample.txt"
    with open(file_path,"w") as f:
        f.write("Hello from airflow DAG!")
    print(f"File is been created at {file_path}")

def read_file():
    file_path = "/tmp/airflow_sample.txt"
    if os.path.exists(file_path):
       with open(file_path,"r") as f:
          content = f.read()
       print(f"File content is: {content}")
    else:
       print("File does not exits")

def end_task():
    print("DAG Execution has been completed")

# pylint: enable=g-import-not-at-top
# default args
default_args = {
    'start_date': datetime.datetime(2000, 1, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# dag definitions
dag = DAG(
    'simple-etl-learning-dag',
    default_args=default_args,
    description='A simple DAG to learn PythonOperator, BashOperator, file handling, and dependencies',
    schedule=None,
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10),
)

# task definitions

start_task_op=PythonOperator(
   task_id='start_task',
   python_callable=start_task,
   dag=dag,
)

create_file_task = PythonOperator(
   task_id="create_file_task",
    python_callable=create_file,
    dag=dag,
)

read_file_task = PythonOperator(
   task_id="read_file_task",
   python_callable=read_file,
    dag=dag,
)

show_date=BashOperator(
   task_id='show_date',
   bash_command='date',
   dag=dag,
)

end_task_op=PythonOperator(
   task_id='end_task',
   python_callable=end_task,
    dag=dag,
)

# dependencies
start_task_op >> create_file_task >> [read_file_task,show_date] >> end_task_op