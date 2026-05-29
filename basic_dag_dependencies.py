import datetime

from airflow import DAG

# pylint: disable=g-import-not-at-top
try:
  from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
  from airflow.operators.bash_operator import BashOperator

from airflow.operators.python_operator import PythonOperator

def hello_world():
    print("Hello World")

# pylint: enable=g-import-not-at-top

default_args = {
    'start_date': datetime.datetime(2000, 1, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'basic-dag-with-dependencies',
    default_args=default_args,
    description='Basic Dag With Dependencies',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10),
)

# priority_weight has type int in Airflow DB, uses the maximum.
bash_operator = BashOperator(
    task_id='task-1',
    bash_command='echo test',
    dag=dag,
    )

python_operator = PythonOperator(
   task_id='task-2',
   python_callable=hello_world,
   dag=dag,
) 

bash_operator >> python_operator