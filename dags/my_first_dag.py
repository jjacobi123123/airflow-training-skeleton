
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='my_first_dag',
    default_args=args,
    schedule_interval='None',
    dagrun_timeout=timedelta(minutes=60),
)

task1 = DummyOperator(
    task_id='task1',
    dag=dag,
)
task2 = DummyOperator(
    task_id='task2',
    dag=dag,
)
task3 = DummyOperator(
    task_id='task3',
    dag=dag,
)
task4 = DummyOperator(
    task_id='task4',
    dag=dag,
)
task5 = DummyOperator(
    task_id='task5',
    dag=dag,
)

task1 >> task2 >> task4 >> task5
task2 >> task3 >> task5
