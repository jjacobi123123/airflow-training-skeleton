
from datetime import timedelta
from airflow.utils import timezone

import datetime

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': timezone.datetime(2019, 11, 1),
}

with DAG(
        dag_id='daily_dag',
        default_args=args,
        schedule_interval='@daily',
        dagrun_timeout=timedelta(minutes=60),
    ) as dag:

    task1 = DummyOperator(task_id='task1')
    task2 = DummyOperator(task_id='task2')
    task3 = DummyOperator(task_id='task3')
    task4 = DummyOperator(task_id='task4')
    task5 = DummyOperator(task_id='task5')

task1 >> task2 >> task4 >> task5
task2 >> task3 >> task5