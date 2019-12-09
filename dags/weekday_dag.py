from datetime import timedelta

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils import timezone

import datetime

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': timezone.datetime(2019, 12, 1),
}

people = ('bob', 'alice', 'joe')


def _print_weekday(**context):
    print(context['execution_date'].strftime('%a'))


def _branching(**context):
    if context['execution_date'].weekday() < 3:
        name = 'bob'
    elif context['execution_date'].weekday() < 5:
        name = 'alice'
    else:
        name = 'joe'
    return 'email_' + name


with DAG(
        dag_id='weekday_dag',
        default_args=args,
        schedule_interval='@daily',
        dagrun_timeout=timedelta(minutes=60),
) as dag:
    print_weekday = PythonOperator(task_id='weekday_dag',
                                   python_callable=_print_weekday,
                                   provide_context=True)
    branching = BranchPythonOperator(task_id='branching',
                                     python_callable=_branching,
                                     provide_context=True)
    final_task = DummyOperator(task_id='final_task', trigger_rule='none_failed')
    for name in people:
        operator = DummyOperator(task_id='email_' + name)
        branching >> operator >> final_task

    print_weekday >> branching
