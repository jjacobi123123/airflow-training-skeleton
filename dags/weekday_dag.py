from datetime import timedelta
from json import dumps

from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils import timezone

import datetime

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from httplib2 import Http

args = {
    'owner': 'Airflow',
    'start_date': timezone.datetime(2019, 12, 1),
}

people = ('bob', 'alice', 'joe')


def _print_weekday(**context):
    print(context['execution_date'].strftime('%a'))
    print(context)


def _branching(**context):
    weekday = context['execution_date'].strftime('%a')
    if weekday in ('Mon', 'Tue'):
        name = 'bob'
    elif weekday in ('Wed', 'Thu'):
        name = 'alice'
    elif weekday in ('Fri', 'Sat', 'Sun'):
        name = 'joe'
    else:
        raise AirflowException(f"Invalid weekday {weekday}")
    return 'email_' + name


def _send_google_chat_notification(**context):
    url = 'https://chat.googleapis.com/v1/spaces/AAAA238a3ug/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=otsGvxyYx1DOcsGhn8xFcSPDSbyyGo7nTbCp1RL8HIw%3D'
    bot_message = {
        'text' : 'Hello from Airflow'}

    message_headers = { 'Content-Type': 'application/json; charset=UTF-8'}

    http_obj = Http()

    response = http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=dumps(bot_message),
    )

    print(response)


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
    final_task = PythonOperator(task_id='final_task',
                                trigger_rule=TriggerRule.NONE_FAILED,
                                python_callable=_send_google_chat_notification,
                                provide_context=True)
    for name in people:
        operator = DummyOperator(task_id='email_' + name)
        branching >> operator >> final_task

    print_weekday >> branching
