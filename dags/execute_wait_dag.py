
from datetime import timedelta

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone

import datetime

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': timezone.datetime(2019, 12, 7),
}

def print_execution_date(**context):
    print(context['execution_date'])

with DAG(
        dag_id='execute_wait_dag',
        default_args=args,
        schedule_interval=None,
        dagrun_timeout=timedelta(minutes=60),
    ) as dag:

    print_execution_date_operator = PythonOperator(task_id='print_execution_date',
                                          python_callable=print_execution_date,
                                                   provide_context=True)
    wait_5 = BashOperator(task_id='wait_5', bash_command="sleep 5")
    wait_1 = BashOperator(task_id='wait_1', bash_command="sleep 5")
    wait_10 = BashOperator(task_id='wait_10', bash_command="sleep 5")
    the_end = DummyOperator(task_id='the_end')

print_execution_date >> wait_1 >> the_end
print_execution_date >> wait_5 >> the_end
print_execution_date >> wait_10 >> the_end

