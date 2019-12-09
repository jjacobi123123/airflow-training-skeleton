from datetime import timedelta
from json import dumps

from airflow import AirflowException
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
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


#def _connect_to_postgres(**context):
##    postgres = PostgresToGoogleCloudStorageOperator(task_id='postgres',
#                                                    postgres_conn_id='postgres_default')
#    postgres.query()

with DAG(
        dag_id='connect_to_postgres_dag',
        default_args=args,
        schedule_interval='@daily',
        dagrun_timeout=timedelta(minutes=60),
) as dag:
    PostgresToGoogleCloudStorageOperator(task_id='postgres',
                                         sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ds}}'::date LIMIT 10",
                                         filename="output.csv",
                                         bucket="land_data_training_jjac_airflow",
                                         postgres_conn_id='postgres_default')
    #print_weekday = PythonOperator(task_id='weekday_dag',
    #                              python_callable=_connect_to_postgres,
    #                              provide_context=True)




