import json
import pathlib
import posixpath
import airflow
import requests
from airflow.models import DAG

from operators.http_to_gcs_operator import HttpToGcsOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(10)}


def _on_failure_callback(context):
    print('failure!!!')
    print(context)


dag = DAG(dag_id="real_estate_dag",
          default_args=args,
          on_failure_callback=_on_failure_callback,
          description="Own stuff",
          schedule_interval="0 0 * * *")





download_rocket_launches = HttpToGcsOperator(gcs_bucket='land_data_training_jjac_airflow',
                                             gcs_path='exchange-rates/exchange-rates-{{ds}}.json',
                                             endpoint='https://api.exchangeratesapi.io/history?start_at={{ds}}&end_at={{tomorrow_ds}}&symbols=EUR&base=GBP',
                                               task_id="get_data",
                                               dag=dag)

