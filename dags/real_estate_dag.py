import json
import pathlib
import posixpath
import airflow
import requests
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, \
    DataprocClusterDeleteOperator
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule

from operators.http_to_gcs_operator import HttpToGcsOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(10)}


def _on_failure_callback(context):
    print('failure!!!')
    print(context)


with DAG(dag_id="real_estate_dag",
          default_args=args,
          on_failure_callback=_on_failure_callback,
          description="Own stuff",
          schedule_interval="0 0 * * *") as dag:





    exchange_to_gcs = HttpToGcsOperator(gcs_bucket='land_data_training_jjac_airflow',
                                                 gcs_path='exchange-rates/exchange-rates-{{ds}}.json',
                                                 endpoint='/history?start_at={{ds}}&end_at={{tomorrow_ds}}&symbols=EUR&base=GBP',
                                                   task_id="get_data")

    start_dataproc = DataprocClusterCreateOperator(project_id='airflowbolcomdec-7601d68caa710',
                                                   cluster_name='test-dataproc-jjac-{{ds}}',
                                                   num_workers=4,
                                                   region='europe-west1',
                                                   task_id='start_dataproc')
    proc_dataproc = DataProcPySparkOperator(main='./spark/build_statistics.py',
                                            project_id='airflowbolcomdec-7601d68caa710',
                                            cluster_name='test-dataproc-jjac-{{ds}}',
                                            region='europe-west1',
                                            arguments=['inp_prop', 'inp_curren', 'target_path', 'tar_curr', 'tar_date'],
                                            task_id="proc_dataproc")
    delete_dataproc = DataprocClusterDeleteOperator(project_id='airflowbolcomdec-7601d68caa710',
                                                    cluster_name='test-dataproc-jjac-{{ds}}',
                                                    region='europe-west1',
                                                    task_id="delete_dataproc", trigger_rule=TriggerRule.ALL_DONE)

    exchange_to_gcs >> start_dataproc >> proc_dataproc >> delete_dataproc