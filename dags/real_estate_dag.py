import json
import pathlib
import posixpath
import airflow
import requests
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, \
    DataprocClusterDeleteOperator
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





exchange_to_gcs = HttpToGcsOperator(gcs_bucket='land_data_training_jjac_airflow',
                                             gcs_path='exchange-rates/exchange-rates-{{ds}}.json',
                                             endpoint='/history?start_at={{ds}}&end_at={{tomorrow_ds}}&symbols=EUR&base=GBP',
                                               task_id="get_data",
                                               dag=dag)

start_dataproc = DataprocClusterCreateOperator(project_id='airflowbolcomdec-7601d68caa710',
                                               cluster_name='test-dataproc-jjac',
                                               num_workers=4)
proc_dataproc = DataProcPySparkOperator(main='build_statistics.py',
                                        arguments=['inp_prop', 'inp_curren', 'target_path', 'tar_curr', 'tar_date'])
delete_dataproc = DataprocClusterDeleteOperator(project_id='airflowbolcomdec-7601d68caa710',
                                                cluster_name='test-dataproc-jjac')

exchange_to_gcs >> start_dataproc >> proc_dataproc >> delete_dataproc