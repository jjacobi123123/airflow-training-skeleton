import json
import pathlib
import posixpath
import airflow
import requests
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from operators.launch_to_gcs_operator import LaunchToGcsOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(10)}


def _on_failure_callback(context):
    print(context)


dag = DAG(dag_id="download_rocket_launches_own",
          default_args=args,
          on_failure_callback=_on_failure_callback,
          description="Own stuff",
          schedule_interval="0 0 * * *")




def _print_stats(ds, **context):
    with open(f"/data/rocket_launches/ds={ds}/launches.json") as f:
        data = json.load(f)
        rockets_launched = [launch["name"] for launch in data["launches"]]
        rockets_str = ""
        if rockets_launched:
            rockets_str = f" ({' & '.join(rockets_launched)})"

        print(f"{len(rockets_launched)} rocket launch(es) on {ds}{rockets_str}.")


download_rocket_launches = LaunchToGcsOperator(bucket='land_data_training_jjac_airflow',
                                               file_name='launches.json',
                                               start_date='{{ds}}',
                                               end_date='{{tomorrow_ds}}',
                                               task_id="download_rocket_launches",
                                               dag=dag)
print_stats = PythonOperator(task_id="print_stats", python_callable=_print_stats, provide_context=True, dag=dag)

download_rocket_launches >> print_stats
