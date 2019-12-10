import datetime
from unittest import TestCase

import pytest
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from operators.launch_to_gcs_operator import LaunchToGcsOperator

"""
docker-compose -f dockercompose-local.yml up -d
run: docker ps
docker exec -it 43e616488d6e /bin/bash
python -m pytest -v
"""

@pytest.fixture
def test_dag():
    return DAG(
        "test_dag",
        default_args={"owner": "airflow", "start_date": datetime.datetime(2018, 1, 1)},
        schedule_interval=datetime.timedelta(days=1),
    )


def run_task(task, dag):

    """Run an Airflow task."""
    dag.clear()
    task.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"],
    )


def test_operator(test_dag, mocker):
    mocked_upload = mocker.patch.object(LaunchToGcsOperator, "_upload_to_gcs", return_value=None)

    task = LaunchToGcsOperator(bucket='land_data_training_jjac_airflow',
                        file_name='launches-{{ds}}.json',
                        start_date_str='{{ds}}',
                        end_date_str='{{tomorrow_ds}}',
                        task_id="download_rocket_launches", dag=test_dag)
    run_task(task=task, dag=test_dag)



#
#
# def test_execute(tmpdir='/tmp'):
#     tmpfile = tmpdir.join("hello.txt")
#
#     task = BashOperator(task_id="test", bash_command=f"echo 'hello' > {tmpfile}", dag=test_dag)
#     pytest.helpers.run_task(task=task, dag=test_dag)
#
#     assert len(tmpdir.listdir()) == 1
#     assert tmpfile.read().replace("\n", "") == "hello"
#
