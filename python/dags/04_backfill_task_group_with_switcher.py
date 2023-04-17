from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.dagrun import DagRun
from airflow.models import TaskInstance
from airflow.exceptions import AirflowSkipException


dag_args = {
    'owner': 'da',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'params': {
        "tasks": ["task_0", "task_1"]
    },
}

def switch_by_config(**context):
    dag_run: DagRun = context['dag_run']
    if dag_run.external_trigger:
        if 'tasks' in dag_run.conf:
            ti: TaskInstance = context['ti']
            task_name = ti.task_id.split(".")[-1]
            feature = task_name.removeprefix("switch_")
            if feature not in dag_run.conf["tasks"]:
                raise AirflowSkipException(f"skip feature {feature}")

with DAG(dag_id="backfill_task_group_with_switcher", default_args=dag_args) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("group", tooltip="Tasks for group") as group:
        prepare = EmptyOperator(task_id="prepare")
        dag_runs = DagRun.find(dag_id="task_selector", external_trigger=True)
        for i in range(3):
            with TaskGroup(f"feature_{i}") as feature:
                switcher = PythonOperator(task_id=f"switch_task_{i}", python_callable=switch_by_config)
                task = EmptyOperator(task_id=f"task_{i}")
                switcher >> task
            prepare >> feature

    end = EmptyOperator(task_id="end")

    start >> group >> end
