from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator


dag_args = {
    'owner': 'da',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'params': {
        "branch": "empty_task_2"
    },
}

@task.branch()
def trigger_branch(**context) -> str:
    dag_run = context['dag_run']
    if 'branch' in dag_run.conf:
        return dag_run.conf['branch']
    else:
        return "empty_task_1"


with DAG(dag_id="trigger_branch_v1", default_args=dag_args) as dag:
    cond = trigger_branch()

    empty_task_1 = EmptyOperator(task_id="empty_task_1")
    empty_task_2 = EmptyOperator(task_id="empty_task_2")
    cond >> [empty_task_1, empty_task_2]

