from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.dagrun import DagRun
from airflow.models import TaskInstance
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule


dag_args = {
    'owner': 'da',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'params': {
        "tasks": ["feature_0", "feature_1"],
        "date_range": "one week",
    },
}

def branch_by_config(**context) -> str:
    dag_run: DagRun = context['dag_run']
    ti: TaskInstance = context['ti']
    prefix_name = ".".join(ti.task_id.split(".")[:-1])
    feature_name = ti.task_id.split(".")[-1].removeprefix("branch_")
    if dag_run.external_trigger:
        if 'tasks' in dag_run.conf:
            if feature_name not in dag_run.conf["tasks"]:
                raise AirflowSkipException(f"skip feature {feature}")
            else:
                if dag_run.conf["date_range"] == "one week":
                    return f"{prefix_name}.backfill_one_week.start"
                else:
                    return f"{prefix_name}.backfill_tg.start"
    else:
        return f"{prefix_name}.real_task"

with DAG(dag_id="backfill_task_group_with_brancher", default_args=dag_args) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("group", tooltip="Tasks for group") as group:
        prepare = EmptyOperator(task_id="prepare")
        dag_runs = DagRun.find(dag_id="task_selector", external_trigger=True)
        for i in range(3):
            feature_name = f"feature_{i}"
            with TaskGroup(feature_name) as feature:
                cond = BranchPythonOperator(task_id=f"branch_{feature_name}", python_callable=branch_by_config)
                task = BashOperator(task_id="real_task", bash_command="echo {{ execution_date }}")
                end = EmptyOperator(task_id="end")
                with TaskGroup(f"backfill_tg") as backfill_group:
                    start_tg = EmptyOperator(task_id=f"start")
                    task = BashOperator(task_id="real_task", bash_command="echo {{ execution_date }}")
                    start_tg >> task
                with TaskGroup(f"backfill_one_week") as backfill_one_week:
                    start_7 = EmptyOperator(task_id=f"start")
                    end_7 = EmptyOperator(task_id="end")
                    tasks = []
                    for i in range(7):
                        task = BashOperator(task_id=f"real_task_{i}", bash_command="echo {{ execution_date.add(days=%s).isoformat()[:10] }}" % i)
                        tasks.append(task)
                    start_7 >> tasks >> end_7
                cond >> [task, backfill_group, backfill_one_week] >> end
            prepare >> feature

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    start >> group >> end
