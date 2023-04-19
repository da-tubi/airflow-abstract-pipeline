from common.pipeline import Pipeline

from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator


class BackfillOneWeekPipeline(Pipeline):
    def task_group(self) -> TaskGroup:
        with TaskGroup(f"backfill_one_week") as backfill_one_week:
            start_7 = EmptyOperator(task_id=f"start")
            end_7 = EmptyOperator(task_id="end")
            tasks = []
            for i in range(7):
                task = BashOperator(task_id=f"real_task-{i}", bash_command="echo {{ execution_date.add(days=-%s).isoformat()[:10] }}" % i)
                tasks.append(task)
            start_7 >> tasks >> end_7
        return backfill_one_week
