from common.pipeline import Pipeline

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


class TwoDayPipeline(Pipeline):
    def task_group(self): 
        with TaskGroup(self.tg_id, tooltip="TwoDayPipeline") as group:
            today = BashOperator(
                task_id="today",
                bash_command="echo {{ ds }}"
            )
        
            tomorrow = BashOperator(
                task_id="tommorrow",
                bash_command="echo {{ next_ds }}"
            )

            today >> tomorrow
