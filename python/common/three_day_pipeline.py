from common.pipeline import Pipeline

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


class ThreeDayPipeline(Pipeline):
    def task_group(self): 
        with TaskGroup(self.tg_id, tooltip="TwoDayPipeline") as group:
            day_1 = BashOperator(
                task_id="今天",
                bash_command="echo {{ ds }}"
            )
        
            day_2 = BashOperator(
                task_id="明天",
                bash_command="echo {{ next_ds }}"
            )

            day_3 = BashOperator(
                task_id="后天",
                bash_command="echo {{ tomorrow_ds }}"
            )

            day_1 >> day_2 >> day_3
        return group
