from common.pipeline import Pipeline

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule


class TryInSeqPipeline(Pipeline):
    def __init__(self, tg_id, gen_tasks, try_next_or_fail):
        super(TryInSeqPipeline, self).__init__(tg_id)
        self.try_next_or_fail = try_next_or_fail
        self.gen_tasks = gen_tasks

    def task_group(self): 
        with TaskGroup(self.tg_id, tooltip="TryInSeqPipeline") as group:
            end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ONE_SUCCESS)
            tasks = self.gen_tasks()
            N = len(tasks)
            assert (N >= 2)
            for task in tasks:
                task >> end
            for i in range(N-1):
                prev_task = tasks[i]
                next_task = tasks[i+1]
                cond = PythonOperator(
                    task_id=f"fail_or_try_next_{i}",
                    python_callable=self.try_next_or_fail, 
                    op_kwargs={
                        "prev_id": prev_task.task_id,
                    },
                    trigger_rule=TriggerRule.ONE_FAILED
                )
                prev_task >> cond >> next_task

        return group

