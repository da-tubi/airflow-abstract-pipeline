from airflow.utils.task_group import TaskGroup

class Pipeline:
    def __init__(self, tg_id):
        self.doc_md = ""
        # task group id
        self.tg_id = tg_id

    def task_group() -> TaskGroup:
        pass
