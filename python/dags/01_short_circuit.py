from datetime import datetime, timedelta
from airflow import DAG
from common.short_circuit import ShortCircuitPipeline
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

dag_args = {
    'owner': 'da',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

def t1_callable(**context):
    if context["data_interval_start"].day % 2 == 0:
        raise AirflowFailException()

def t2_callable(**context):
    if context["data_interval_start"].day % 3 == 0:
        raise AirflowFailException()

def t3_callable(**context):
    if context["data_interval_start"].day % 4 == 0:
        raise AirflowFailException()

def try_next_or_fail(**context):
    print("prev_id:")
    print(context["prev_id"])

def gen_tasks():
    t1 = PythonOperator(task_id="t1", python_callable=t1_callable)
    t2 = PythonOperator(task_id="t2", python_callable=t2_callable)
    t3 = PythonOperator(task_id="t3", python_callable=t3_callable)
    return [t1, t2, t3]

with DAG(dag_id='short_circuit', default_args=dag_args) as dag:
    final_end = EmptyOperator(task_id = "final_end")
    sc_tg = ShortCircuitPipeline(tg_id="sc_tg", gen_tasks=gen_tasks, try_next_or_fail=try_next_or_fail).task_group()
    sc_tg >> final_end
