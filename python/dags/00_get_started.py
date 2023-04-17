from datetime import datetime, timedelta
from airflow import DAG
from common.two_day_pipeline import TwoDayPipeline

dag_args = {
    'owner': 'da',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}


with DAG(dag_id='demo_dag', default_args=dag_args) as dag:
    two_day_tg = TwoDayPipeline("two_day_tg").task_group()
    two_day_tg