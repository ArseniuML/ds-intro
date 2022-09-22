import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator

path = os.path.expanduser('~/airflow_hw')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.pipeline import pipeline
from modules.predict import predict

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}


def pipeline_wrapper(**kwargs):
    return pipeline()


def predict_wrapper(**kwargs):
    timestamp = kwargs['task_instance'].xcom_pull(task_ids='pipeline')
    predict(timestamp)


with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    pipeline_op = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline_wrapper,
        provide_context=True,
        dag=dag
    )

    predict_op = PythonOperator(
        task_id='predict',
        python_callable=predict_wrapper,
        provide_context=True,
        dag=dag
    )

    pipeline_op >> predict_op



