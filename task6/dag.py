# Получить JSON из внешнего API ендпоинт: GET https://api.gazprombank.ru/very/important/docs?documents_date={"начало дня сегодня в виде таймстемп"}

from airflow.models import DAG
from airflow.operators.python import PythonOperator
import datetime
from airflow.providers.http.hooks.http import HttpHook

kwargs = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 5, 17),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'depends_on_past': False,
}


def hook_function(**kwargs):
    start_date = kwargs["start_date"]
    api_url = f"127.0.0.1:7500/api.gazprombank.ru/very/important/docs/{start_date}"
    hook = HttpHook(method="GET", http_conn_id=api_url)
    response = hook.run(endpoint='', headers={}, extra_options={'data': {'url': api_url}})
    return response


with DAG(dag_id='download_json', default_args=kwargs, schedule_interval=None) as dag:
    download_json = PythonOperator(
        task_id='download_api_json',
        python_callable=hook_function,
        dag=dag
    )
download_json