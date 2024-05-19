# Получить JSON из внешнего API ендпоинт: GET https://api.gazprombank.ru/very/important/docs?documents_date={"начало дня сегодня в виде таймстемп"} (download_function)
# Валидировать входящий JSON используя модель pydantic (из ТЗ известно что поле "key1" имеет тип int, "key2"(datetime), "key3"(str)) (validation_functuin)
# Представить данные "Columns" и "Rows" в виде плоского csv-подобного pandas.DataFrame. (json_preparation_function)
# В полученном DataFrame произвести переименование полей по след. маппингу key1" -> "document_id", "key2" -> "document_dt", "key3" -> "document_name" (json_preparation_function)
import itertools
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from pydantic import ValidationError

from schema import CheckJson
import datetime


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1)
}


def download_function(**context):
    start_date = datetime.datetime(2024, 5, 17)
    api_url = f"127.0.0.1:7500/api.gazprombank.ru/very/important/docs/{start_date}"
    hook = HttpHook(method="GET", http_conn_id=api_url)
    response = hook.run(endpoint='', headers={}, extra_options={'data': {'url': api_url}})
    context['ti'].xcom_push(key='json', value=response)


def validation_function(**context):
    json = context['ti']
    try:
        CheckJson(**json)
        context['ti'].xcom_push(key='json', value=json)
    except ValidationError as err:
        return 'Error validating json: ', err


def json_preparation_function(**context):
    df = pd.DataFrame()
    lst_columns = []
    json = context['ti']
    for i in json:
        match i:
            case "Rows":
                df = pd.DataFrame(json[i], columns=lst_columns)
            case "Columns":
                lst_columns.append(json[i])
                lst_columns = list(itertools.chain(*lst_columns))
    df = df.rename(columns={df.columns[0]: "document_id", df.columns[1]: "document_dt", df.columns[2]: "document_name"})
    df['load_dt'] = [datetime.datetime.now() for i in df['document_id']]


with DAG(
    dag_id="download_and_validate_json",
    start_date= datetime.datetime(2024, 5, 17),
    default_args=default_args,
    catchup=False,
    schedule="@once",
    tags=["test"],

) as dag:
    json_download = PythonOperator(
        task_id="download_data",
        python_callable=download_function,
        provide_context=True
    )
    task_validation = PythonOperator(
        task_id="validating_data",
        python_callable=validation_function,
        provide_context=True
    )
    data_prep = PythonOperator(
        task_id="preparing_data",
        python_callable=json_preparation_function,
        provide_context=True
    )
    json_download >> task_validation >> data_prep
