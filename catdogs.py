import json
import telebot
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator


args = {
    'owner': 'max_zorn',
    'provide_context': True
}

def send_photos(**kwargs):
    bot = telebot.TeleBot(token=Variable.get("BOT_TOKEN"), threaded=False)

    task_instance = kwargs['task_instance']
    cat_url = json.loads(task_instance.xcom_pull(task_ids='find_cat', key='return_value'))
    dog_url = json.loads(task_instance.xcom_pull(task_ids='find_dog', key='return_value'))

    bot.send_photo(chat_id=Variable.get("MAX_ZORN_CHAT_ID"), photo=cat_url[0]['url'])
    bot.send_photo(chat_id=Variable.get("MAX_ZORN_CHAT_ID"), photo=dog_url[0]['url'])

with DAG(
    dag_id='catdogs',
    default_args=args,
    description = 'Sends photos of cats and dogs to telegram chat @airflowCatDogBot',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['cat', 'dog', 'telegram', 'api', 'http', 'python'],   
) as dag:

    find_cat = SimpleHttpOperator(
        task_id='find_cat',
        method='GET',
        http_conn_id='cat_api',
        endpoint='/v1/images/search',
        headers={"Content-Type": "application/json"},
        dag=dag
    )

    find_dog = SimpleHttpOperator(
        task_id='find_dog',
        method='GET',
        http_conn_id='dog_api',
        endpoint='/v1/images/search',
        headers={"Content-Type": "application/json"},
        dag=dag
    )

    send_message = PythonOperator(
            task_id='send_message',
            python_callable=send_photos,
        )

    [find_cat, find_dog] >> send_message
