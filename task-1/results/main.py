import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.providers.slack.operators.slack_api import SlackAPIOperator
from airflow.providers.standard.operators.python import PythonOperator


def check_data():
    filepath = "/text.txt"
    if os.path.exists(filepath):
        return "process_data"
    else:
        return "no_data_alert"


def process_data():
    print("✅ Данные найдены! Обрабатываем файл...")


def no_data_alert():
    print("⚠️ Данных нет. Отправляем уведомление в Slack/Email.")


with DAG(
    dag_id="daily_etl",
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract = PostgresOperator(
        task_id="extract",
        sql="SELECT * FROM orders"
    )

    transform = SparkSubmitOperator(
        task_id="transform",
        application="clean_orders.py"
    )

    load = BigQueryOperator(
        task_id="load",
        sql="INSERT INTO report ..."
    )

    notify = SlackAPIOperator(
        task_id="notify",
        message="Report is ready"
    )


    def notify_slack(context):
        message = f"DAG {context['dag']} task {context['task_instance']} failed."
        # тут вызов Slack API
        print(message)


    PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        on_failure_callback=notify_slack,
        retries=3,                           # сколько раз повторить
        retry_delay=timedelta(minutes=5),    # задержка между попытками
        retry_exponential_backoff=True,      # экспоненциальный рост задержек
        max_retry_delay=timedelta(minutes=60)  # максимум до часа между попытками
    )

    extract >> transform >> load >> notify