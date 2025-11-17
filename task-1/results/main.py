from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}


def read_data_source():
    """Чтение данных из CSV файла"""
    try:
        # Создаем тестовый файл с данными
        data = {
            'id': [1, 2, 3, 4, 5],
            'value': [10, 25, 15, 30, 5],
            'category': ['A', 'B', 'A', 'B', 'A']
        }
        df = pd.DataFrame(data)
        df.to_csv('/tmp/sample_data.csv', index=False)
        print("✅ Данные успешно записаны в файл")
        return True
    except Exception as e:
        print(f"❌ Ошибка при записи данных: {e}")
        raise


def analyze_data_and_branch(**kwargs):
    """Анализ данных и ветвление пайплайна"""
    try:
        # Чтение данных из файла
        df = pd.read_csv('/tmp/sample_data.csv')

        # Анализ данных
        avg_value = df['value'].mean()
        total_records = len(df)

        print(f"📊 Анализ данных:")
        print(f"   - Среднее значение: {avg_value}")
        print(f"   - Всего записей: {total_records}")

        # Условие для ветвления
        if avg_value > 15:
            print("🔀 Переход на ветку 'high_values'")
            return 'high_values_branch'
        else:
            print("🔀 Переход на ветку 'low_values'")
            return 'low_values_branch'

    except Exception as e:
        print(f"❌ Ошибка при анализе данных: {e}")
        return 'error_branch'


def process_high_values():
    """Обработка высоких значений"""
    print("🎯 Обрабатываем высокие значения")
    # Имитация обработки
    df = pd.read_csv('/tmp/sample_data.csv')
    high_values = df[df['value'] > 15]
    print(f"   Найдено {len(high_values)} записей с высокими значениями")
    return "High values processed"


def process_low_values():
    """Обработка низких значений"""
    print("📉 Обрабатываем низкие значения")
    # Имитация обработки
    df = pd.read_csv('/tmp/sample_data.csv')
    low_values = df[df['value'] <= 15]
    print(f"   Найдено {len(low_values)} записей с низкими значениями")
    return "Low values processed"


def handle_error():
    """Обработка ошибок"""
    print("🚨 Обработка ошибки в пайплайне")
    return "Error handled"


def cleanup():
    """Очистка временных файлов"""
    try:
        if os.path.exists('/tmp/sample_data.csv'):
            os.remove('/tmp/sample_data.csv')
            print("🧹 Временные файлы удалены")
    except Exception as e:
        print(f"⚠️ Ошибка при очистке: {e}")


with DAG(
        'data_processing_pipeline',
        default_args=default_args,
        description='Пайплайн обработки данных с ветвлением и уведомлениями',
        schedule_interval=timedelta(hours=1),
        catchup=False,
        tags=['data_processing', 'demo'],
) as dag:
    start = DummyOperator(task_id='start')

    # Чтение данных из источника
    read_data = PythonOperator(
        task_id='read_data_source',
        python_callable=read_data_source,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    # Анализ и ветвление
    analyze_and_branch = BranchPythonOperator(
        task_id='analyze_data_and_branch',
        python_callable=analyze_data_and_branch,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )

    # Ветки обработки
    high_values_branch = PythonOperator(
        task_id='high_values_branch',
        python_callable=process_high_values,
        retries=1,
    )

    low_values_branch = PythonOperator(
        task_id='low_values_branch',
        python_callable=process_low_values,
        retries=1,
    )

    error_branch = PythonOperator(
        task_id='error_branch',
        python_callable=handle_error,
    )

    # Очистка
    cleanup_task = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup,
        trigger_rule='all_done',
        # Выполняется независимо от успеха предыдущих задач
    )

    # Email уведомления
    success_email = EmailOperator(
        task_id='success_email',
        to='your_email@example.com',
        subject='Airflow: Пайплайн успешно выполнен',
        html_content="""
        <h3>Пайплайн обработки данных успешно завершен!</h3>
        <p>Все задачи выполнены без ошибок.</p>
        <p>Время выполнения: {{ execution_date }}</p>
        """,
        trigger_rule='all_success',
    )

    failure_email = EmailOperator(
        task_id='failure_email',
        to='your_email@example.com',
        subject='Airflow: Ошибка в пайплайне',
        html_content="""
        <h3>В пайплайне обработки данных произошла ошибка!</h3>
        <p>Пожалуйста, проверьте логи для деталей.</p>
        <p>Время выполнения: {{ execution_date }}</p>
        """,
        trigger_rule='one_failed',
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='all_done',
    )

    # Определение порядка выполнения
    start >> read_data >> analyze_and_branch
    analyze_and_branch >> [high_values_branch, low_values_branch, error_branch]
    [high_values_branch, low_values_branch, error_branch] >> cleanup_task
    cleanup_task >> [success_email, failure_email] >> end