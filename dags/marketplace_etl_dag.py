from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Добавляем папку src в путь, чтобы импортировать наши скрипты
sys.path.append('/opt/airflow/src')

from extract import extract_wb_sales
from load_clickhouse import load_to_clickhouse

default_args = {
    'owner': 'Anna Olasyuk',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='wildberries_etl_pipeline',
    default_args=default_args,
    description='ETL пайплайн: Wildberries API -> JSON -> ClickHouse',
    schedule_interval='@daily',
    start_date=datetime(2023, 10, 20),
    catchup=False,
    tags=['wildberries', 'etl', 'clickhouse', 'portfolio'],
) as dag:

    # Задача 1: Выгрузка (Extract)
    extract_task = PythonOperator(
        task_id='extract_wb_sales',
        python_callable=extract_wb_sales,
        op_kwargs={
            'api_key': '{{ var.value.get("wb_api_key", "mock_token") }}',
            'days_ago': 7,
            'use_mock': True # Пока используем мок-данные
        },
    )

    # Задача 2: Загрузка в ClickHouse (Load)
    load_task = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_to_clickhouse,
        op_kwargs={
            'json_file_path': '/opt/airflow/data/raw/wb_sales_20261029.json'  # Актуальное имя файла
        },
    )

    # Настраиваем порядок выполнения
    extract_task >> load_task