from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Добавляем папку src в путь, чтобы импортировать наши скрипты
sys.path.append('/opt/airflow/src')

from extract import extract_wb_sales
from load_clickhouse import load_to_clickhouse
# Импортируем главную функцию из transform.py
from transform import main as run_spark_transform_main

default_args = {
    'owner': 'Anna Olasyuk',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='wildberries_etl_pipeline_v2',
    default_args=default_args,
    description='ETL пайплайн: Wildberries Parquet -> Spark Aggregation & ClickHouse',
    # В Airflow 2.4+ schedule_interval заменен на schedule (используем современный синтаксис)
    schedule='@daily', 
    start_date=datetime(2023, 10, 20),
    catchup=False,
    tags=['wildberries', 'etl', 'clickhouse', 'spark', 'portfolio'],
) as dag:

    # Задача 1: Выгрузка / Подготовка (Extract)
    # Читает Parquet, валидирует и пишет NDJSON для ClickHouse
    extract_task = PythonOperator(
        task_id='extract_wb_sales',
        python_callable=extract_wb_sales,
        op_kwargs={
            'api_key': '{{ var.value.get("wb_api_key", "mock_token") }}',
            'days_ago': 7,
            'use_mock': True  # Пока используем мок-данные
        },
    )

    # Задача 2: Загрузка сырых данных в ClickHouse (Load)
    # ⭐ ИСПОЛЬЗУЕМ XCOM для получения имени файла от extract_task
    load_task = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_to_clickhouse,
        op_kwargs={
            # Airflow автоматически подставит значение, которое вернула extract_wb_sales
            'json_file_path': "{{ ti.xcom_pull(task_ids='extract_wb_sales') }}"
        },
    )

    # Задача 3: Аналитическая трансформация в PySpark (Transform)
    # Запускает наш скрипт transform.py, который читает Parquet, чистит и агрегирует данные
    transform_task = PythonOperator(
        task_id='spark_transform_data',
        python_callable=run_spark_transform_main,
    )

    # Настраиваем порядок выполнения:
    # Сначала extract, затем параллельно запускаются load (в ClickHouse) и transform (в Spark)
    extract_task >> [load_task, transform_task]