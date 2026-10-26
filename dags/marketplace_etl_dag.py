from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow/src')

from extract import extract_wb_sales

default_args = {
    'owner': 'Anna Olasyuk',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='wildberries_etl_pipeline',
    default_args=default_args,
    description='ETL пайплайн для выгрузки и обработки данных Wildberries',
    schedule_interval='@daily',
    start_date=datetime(2023, 10, 20),
    catchup=False,
    tags=['wildberries', 'etl', 'api', 'marketplace', 'portfolio'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_wb_sales',
        python_callable=extract_wb_sales,
        op_kwargs={
            'api_key': None,  # Пока используем mock, ключ не нужен
            'days_ago': 7,
            'use_mock': True  # Mock-режим для стабильности
        },
    )

    transform_task = PythonOperator(
        task_id='transform_wb_data',
        python_callable=lambda: print("Этап трансформации будет добавлен следующим шагом"),
    )

    extract_task >> transform_task