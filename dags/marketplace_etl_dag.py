from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Anna Olasyuk',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='marketplace_etl_pipeline',
    default_args=default_args,
    description='ETL пайплайн для обработки данных маркетплейса',
    schedule_interval='@daily',
    start_date=datetime(2026, 10, 20),
    catchup=False,
    tags=['marketplace', 'etl', 'pyspark'],
) as dag:

    # Задача 1: Извлечение данных
    extract_task = BashOperator(
        task_id='run_extract',
        bash_command='docker run --rm --network marketplace-data-pipeline_pipeline-net -v /workspaces/marketplace-data-pipeline/src:/app/src -v /workspaces/marketplace-data-pipeline/data:/app/data marketplace-data-pipeline-etl-extract python src/extract.py',
    )

    # Задача 2: Трансформация данных
    transform_task = BashOperator(
        task_id='run_transform',
        bash_command='docker run --rm --network marketplace-data-pipeline_pipeline-net -v /workspaces/marketplace-data-pipeline/src:/app/src -v /workspaces/marketplace-data-pipeline/data:/app/data marketplace-data-pipeline-etl-transform python src/transform.py',
    )

    # Задаём порядок выполнения: сначала extract, потом transform
    extract_task >> transform_task