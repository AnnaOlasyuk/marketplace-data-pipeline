# Marketplace Data Pipeline (S3 ETL)

ETL-пайплайн для обработки данных маркетплейса с использованием PySpark, Docker и Apache Airflow.

## Что реализовано

- **Extract**: загрузка CSV в S3 через boto3
- **Transform**: агрегация данных по категориям с проверкой качества
- **Оркестрация**: Apache Airflow (DAGs) с зависимостями между задачами
- **Data Quality**: фильтрация записей с отрицательной/нулевой ценой и пустыми категориями
- **Logging**: профессиональное логирование через модуль `logging`
- **Config**: вынесено в `.env` (стандарт 12-Factor App)
- **Tests**: unit-тесты на pytest
- **CI/CD**: автоматический запуск тестов через GitHub Actions

## Технологии

- Python 3.10
- PySpark 3.5.0
- Docker & Docker Compose
- Apache Airflow 2.6.3
- Moto (эмулятор AWS S3)
- boto3
- pytest
- python-dotenv


## Запуск
```bash
docker-compose build
docker-compose up
```

## Тесты
```bash
docker-compose run --rm etl-transform python -m pytest tests/ -v 
```

## Orchestration with Apache Airflow

The ETL pipeline is orchestrated using Apache Airflow. The DAG (`dags/marketplace_etl_dag.py`) defines two tasks:

1. **run_extract**: Extracts data from source and loads to S3 (Moto)
2. **run_transform**: Transforms data using PySpark with Data Quality checks


### Running with Airflow

1. Start Airflow standalone:
   ```bash
   docker run -d --name airflow-standalone -p 8080:8080 \
     -v $(pwd)/dags:/opt/airflow/dags \
     -v $(pwd):/opt/airflow/project \
     -v /var/run/docker.sock:/var/run/docker.sock \
     apache/airflow:2.6.3 standalone

## Настройка

### Mock-режим (по умолчанию)
Пайплайн работает сразу после клонирования. Данные генерируются автоматически.

### Реальный API Wildberries
1. Получи токен в [кабинете поставщика](https://seller.wildberries.ru/)
2. В Airflow UI создай переменную:
   - Admin → Variables → Create
   - Key: `wb_api_key`
   - Val: `твой_токен`
3. В файле `dags/marketplace_etl_dag.py` измени:
   ```python
   'api_key': '{{ var.value.wb_api_key }}',
   'use_mock': False

**Автор**:`Anna Olasyuk`
GitHub: github.com/AnnaOlasyuk