# Marketplace Data Pipeline (S3 ETL)

ETL-пайплайн для обработки данных маркетплейса с использованием PySpark и Docker.


## Что реализовано

- **Extract**: загрузка CSV в S3 через boto3
- **Transform**: агрегация данных по категориям с проверкой качества
- **Data Quality**: фильтрация записей с отрицательной/нулевой ценой и пустыми категориями
- **Logging**: профессиональное логирование вместо print
- **Config**: вынесено в .env (12-Factor App)
- **Tests**: unit-тесты на pytest

## Технологии

- Python 3.10
- PySpark 3.5.0
- Docker
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

**Автор**:`Anna Olasyuk`
GitHub: github.com/AnnaOlasyuk