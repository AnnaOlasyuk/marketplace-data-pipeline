# Базовый лёгкий образ Python
FROM python:3.10-slim

# Устанавливаем Java (нужна для PySpark)
RUN apt-get update && apt-get install -y default-jre curl && rm -rf /var/lib/apt/lists/*

# Переменные окружения для Spark
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

# Устанавливаем Python-библиотеки
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    boto3 \
    pandas \
    moto[server]

# Рабочая директория внутри контейнера
WORKDIR /app

# Копируем наш код и данные
COPY src/ /app/src/
COPY data/ /app/data/

# Команда по умолчанию
CMD ["python", "src/extract.py"]