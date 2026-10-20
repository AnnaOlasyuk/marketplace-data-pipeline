FROM python:3.10-slim

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y default-jre curl wget && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

# Устанавливаем Python-библиотеки (ВКЛЮЧАЯ pytest!)
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    boto3 \
    pandas \
    moto[server] \
    python-dotenv \
    pytest

# Скачиваем JAR-файлы для работы Spark с S3
RUN mkdir -p /usr/local/lib/python3.10/site-packages/pyspark/jars && \
    wget -q -O /usr/local/lib/python3.10/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q -O /usr/local/lib/python3.10/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /app

COPY src/ /app/src/
COPY tests/ /app/tests/ 

CMD ["python", "src/extract.py"]