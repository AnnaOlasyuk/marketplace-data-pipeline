# Используем актуальную стабильную версию Airflow (на базе Debian 12)
FROM apache/airflow:2.9.3

USER root

# Обновляем пакеты и устанавливаем Java и wget (в Debian 12 репозитории работают стабильно)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Копируем requirements и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Динамически определяем путь к site-packages и скачиваем JAR-файлы для PySpark + S3
# Это гарантирует, что путь будет правильным независимо от версии Python (3.10 или 3.11)
RUN JARS_DIR="$(python3 -c 'import site; print(site.getsitepackages()[0])')/pyspark/jars" && \
    mkdir -p "$JARS_DIR" && \
    wget -q -O "$JARS_DIR/hadoop-aws-3.3.4.jar" https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q -O "$JARS_DIR/aws-java-sdk-bundle-1.12.262.jar" https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Копируем наш код
COPY src/ /opt/airflow/src/