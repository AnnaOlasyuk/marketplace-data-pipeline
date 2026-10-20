"""
Transform-слой пайплайна.
Читает сырые CSV, агрегирует данные и сохраняет в Parquet.
"""

import logging
import os
from datetime import datetime

from dotenv import load_dotenv 
load_dotenv()               

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, min, max, sum

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def init_spark():
    """Инициализация Spark-сессии с настройками."""
    logger.info("Инициализация Spark-сессии...")
    spark = SparkSession.builder \
        .appName("MarketplaceTransform") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv('S3_ENDPOINT_URL', 'http://moto:4566')) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID', 'test')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY', 'test')) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    logger.info("Spark-сессия успешно создана.")
    return spark


def read_raw_data(spark, bucket_name, date_str):
    """Чтение сырых данных из локальной папки (через Docker Volume)."""
    raw_path = f"/app/data/"
    logger.info(f"Чтение данных из: {raw_path}")
    
    df = spark.read.csv(raw_path, header=True, inferSchema=True)
    row_count = df.count()
    logger.info(f"Успешно прочитано строк: {row_count}")
    
    return df


def validate_and_clean_data(df):
    """
    Проверка качества данных и очистка.
    Возвращает очищенный DataFrame и статистику.
    """
    logger.info("=" * 60)
    logger.info("ЗАПУСК ПРОВЕРКИ КАЧЕСТВА ДАННЫХ (Data Quality)")
    logger.info("=" * 60)
    
    total_rows = df.count()
    logger.info(f"Всего строк до очистки: {total_rows}")
    
    # Проверка 1: Отрицательные цены
    negative_prices = df.filter(col("price") < 0).count()
    if negative_prices > 0:
        logger.warning(f"Найдено строк с отрицательной ценой: {negative_prices}. Они будут удалены.")
    
    # Проверка 2: Пустые категории
    null_categories = df.filter(col("category").isNull() | (col("category") == "")).count()
    if null_categories > 0:
        logger.warning(f"Найдено строк с пустой категорией: {null_categories}. Они будут удалены.")
    
    # Проверка 3: Нулевые цены
    zero_prices = df.filter(col("price") == 0).count()
    if zero_prices > 0:
        logger.warning(f"Найдено строк с нулевой ценой: {zero_prices}. Они будут удалены.")
    
    # Очистка данных
    clean_df = df.filter(
        (col("price") > 0) & 
        (col("category").isNotNull()) & 
        (col("category") != "")
    )
    
    clean_rows = clean_df.count()
    removed_rows = total_rows - clean_rows
    
    logger.info(f"Всего строк после очистки: {clean_rows}")
    logger.info(f"Удалено строк: {removed_rows} ({removed_rows/total_rows*100:.1f}% от общего количества)")
    logger.info("=" * 60)
    
    return clean_df


def transform_data(df):
    """Трансформация данных: агрегация по категориям."""
    if df.isEmpty():
        logger.warning("Входной DataFrame пуст. Обработка прервана.")
        return None
    
    logger.info("Запуск агрегации данных по категориям...")
    processed_df = df.groupBy("category") \
        .agg(
            avg("price").alias("avg_price"),
            sum("price").alias("total_revenue"),
            min("price").alias("min_price"),
            max("price").alias("max_price"),
            count("*").alias("product_count")
        ) \
        .orderBy(col("product_count").desc())
    
    logger.info(f"Агрегация завершена. Количество уникальных категорий: {processed_df.count()}")
    return processed_df


def save_processed_data(df, bucket_name, date_str):
    """Сохранение обработанных данных локально в формате Parquet."""
    processed_path = f"/app/processed/{date_str}/"
    logger.info(f"Сохранение данных в: {processed_path}")
    
    df.write.mode("overwrite").parquet(processed_path)
    logger.info("Данные успешно сохранены в формат Parquet.")


def main():
    """Главная функция пайплайна."""
    logger.info("=" * 60)
    logger.info("ЗАПУСК TRANSFORM-СЛОЯ")
    logger.info("=" * 60)
    
    bucket_name = os.getenv('BUCKET_NAME', 'marketplace-raw-data')
    today = datetime.now().strftime('%Y%m%d')
    
    try:
        spark = init_spark()
        raw_df = read_raw_data(spark, bucket_name, today)
        
        # НОВЫЙ ШАГ: Проверка и очистка данных
        clean_df = validate_and_clean_data(raw_df)
        
        # Трансформация очищенных данных
        processed_df = transform_data(clean_df)
        
        if processed_df is not None:
            save_processed_data(processed_df, bucket_name, today)
            logger.info("\nИтоговый результат трансформации:")
            processed_df.show(truncate=False)
        
        logger.info("=" * 60)
        logger.info("TRANSFORM-СЛОЙ ЗАВЕРШЁН УСПЕШНО")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.exception("Критическая ошибка в Transform-слое. Пайплайн остановлен.")


        raise
    finally:
        if 'spark' in locals():
            logger.info("Остановка Spark-сессии и освобождение ресурсов.")
            spark.stop()


if __name__ == "__main__":
    main()