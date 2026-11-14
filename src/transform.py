"""
Transform-слой пайплайна.
Читает сырой Parquet, фильтрует мусор и агрегирует данные.
"""

import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def init_spark():
    """Инициализация Spark-сессии."""
    logger.info("Инициализация Spark-сессии...")
    spark = SparkSession.builder \
        .appName("WildberriesTransform") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "false") \
        .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
        .getOrCreate()
    return spark

def read_raw_data(spark, date_str):
    """Чтение сырых данных из Parquet."""
    # Читаем исходный файл wildberries_sales.parquet (путь исправлен под Docker volume)
    raw_path = "/opt/airflow/data/raw/wildberries_sales.parquet"
    logger.info(f"Чтение данных из: {raw_path}")
    
    df = spark.read.parquet(raw_path)
    logger.info(f"Успешно прочитано строк: {df.count()}")
    return df

def validate_and_clean_data(df):
    """
    Проверка качества данных (Data Quality).
    Очищаем данные от аномалий.
    """
    logger.info("=" * 60)
    logger.info("ЗАПУСК ПРОВЕРКИ КАЧЕСТВА ДАННЫХ (Data Quality)")
    
    total_rows = df.count()
    
    # Очистка: цены > 0, количество >= 0, nm_id не пустой
    clean_df = df.filter(
        (col("price") > 0) & 
        (col("qty") >= 0) & 
        (col("nm_id").isNotNull())
    )
    
    clean_rows = clean_df.count()
    removed_rows = total_rows - clean_rows
    
    logger.info(f"Всего строк до очистки: {total_rows}")
    logger.info(f"Удалено строк: {removed_rows}")
    logger.info("=" * 60)
    
    return clean_df

def transform_data(df):
    """Трансформация: группируем по дням и промо-акциям."""
    if df.isEmpty():
        logger.warning("Входной DataFrame пуст. Обработка прервана.")
        return None
    
    logger.info("Запуск агрегации данных по дням и промо-акциям...")
    
    processed_df = df.groupBy("dt", "is_promo") \
        .agg(
            _sum("qty").alias("total_qty_sold"),
            _sum(col("price") * col("qty")).alias("total_revenue"),
            avg("price").alias("avg_price"),
            count("nm_id").alias("unique_items")
        ) \
        .orderBy("dt", "is_promo")
    
    logger.info(f"Агрегация завершена. Получено строк: {processed_df.count()}")
    return processed_df

def save_processed_data(df, date_str):
    """Сохранение обработанных данных в Parquet."""
    # Путь исправлен, чтобы не было ошибки PATH_NOT_FOUND при записи
    processed_path = f"/opt/airflow/processed/aggregated_sales_{date_str}/"
    logger.info(f"Сохранение данных в: {processed_path}")
    
    df.write.mode("overwrite").parquet(processed_path)
    logger.info("Данные успешно сохранены.")

def main():
    logger.info("ЗАПУСК TRANSFORM-СЛОЯ")
    today = datetime.now().strftime('%Y%m%d')
    
    try:
        spark = init_spark()
        raw_df = read_raw_data(spark, today)
        clean_df = validate_and_clean_data(raw_df)
        processed_df = transform_data(clean_df)
        
        if processed_df is not None:
            save_processed_data(processed_df, today)
            logger.info("\nИтоговый результат трансформации:")
            processed_df.show(5, truncate=False)
        
        logger.info("TRANSFORM-СЛОЙ ЗАВЕРШЁН УСПЕШНО")
        
    except Exception as e:
        logger.exception("Критическая ошибка в Transform-слое.")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()