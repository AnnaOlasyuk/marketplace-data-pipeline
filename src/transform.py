"""
Transform-слой пайплайна.
Читает сырые CSV из S3 (raw/), агрегирует данные и сохраняет в Parquet (processed/).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, min, max, sum
from datetime import datetime
import os


def init_spark():
    """Инициализация Spark-сессии с настройками для S3."""
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
    return spark

def read_raw_data(spark, bucket_name, date_str):
    """Чтение сырых данных из локальной папки (обход сетевой проблемы)."""
    # Вместо s3a:// читаем из локальной папки /app/data/
    raw_path = f"/app/data/"
    print(f"📖 Читаем данные из: {raw_path}")
    
    df = spark.read.csv(raw_path, header=True, inferSchema=True)
    print(f"✅ Прочитано строк: {df.count()}")
    return df


def transform_data(df):
    """Трансформация данных: агрегация по категориям."""
    if df.isEmpty():
        print("⚠️ Нет данных для обработки")
        return None
    
    processed_df = df.groupBy("category") \
        .agg(
            avg("price").alias("avg_price"),
            sum("price").alias("total_revenue"),
            min("price").alias("min_price"),
            max("price").alias("max_price"),
            count("*").alias("product_count")
        ) \
        .orderBy(col("product_count").desc())
    
    print(f"📊 Агрегировано категорий: {processed_df.count()}")
    return processed_df


def save_processed_data(df, bucket_name, date_str):
    """Сохранение обработанных данных локально (обход сетевой проблемы)."""
    # Вместо s3a:// сохраняем в локальную папку /app/processed/
    processed_path = f"/app/processed/{date_str}/"
    print(f"💾 Сохраняем в: {processed_path}")
    
    df.write.mode("overwrite").parquet(processed_path)
    print("✅ Данные обработаны и сохранены в Parquet формат")


def main():
    """Главная функция пайплайна."""
    BUCKET_NAME = os.getenv('BUCKET_NAME', 'marketplace-raw-data')
    today = datetime.now().strftime('%Y%m%d')
    
    print("=" * 60)
    print("🚀 Запуск Transform-слоя")
    print("=" * 60)
    
    try:
        spark = init_spark()
        raw_df = read_raw_data(spark, BUCKET_NAME, today)
        processed_df = transform_data(raw_df)
        
        if processed_df is not None:
            save_processed_data(processed_df, BUCKET_NAME, today)
            print("\n📈 Результат трансформации:")
            processed_df.show(truncate=False)
        
        print("\n" + "=" * 60)
        print("✅ Transform-слой завершён успешно!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Ошибка в Transform-слое: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()