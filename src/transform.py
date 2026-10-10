from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
import os

# Инициализация Spark
spark = SparkSession.builder \
    .appName("MarketplaceTransform") \
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')) \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID', 'test')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY', 'test')) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

BUCKET_NAME = 'marketplace-raw-data'

# Чтение данных из S3
df = spark.read.csv(f"s3a://{BUCKET_NAME}/raw/20261010/", header=True, inferSchema=True)

# Трансформации
processed_df = df.groupBy("category") \
    .agg(
        avg("price").alias("avg_price"),
        count("*").alias("product_count")
    )

# Сохранение результата
processed_df.write.mode("overwrite").parquet(f"s3a://{BUCKET_NAME}/processed/20261010/")

print("✅ Данные обработаны и сохранены в S3!")
spark.stop()