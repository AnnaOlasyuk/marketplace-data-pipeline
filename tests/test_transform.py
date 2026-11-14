import pytest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from transform import validate_and_clean_data

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("pytest-pyspark-local-testing") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_validate_and_clean_data_removes_invalid_rows(spark):
    """Тест проверяет, что функция удаляет строки с отрицательной ценой, пустым ID и отрицательным qty."""
    
    # Создаем тестовые данные (схема как в Parquet)
    data = [
        ("uuid-1", 5, 100.0, 1),    # Хорошая строка
        ("uuid-2", -2, 50.0, 0),    # Плохая: отрицательное количество
        (None, 3, 200.0, 0),        # Плохая: пустой nm_id
        ("uuid-3", 1, 0.0, 1),      # Плохая: нулевая цена
        ("uuid-4", 2, 300.0, 0)     # Хорошая строка
    ]
    
    # Схема колонок
    schema = StructType([
        StructField("nm_id", StringType(), True),
        StructField("qty", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("is_promo", LongType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)

    # Вызываем функцию очистки
    clean_df = validate_and_clean_data(df)

    # Проверяем, что осталось ровно 2 строки
    assert clean_df.count() == 2, f"Ожидали 2 строки, но получили {clean_df.count()}"
    
    # Проверяем, что остались именно нужные ID
    remaining_ids = [row.nm_id for row in clean_df.collect()]
    assert "uuid-1" in remaining_ids
    assert "uuid-4" in remaining_ids
    assert "uuid-2" not in remaining_ids