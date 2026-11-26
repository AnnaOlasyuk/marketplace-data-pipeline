import pytest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
# ДОБАВЛЕНА функция transform_data в импорт
from transform import validate_and_clean_data, transform_data

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


def test_transform_data_calculates_correct_aggregations(spark):
    """Тест проверяет, что функция агрегации правильно считает выручку, количество и добавляет оконные функции."""
    
    # 1. Создаем тестовые данные (добавлен nm_id!)
    data = [
        ("uuid-1", "2024-01-01", 0, 100.0, 2),  # День 1, не промо, цена 100, кол-во 2. Выручка: 200
        ("uuid-2", "2024-01-01", 0, 200.0, 1),  # День 1, не промо, цена 200, кол-во 1. Выручка: 200
        ("uuid-3", "2024-01-02", 1, 50.0, 10),  # День 2, промо, цена 50, кол-во 10. Выручка: 500
    ]
    
    schema = StructType([
        StructField("nm_id", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("is_promo", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("qty", LongType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)

    # 2. Вызываем функцию агрегации
    result_df = transform_data(df)

    # 3. Проверяем результаты (Assertions)
    assert result_df.count() == 2
    
    results = {(row.dt, row.is_promo): row for row in result_df.collect()}
    
    # Проверка базовых агрегатов
    assert results[("2024-01-01", 0)]["total_qty_sold"] == 3
    assert results[("2024-01-01", 0)]["total_revenue"] == 400.0
    assert results[("2024-01-02", 1)]["total_revenue"] == 500.0
    
    # НОВАЯ ПРОВЕРКА: Проверяем наличие колонок, добавленных оконными функциями
    assert "revenue_moving_avg_3d" in result_df.columns, "Колонка со скользящим средним не найдена!"
    assert "revenue_rank" in result_df.columns, "Колонка с рангом не найдена!"
