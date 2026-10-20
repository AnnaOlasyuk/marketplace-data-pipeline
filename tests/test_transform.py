import pytest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Добавляем папку src в путь, чтобы Python мог найти наш модуль transform
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from transform import validate_and_clean_data


# Создаем локальную Spark-сессию специально для тестов
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("pytest-pyspark-local-testing") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_validate_and_clean_data_removes_invalid_rows(spark):
    """Тест проверяет, что функция удаляет строки с плохими данными."""
    
    # 1. Создаем тестовые данные (смесь хороших и "грязных" строк)
    data = [
        ("electronics", 100.0),    # Хорошая строка
        ("food", -50.0),           # Плохая: отрицательная цена
        (None, 200.0),             # Плохая: категория None
        ("clothing", 0.0),         # Плохая: нулевая цена
        ("", 300.0),               # Плохая: пустая строка категории
        ("books", 50.0)            # Хорошая строка
    ]
    
    # Описываем схему данных (как в CSV: category и price)
    schema = StructType([
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True)
    ])
    
    # Создаем DataFrame из тестовых данных
    df = spark.createDataFrame(data, schema)

    # 2. Вызываем тестируемую функцию
    clean_df = validate_and_clean_data(df)

    # 3. ПРОВЕРКИ (Assertions)
    # Проверяем, что осталось ровно 2 строки (только хорошие)
    assert clean_df.count() == 2, f"Ожидали 2 строки, но получили {clean_df.count()}"
    
    # Проверяем, что остались именно нужные категории
    remaining_categories = [row.category for row in clean_df.collect()]
    assert "electronics" in remaining_categories, "Категория 'electronics' должна была сохраниться"
    assert "books" in remaining_categories, "Категория 'books' должна была сохраниться"
    
    # Проверяем, что плохие категории точно удалены
    assert "food" not in remaining_categories
    assert "clothing" not in remaining_categories