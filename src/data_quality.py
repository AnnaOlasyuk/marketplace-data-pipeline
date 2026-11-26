import logging
import pandas as pd
import great_expectations as gx
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

def check_data_quality(df: DataFrame) -> bool:
    """
    Проверяет качество данных с помощью Great Expectations 1.25.0.
    """
    logger.info("=" * 60)
    logger.info("ЗАПУСК ПРОВЕРКИ КАЧЕСТВА ДАННЫХ (Great Expectations)")
    
    # Конвертируем Spark DataFrame в Pandas (берем 10 000 строк для быстроты)
    pdf = df.limit(10000).toPandas()
    
    try:
        # 1. Инициализируем контекст
        context = gx.get_context()
        
        # 2. Создаем Data Source и Asset
        data_source = context.data_sources.add_pandas(name="my_pandas_datasource")
        data_asset = data_source.add_dataframe_asset(name="my_dataframe_asset")
        
        # 3. Строим Batch Request и передаем данные через options
        batch_request = data_asset.build_batch_request(
            options={"dataframe": pdf}
        )
        
        # 4. Создаем Validator БЕЗ явного ExpectationSuite
        # В GE 1.25.0 можно использовать default_suite
        validator = context.get_validator(
            batch_request=batch_request,
            create_expectation_suite_with_name="wb_sales_suite_auto"
        )

        # --- ПИШЕМ ПРОВЕРКИ (Ожидания) ---
        
        validator.expect_column_values_to_be_between("price", min_value=0)
        validator.expect_column_values_to_be_between("qty", min_value=0)
        validator.expect_column_values_to_not_be_null("nm_id")
        validator.expect_column_to_exist("dt")

        # Запускаем проверки
        results = validator.validate()
        
        # Смотрим, все ли проверки прошли
        if results["success"]:
            logger.info("✅ Все проверки качества данных ПРОЙДЕНЫ.")
            return True
        else:
            logger.error("❌ Проверки качества данных НЕ ПРОЙДЕНЫ!")
            for result in results["results"]:
                if not result["success"]:
                    logger.error(f"Упала проверка: {result['expectation_config']['expectation_type']}")
                    logger.error(f"Детали: {result['result']}")
            return False

    except Exception as e:
        logger.error(f"❌ Ошибка при работе Great Expectations: {e}")
        return False