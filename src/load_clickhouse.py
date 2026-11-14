import json
import os
import logging
from datetime import date, datetime
from typing import Iterator, Tuple

from pydantic import BaseModel, ValidationError, field_validator
from clickhouse_driver import Client
from airflow.hooks.base import BaseHook

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==========================================
# 1. Схема данных (Pydantic)
# ==========================================
class WBSalesItem(BaseModel):
    date: date
    supplier_article: str = ""
    tech_size: str = ""
    barcode: str = ""
    total_price: float = 0.0
    discount_percent: int = 0
    warehouse_name: str = ""
    oblast: str = ""
    nm_id: int = 0
    subject: str = ""
    category: str = ""
    brand: str = ""
    is_cancel: bool = False

    @field_validator('date', mode='before')
    @classmethod
    def parse_custom_date(cls, v):
        """Безопасный парсер даты: обрезает время, если API отдает datetime."""
        if isinstance(v, str):
            return v[:10]
        return v

# ==========================================
# 2. Потоковое чтение и валидация
# ==========================================
def _read_and_validate_ndjson(file_path: str) -> Iterator[Tuple]:
    """Генератор: читает NDJSON, валидирует через Pydantic и отдает кортежи для ClickHouse."""
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                item_dict = json.loads(line)
                item = WBSalesItem(**item_dict)
                
                yield (
                    item.date,
                    item.supplier_article,
                    item.tech_size,
                    item.barcode,
                    item.total_price,
                    item.discount_percent,
                    item.warehouse_name,
                    item.oblast,
                    item.nm_id,
                    item.subject,
                    item.category,
                    item.brand,
                    item.is_cancel
                )
            except ValidationError as e:
                logging.warning(f"⚠️ Строка {line_num} не прошла валидацию схемы: {e.errors()}")
            except json.JSONDecodeError:
                logging.warning(f"⚠️ Строка {line_num}: битый JSON.")
            except Exception as e:
                logging.error(f"❌ Непредвиденная ошибка на строке {line_num}: {e}")

# ==========================================
# 3. Основная функция загрузки
# ==========================================
def load_to_clickhouse(json_file_path: str, **kwargs):
    """
    Загружает данные в ClickHouse.
    Принимает **kwargs, чтобы получить контекст Airflow.
    """
    ds = kwargs.get('ds', datetime.now().strftime('%Y-%m-%d'))
    client = None # Инициализируем заранее

    # Безопасное получение кредов из Airflow Connections
    try:
        conn = BaseHook.get_connection('clickhouse_default')
        client = Client(
            host=conn.host or 'clickhouse',
            port=int(conn.port or 9000),
            user=conn.login or 'default',
            password=conn.password or ''
        )
    except Exception as e:
        logging.error(f"Не удалось получить подключение из Airflow: {e}. Падаем.")
        raise

    try:
        # Создаем таблицу (если её нет)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS wb_sales (
            date Date, 
            supplier_article String, 
            tech_size String, 
            barcode String,
            total_price Float64, 
            discount_percent Int32, 
            warehouse_name String,
            oblast String, 
            nm_id Int64, 
            subject String, 
            category String,
            brand String, 
            is_cancel Boolean
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (date, supplier_article)
        """
        logging.info("🛠 Проверяем/создаем таблицу в ClickHouse...")
        client.execute(create_table_query)

        # ИДЕМПОТЕНТНОСТЬ: Удаляем данные за текущий день перед загрузкой
        delete_query = f"""
        ALTER TABLE wb_sales DELETE WHERE date = '{ds}'
        SETTINGS mutations_sync = 1
        """
        logging.info(f"🧹 Чистим старые данные за {ds} (обеспечиваем идемпотентность)...")
        client.execute(delete_query)

        # Чтение и батчевая вставка
        insert_query = """
        INSERT INTO wb_sales (
            date, supplier_article, tech_size, barcode, total_price, 
            discount_percent, warehouse_name, oblast, nm_id, subject, 
            category, brand, is_cancel
        ) VALUES
        """

        batch = []
        total_inserted = 0
        batch_size = 50_000
        
        logging.info("🚀 Начинаем загрузку...")
        for row in _read_and_validate_ndjson(json_file_path):
            batch.append(row)

            if len(batch) >= batch_size:
                client.execute(insert_query, batch)
                total_inserted += len(batch)
                logging.info(f"✅ Загружено {total_inserted} строк...")
                batch = []

        if batch:
            client.execute(insert_query, batch)
            total_inserted += len(batch)

        logging.info(f"🎉 УСПЕХ! Всего загружено в ClickHouse: {total_inserted} строк за {ds}.")

    except Exception as e:
        logging.error(f"❌ Критическая ошибка при работе с ClickHouse: {e}")
        raise
    finally:
        if client is not None:
            client.disconnect()