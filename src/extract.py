import json
import os
import logging
from datetime import datetime

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

def extract_wb_sales(**context):
    """
    Читает Parquet-датасет Wildberries и конвертирует в JSON для ClickHouse.
    Ожидает колонки: nm_id, dt, price, is_promo, prev_leftovers
    """
    source_file = "data/raw/wildberries_sales.parquet"
    
    if not os.path.exists(source_file):
        logging.error(f"❌ Файл {source_file} не найден!")
        raise FileNotFoundError("Скачай train.parquet и положи в data/raw/ как wildberries_sales.parquet")

    logging.info(f"📖 Читаем Parquet-датасет Wildberries из {source_file}...")
    
    # Справочники для генерации недостающих полей
    warehouses = ["Коледино", "Электросталь", "Казань", "Хабаровск", "Краснодар", "Санкт-Петербург"]
    oblasts = ["Московская обл.", "Татарстан", "Краснодарский край", "Свердловская обл.", "Новосибирская обл."]
    subjects = ["Платье", "Футболка", "Джинсы", "Кроссовки", "Сумка", "Куртка"]
    categories = ["Одежда", "Обувь", "Аксессуары", "Спорт"]
    brands = ["Zarina", "Gloria Jeans", "Befree", "Love Republic", "Sela", "ТВОЕ"]
    
    data = []
    
    try:
        # Читаем Parquet файл
        table = pq.read_table(source_file)
        df = table.to_pandas()
        
        logging.info(f"🔍 Колонки в файле: {list(df.columns)}")
        logging.info(f"📊 Всего строк: {len(df)}")
        
        for idx, row in df.iterrows():
            try:
                # 1. Берем реальные данные из датасета
                dt = str(row.get('dt', '2024-01-01'))[:10]
                price = float(row.get('price', 0) or 0)
                is_promo = int(row.get('is_promo', 0) or 0)
                nm_id_raw = str(row.get('nm_id', '0'))
                
                # 2. Преобразуем nm_id из UUID в число
                try:
                    nm_id = int(nm_id_raw[-10:], 16) if nm_id_raw else 0
                except (ValueError, TypeError):
                    nm_id = abs(hash(nm_id_raw)) % 10000000000
                
                # 3. Генерируем осмысленные заглушки для недостающих полей
                seed = abs(hash(nm_id_raw)) % 1000
                
                item = {
                    "date": dt,
                    "supplier_article": f"ART-{seed:05d}",
                    "tech_size": "ONE_SIZE",  # В датасете нет размеров, ставим универсальный
                    "barcode": f"{1000000000000 + seed}",
                    "total_price": round(price, 2),  # price уже итоговая сумма
                    "discount_percent": 15 if is_promo == 1 else 0,
                    "warehouse_name": warehouses[seed % len(warehouses)],
                    "oblast": oblasts[seed % len(oblasts)],
                    "nm_id": nm_id,
                    "subject": subjects[seed % len(subjects)],
                    "category": categories[seed % len(categories)],
                    "brand": brands[seed % len(brands)],
                    "is_cancel": False
                }
                data.append(item)
                
            except Exception as e:
                continue

    except Exception as e:
        logging.error(f"❌ Ошибка чтения Parquet: {e}")
        raise

    logging.info(f"✅ Успешно обработано строк: {len(data)}")

    # Ограничиваем до 10 000 строк для быстрого теста
    if len(data) > 10000:
        logging.info("✂️ Ограничиваем до 10 000 строк для теста")
        data = data[:10000]

    # Сохраняем в формате NDJSON (Newline Delimited JSON) для потокового чтения
    filename = f"data/raw/wb_sales_{datetime.now().strftime('%Y%m%d')}.json"
    os.makedirs("data/raw", exist_ok=True)
    
    logging.info(f"💾 Сохраняем JSON в формате NDJSON: {filename}")
    with open(filename, 'w', encoding='utf-8') as f:
        for item in data:
            # Пишем каждый словарь как отдельную строку
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
            
    logging.info(f"💾 JSON успешно сохранен: {filename}")
    
    return filename