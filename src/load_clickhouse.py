import json
import os
from clickhouse_driver import Client
from datetime import datetime

def load_to_clickhouse(json_file_path: str):
    """
    Загружает данные из JSON файла в ClickHouse.
    """
    # Подключаемся к ClickHouse. 
    # 'clickhouse' - это имя сервиса из docker-compose.yml
    client = Client(host='clickhouse', port=9000, user='default', password='')

    # 1. Создаем таблицу, если её нет
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
    ORDER BY (date, supplier_article)
    """
    
    print("🛠 Создаем таблицу в ClickHouse...")
    client.execute(create_table_query)

    # 2. Читаем данные из JSON
    if not os.path.exists(json_file_path):
        print(f"❌ Файл {json_file_path} не найден! (Возможно, сначала нужно запустить Extract)")
        return

    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 3. Подготавливаем данные для вставки
    rows_to_insert = []
    for item in data:
        try:
            # Преобразуем строку в объект даты, который понимает ClickHouse
            date_str = item.get('date', '1970-01-01')[:10]
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            row = (
                date_obj,  # <-- было date_str, стало date_obj
                item.get('supplier_article', ''),
                item.get('tech_size', ''),
                item.get('barcode', ''),
                float(item.get('total_price', 0)),
                int(item.get('discount_percent', 0)),
                item.get('warehouse_name', ''),
                item.get('oblast', ''),
                int(item.get('nm_id', 0)),
                item.get('subject', ''),
                item.get('category', ''),
                item.get('brand', ''),
                bool(item.get('is_cancel', False))
            )
            rows_to_insert.append(row)
        except Exception as e:
            print(f"Ошибка обработки строки: {e}")

    # 4. Вставляем данные
    if rows_to_insert:
        insert_query = """
        INSERT INTO wb_sales (
            date, supplier_article, tech_size, barcode, total_price, 
            discount_percent, warehouse_name, oblast, nm_id, subject, 
            category, brand, is_cancel
        ) VALUES
        """
        print(f"🚀 Загружаем {len(rows_to_insert)} строк в ClickHouse...")
        client.execute(insert_query, rows_to_insert)
        print("✅ Успешно загружено в ClickHouse!")
    else:
        print("⚠️ Нет данных для загрузки.")

if __name__ == "__main__":
    import glob
    import os
    
    # Ищем все JSON файлы в папке data/raw (относительный путь работает, т.к. мы в /opt/airflow)
    files = glob.glob('data/raw/*.json')
    
    if not files:
        print("❌ В папке data/raw нет ни одного JSON файла! Сначала запусти задачу Extract.")
    else:
        # Находим самый свежий файл по времени изменения
        latest_file = max(files, key=os.path.getmtime)
        print(f"📂 Найден последний файл для загрузки: {latest_file}")
        
        # Вызываем функцию загрузки с правильным путем
        load_to_clickhouse(latest_file)

   