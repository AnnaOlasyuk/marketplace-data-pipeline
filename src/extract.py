import os
import requests
import json
import shutil
from datetime import datetime, timedelta
import logging


def ensure_mock_file_exists():
    """
    Создаёт mock_wb_response.json, если он не существует.
    Это нужно для того, чтобы проект работал "из коробки" без ручной настройки.
    """
    mock_path = os.path.join(os.path.dirname(__file__), "mock_wb_response.json")
    
    if not os.path.exists(mock_path):
        logging.info("Файл mock_wb_response.json не найден. Создаём файл с тестовыми данными...")
        
        mock_data = [
            {
                "date": "2023-10-20T10:15:30Z",
                "lastChangeDate": "2023-10-20T10:20:00Z",
                "warehouseName": "Коледино",
                "countryName": "Россия",
                "oblastOkrugName": "Московская область",
                "regionName": "Москва",
                "supplierArticle": "WB-ART-001",
                "nmId": 154321098,
                "brand": "Мой Бренд",
                "tsName": "Футболка оверсайз",
                "price": 1500,
                "discountPercent": 20,
                "isSupply": True,
                "isRealization": True,
                "totalPrice": 1200,
                "sticker": "1234567890123"
            },
            {
                "date": "2023-10-21T14:00:00Z",
                "lastChangeDate": "2023-10-21T14:05:00Z",
                "warehouseName": "Электросталь",
                "countryName": "Россия",
                "oblastOkrugName": "Московская область",
                "regionName": "Москва",
                "supplierArticle": "WB-ART-002",
                "nmId": 987654321,
                "brand": "Мой Бренд",
                "tsName": "Джинсы прямые",
                "price": 3500,
                "discountPercent": 10,
                "isSupply": True,
                "isRealization": True,
                "totalPrice": 3150,
                "sticker": "9876543210987"
            }
        ]
        
        with open(mock_path, "w", encoding="utf-8") as f:
            json.dump(mock_data, f, ensure_ascii=False, indent=2)
        
        logging.info(f"Файл создан: {mock_path}")



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_wb_sales(api_key: str = None, days_ago: int = 7, use_mock: bool = False):
    """
    Выгружает данные о заказах. 
    Если use_mock=True, использует локальный файл-имитацию.
    """
    os.makedirs("data/raw", exist_ok=True)
    filename = f"data/raw/wb_sales_{datetime.now().strftime('%Y%m%d')}.json"
    
    if use_mock:
        logging.info("Режим MOCK: Используем тестовые данные Wildberries")
        
        # АВТОМАТИЧЕСКИ СОЗДАЁМ ФАЙЛ, ЕСЛИ ЕГО НЕТ
        ensure_mock_file_exists()
        
        mock_path = os.path.join(os.path.dirname(__file__), "mock_wb_response.json")
        shutil.copy(mock_path, filename)
        logging.info(f"✅ Mock-данные успешно скопированы в {filename}")
        return filename

    # --- РЕЖИМ РЕАЛЬНОГО API ---
    logging.info("Режим REAL: Запрос к реальному API Wildberries")
    base_url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    
    date_to = datetime.now()
    date_from = date_to - timedelta(days=days_ago)
    
    params = {
        "dateFrom": date_from.strftime("%Y-%m-%d"),
        "dateTo": date_to.strftime("%Y-%m-%d")
    }
    
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(base_url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        logging.info(f"Реальные данные успешно сохранены в {filename}. Записей: {len(data)}")
        return filename
        
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"❌ HTTP ошибка: {http_err}. Ответ сервера: {response.text}")
        raise
    except Exception as err:
        logging.error(f"❌ Ошибка: {err}")
        raise

if __name__ == "__main__":
    # Для локального теста включаем MOCK режим по умолчанию
    extract_wb_sales(use_mock=True)