import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from datetime import datetime, timedelta
import time
import schedule
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from itertools import islice
import json

load_dotenv()

# Константы
CREDS_FILE = 'credentials.json'
SHEET_URL = os.getenv('GOOGLE_SHEETS_URL')
API_KEY = os.getenv('MPSTATS_API_KEY')
BASE_URL = 'https://mpstats.io/api/wb/get/item'
BATCH_SIZE = 50  # Оптимальный размер пакета
REQUEST_DELAY = 2  # Задержка между пакетами
MAX_RETRIES = 3
MAX_REQUESTS_PER_MINUTE = 100  # Лимит API


class MpStatsAPIError(Exception):
    """Кастомное исключение для ошибок API"""
    pass


def connect_google():
    """Подключение к Google Sheets с проверкой credentials"""
    if not os.path.exists(CREDS_FILE):
        raise FileNotFoundError(f"Файл учетных данных {CREDS_FILE} не найден")

    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
        return gspread.authorize(creds)
    except (json.JSONDecodeError, KeyError) as e:
        raise ValueError(f"Ошибка в файле учетных данных: {e}") from e


@retry(stop=stop_after_attempt(MAX_RETRIES),
       wait=wait_exponential(multiplier=1, min=2, max=10),
       reraise=True)
def fetch_batch_data(skus, date_str):
    """Безопасный запрос данных для пакета артикулов"""
    if not API_KEY:
        raise ValueError("API_KEY не установлен")

    if not isinstance(skus, list) or len(skus) == 0:
        raise ValueError("Список артикулов должен быть непустым списком")

    url = f"{BASE_URL}/batch/balance_by_day"
    payload = {"d": date_str, "skus": ",".join(str(sku) for sku in skus)}

    try:
        response = requests.get(
            url,
            headers={"X-Mpstats-TOKEN": API_KEY},
            params=payload,
            timeout=15
        )

        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            print(f"Превышен лимит запросов. Пауза {retry_after} сек.")
            time.sleep(retry_after)
            raise MpStatsAPIError("Превышен лимит запросов")

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        raise MpStatsAPIError(f"Ошибка API: {str(e)}") from e


def validate_skus(sku_list):
    """Валидация и очистка списка артикулов"""
    return [str(sku).strip() for sku in sku_list if str(sku).strip().isdigit()]


def prepare_sheet_data(batch_data, date_str):
    """Подготовка данных для вставки в таблицу"""
    if not batch_data or not isinstance(batch_data, dict):
        return []

    sheet_data = []
    for sku, items in batch_data.items():
        if not items or not isinstance(items, list):
            continue

        item = items[0]
        if not isinstance(item, dict):
            continue

        sheet_data.append([
            date_str,
            sku,
            item.get("price", ""),
            item.get("final_price", ""),
            item.get("sales", "")
        ])

    return sheet_data


def process_skus(gc, skus, date_str):
    """Обработка всех артикулов с разбиением на пакеты"""
    total_skus = len(skus)
    processed = 0
    batch_num = 0

    while processed < total_skus:
        batch_num += 1
        batch = skus[processed:processed + BATCH_SIZE]

        print(f"\nПакет {batch_num}: артикулы {processed + 1}-{min(processed + BATCH_SIZE, total_skus)}")

        try:
            # Получаем данные
            batch_data = fetch_batch_data(batch, date_str)
            sheet_data = prepare_sheet_data(batch_data, date_str)

            if sheet_data:
                # Вставляем данные
                sheet = gc.open_by_url(SHEET_URL)
                data_ws = sheet.worksheet("Данные")
                data_ws.append_rows(sheet_data)
                print(f"Добавлено {len(sheet_data)} записей")

            processed += len(batch)

            # Соблюдаем лимиты API
            if batch_num % (MAX_REQUESTS_PER_MINUTE // BATCH_SIZE) == 0:
                print("Пауза для соблюдения лимитов API")
                time.sleep(60)
            else:
                time.sleep(REQUEST_DELAY)

        except Exception as e:
            print(f"Ошибка обработки пакета: {str(e)}")
            # Пропускаем проблемный пакет и продолжаем
            processed += len(batch)
            continue


def daily_collect():
    """Основная функция сбора данных с улучшенной обработкой ошибок"""
    date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"\n=== Начало сбора данных за {date_str} в {datetime.now()} ===")

    try:
        # Инициализация подключения
        gc = connect_google()
        sheet = gc.open_by_url(SHEET_URL)

        # Получаем артикулы
        sku_ws = sheet.worksheet("Артикулы")
        raw_skus = sku_ws.col_values(1)[1:]  # Пропускаем заголовок
        skus = validate_skus(raw_skus)

        if not skus:
            print("Нет валидных артикулов для обработки")
            return

        print(f"Найдено {len(skus)} валидных артикулов")

        # Обработка данных
        process_skus(gc, skus, date_str)

        print(f"\n=== Сбор данных завершен в {datetime.now()} ===")

    except Exception as e:
        print(f"\n!!! Критическая ошибка: {str(e)} !!!")


if __name__ == "__main__":
    # Проверка окружения
    if not all([SHEET_URL, API_KEY]):
        print("Ошибка: Необходимо установить GOOGLE_SHEETS_URL и MPSTATS_API_KEY в .env")
        exit(1)

    daily_collect()

    # Настройка расписания
    schedule.every().day.at("08:00").do(daily_collect)
    print("Скрипт запущен. Ожидание следующего запуска по расписанию...")

    # Основной цикл
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            print("\nСкрипт остановлен пользователем")
            break
        except Exception as e:
            print(f"Ошибка в основном цикле: {str(e)}")
            time.sleep(60)