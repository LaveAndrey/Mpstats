import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from datetime import datetime, timedelta
import time
import schedule
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from typing import List, Dict, Optional
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wb_data_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Константы
CREDS_FILE = 'credentials.json'
SHEET_URL = os.getenv('GOOGLE_SHEETS_URL')
API_KEY = os.getenv('MPSTATS_API_KEY')
BASE_URL = 'https://mpstats.io/api/wb/get/item'
REQUEST_DELAY = 1  # Задержка между запросами (в секундах)
MAX_RETRIES = 3
MAX_REQUESTS_PER_MINUTE = 100  # Лимит API


class MpStatsAPIError(Exception):
    """Кастомное исключение для ошибок API"""
    pass


def connect_google() -> gspread.Client:
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
def fetch_item_data(sku: str, date_str: str) -> Optional[Dict]:
    """Запрос данных для одного артикула"""
    if not API_KEY:
        raise ValueError("API_KEY не установлен")

    url = f"{BASE_URL}/{sku}/balance_by_day"
    params = {"d": date_str}

    try:
        response = requests.get(
            url,
            headers={"X-Mpstats-TOKEN": API_KEY},
            params=params,
            timeout=15
        )

        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            logger.warning(f"Превышен лимит запросов. Пауза {retry_after} сек.")
            time.sleep(retry_after)
            raise MpStatsAPIError("Превышен лимит запросов")

        response.raise_for_status()
        data = response.json()

        if not data:
            logger.warning(f"Нет данных для артикула {sku} на дату {date_str}")
            return None

        return data[0]  # Возвращаем первый элемент списка

    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса для артикула {sku}: {str(e)}")
        raise MpStatsAPIError(f"Ошибка API: {str(e)}") from e


def validate_skus(sku_list: List[str]) -> List[str]:
    """Валидация и очистка списка артикулов"""
    return [str(sku).strip() for sku in sku_list if str(sku).strip().isdigit()]


def prepare_row_data(item_data: Dict, date_str: str, sku: str) -> List:
    """Подготовка строки для вставки в таблицу"""
    return [
        date_str,
        sku,
        item_data.get("price", ""),
        item_data.get("final_price", ""),
        item_data.get("sales", "")
    ]


def process_skus(gc: gspread.Client, skus: List[str], date_str: str):
    """Обработка всех артикулов по одному"""
    total_skus = len(skus)
    processed = 0
    request_count = 0

    # Открываем таблицу один раз
    sheet = gc.open_by_url(SHEET_URL)
    data_ws = sheet.worksheet("Данные")

    for sku in skus:
        processed += 1
        request_count += 1
        logger.info(f"Обработка артикула {processed}/{total_skus}: {sku}")

        try:
            # Получаем данные
            item_data = fetch_item_data(sku, date_str)

            if item_data:
                # Подготавливаем и добавляем данные
                row_data = prepare_row_data(item_data, date_str, sku)
                data_ws.append_row(row_data)
                logger.info(f"Добавлены данные для артикула {sku}")

            # Соблюдаем лимиты API
            if request_count >= MAX_REQUESTS_PER_MINUTE:
                logger.info("Достигнут лимит запросов. Пауза 60 сек.")
                time.sleep(60)
                request_count = 0
            else:
                time.sleep(REQUEST_DELAY)

        except Exception as e:
            logger.error(f"Ошибка обработки артикула {sku}: {str(e)}")
            continue


def daily_collect():
    """Основная функция сбора данных"""
    date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"\n=== Начало сбора данных за {date_str} ===")

    try:
        # Инициализация подключения
        gc = connect_google()

        # Получаем артикулы
        sheet = gc.open_by_url(SHEET_URL)
        sku_ws = sheet.worksheet("Артикулы")
        raw_skus = sku_ws.col_values(1)[1:]  # Пропускаем заголовок
        skus = validate_skus(raw_skus)

        if not skus:
            logger.warning("Нет валидных артикулов для обработки")
            return

        logger.info(f"Найдено {len(skus)} валидных артикулов")

        # Обработка данных
        process_skus(gc, skus, date_str)

        logger.info("=== Сбор данных завершен ===")

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)


if __name__ == "__main__":
    # Проверка окружения
    if not all([SHEET_URL, API_KEY]):
        logger.error("Необходимо установить GOOGLE_SHEETS_URL и MPSTATS_API_KEY в .env")
        exit(1)

    # Первый запуск
    daily_collect()

    # Настройка расписания
    schedule.every().day.at("08:00").do(daily_collect)
    logger.info("Скрипт запущен. Ожидание следующего запуска по расписанию...")

    # Основной цикл
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            logger.info("\nСкрипт остановлен пользователем")
            break
        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {str(e)}")
            time.sleep(60)