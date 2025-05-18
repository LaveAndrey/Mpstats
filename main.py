import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from datetime import datetime, timedelta
import time
import schedule
from dotenv import load_dotenv

load_dotenv()

CREDS_FILE = 'credentials.json'
SHEET_URL = os.getenv('GOOGLE_SHEETS_URL')
API_KEY = os.getenv('MPSTATS_API_KEY')
BASE_URL = 'https://mpstats.io/api/wb/get/item'


def connect_google():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    return gspread.authorize(creds)


def fetch_item_data(sku, date_str):
    try:
        url = f"{BASE_URL}/{sku}/balance_by_day"
        response = requests.get(url, headers={"X-Mpstats-TOKEN": API_KEY}, params={"d": date_str}, timeout=10)

        if response.status_code != 200:
            print(f"Ошибка запроса по артикулу {sku}: HTTP {response.status_code}")
            return None

        data = response.json()
        if not data:
            print(f"Нет данных на дату {date_str} для артикула {sku}")
            return None

        item = data[0]
        return {
            "price": item.get("price"),
            "sales": item.get("sales"),
            "final_price": item.get("final_price")
        }

    except Exception as e:
        print(f"Ошибка получения данных для {sku}: {e}")
        return None


def update_sheet(ws, sku, item_data, date_str):
    try:
        values = [
            date_str,
            sku,
            item_data.get('price', ''),
            item_data.get('final_price', ''),
            item_data.get('sales', '')
        ]
        ws.append_row(values)
        print(f"Добавлено: Артикул {sku} за дату {date_str}")
    except Exception as e:
        print(f"Ошибка обновления таблицы: {e}")


def daily_collect():
    date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"\nЗапуск сбора данных за {date_str} в {datetime.now()}")

    try:
        gc = connect_google()
        sheet = gc.open_by_url(SHEET_URL)
        sku_ws = sheet.worksheet("Артикулы")
        data_ws = sheet.worksheet("Данные")

        skus = [s.strip() for s in sku_ws.col_values(1)[1:] if s.strip().isdigit()]

        for sku in skus:
            print(f"Обработка артикула {sku}...")
            item_data = fetch_item_data(sku, date_str)
            if item_data:
                update_sheet(data_ws, sku, item_data, date_str)
            time.sleep(15)

    except Exception as e:
        print(f"Критическая ошибка: {e}")


if __name__ == "__main__":
    # Одноразовый запуск при старте
    daily_collect()

    # Запуск по расписанию каждый день в 23:30
    schedule.every().day.at("23:30").do(daily_collect)

    print("Скрипт запущен. Ожидание расписания...")

    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            print("Скрипт остановлен пользователем")
            break
        except Exception as e:
            print(f"Ошибка в основном цикле: {e}")
            time.sleep(60)
