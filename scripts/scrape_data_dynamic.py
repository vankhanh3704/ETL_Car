import requests
import json
import os
import time
import random
from config.config import DATA_RAW_DIR, SCRAPE_URL
import logging
from datetime import datetime

# Cấu hình logging
logging.basicConfig(level=logging.INFO, filename='scrape.log', format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = SCRAPE_URL
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://carpla.vn/",
    "Connection": "keep-alive"
}

def scrape_data_dynamic():
    session = requests.Session()
    cars = []

    for offset in range(1, 571, 12):
        params = {
            "status": 2,
            "offset": offset,
            "limit": 15,
            "saleState": 1,
            "type": 1
        }

        try:
            response = session.get(BASE_URL, headers=HEADERS, params=params, timeout=10)
            logging.info(f"API Status code: {response.status_code} at offset {offset}")

            if response.status_code != 200:
                logging.error(f"Lỗi kết nối: Status code {response.status_code} tại offset {offset}")
                continue

            data = response.json()
            if "data" not in data or not data["data"]:
                logging.info(f"Không còn dữ liệu tại offset {offset}")
                break

            for item in data["data"]:
                title = item.get("title", "")
                price = item.get("price", "")
                additional = item.get("additional", {})

                year = additional.get("year", "")
                brand = additional.get("brand", {}).get("name", "")
                model = additional.get("model", {}).get("name", "") or " ".join(title.split()[1:]) if title else ""
                fuel = additional.get("fuel", {}).get("type", "")
                gear = additional.get("gear", {}).get("name", "")
                color = additional.get("color", {}).get("color", "")
                specifications = additional.get("description", "")

                note_parts = [str(year)]
                if color:
                    note_parts.append(color)
                note = " - ".join(note_parts)

                car_data = {
                    "carName": title,
                    "brand": brand,
                    "model": model,
                    "price": str(price),
                    "fuel": fuel,
                    "gear": gear,
                    "color": color,
                    "imageUrl": item.get("image", ""),
                    "specifications": specifications,
                    "note": note
                }
                cars.append(car_data)

            logging.info(f"Đã xử lý offset {offset}")
            print(f"Đã xử lý offset {offset}")
            time.sleep(random.randint(3, 10))

        except Exception as e:
            logging.error(f"Lỗi tại offset {offset}: {e}")
            print(f"Lỗi tại offset {offset}: {e}")
            continue

    # Lưu dữ liệu với timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(DATA_RAW_DIR, f"cars_{timestamp}.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(cars, f, ensure_ascii=False, indent=4)

    logging.info(f"Đã cào {len(cars)} xe, lưu tại {output_file}")
    print(f"Đã cào {len(cars)} xe, lưu tại {output_file}")

    # Lưu file tổng hợp (ghi đè)
    total_file = os.path.join(DATA_RAW_DIR, "cars.json")
    with open(total_file, "w", encoding="utf-8") as f:
        json.dump(cars, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    scrape_data_dynamic()