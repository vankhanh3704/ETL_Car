import pandas as pd
import os
from config.config import DATA_RAW_DIR, DATA_PROCESSED_DIR
import json


def transform_data():
    # Đọc dữ liệu thô
    input_file = os.path.join(DATA_RAW_DIR, "cars.json")
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"File {input_file} không tồn tại. Hãy chạy scrape_data_dynamic trước.")

    with open(input_file, "r", encoding="utf-8") as f:
        cars_data = json.load(f)

    df_cars = pd.DataFrame(cars_data)

    # Làm sạch dữ liệu
    # Xử lý giá
    df_cars["price"] = df_cars["price"].str.replace("VNĐ", "").str.replace(",", "").str.strip()
    df_cars["price"] = pd.to_numeric(df_cars["price"], errors="coerce").fillna(0).astype(int)

    # Chuẩn hóa fuel và gear
    df_cars["fuel"] = df_cars["fuel"].str.lower().replace({
        "dầu": "Diesel",
        "xăng": "Petrol",
        "điện": "Electric"
    }).fillna("Unknown")

    df_cars["gear"] = df_cars["gear"].str.lower().replace({
        "số sàn": "Manual",
        "tự động": "Automatic"
    }).fillna("Unknown")

    # Chuẩn hóa color
    df_cars["color"] = df_cars["color"].str.lower().replace({
        "trắng": "White",
        "đen": "Black",
        "đỏ": "Red"
    }).fillna("Unknown")

    # Trích xuất số chỗ ngồi
    df_cars["number_of_seats"] = df_cars["specifications"].str.extract(r"(\d+)\s*chỗ").astype(float).fillna(5).astype(
        int)

    # Thêm cột category (danh mục con) dựa trên carName hoặc model
    def categorize_car(row):
        name = row["carName"].lower()
        model = row["model"].lower()
        if "suv" in name or "suv" in model:
            return "SUV"
        elif "sedan" in name or "sedan" in model:
            return "Sedan"
        elif "hatchback" in name or "hatchback" in model:
            return "Hatchback"
        elif "pickup" in name or "pickup" in model:
            return "Pickup"
        else:
            return "Other"

    df_cars["category"] = df_cars.apply(categorize_car, axis=1)

    # Thêm các trường mặc định
    df_cars["status"] = "Available"
    df_cars["warrantyPeriod"] = 12
    df_cars["count"] = 1
    df_cars["manufactureYear"] = df_cars["note"].str.extract(r"(\d{4})").astype(float).fillna(2025).astype(int)
    df_cars["licensePlate"] = ""

    # Chọn các cột cần thiết (bỏ note nếu không cần)
    df_car = df_cars[[
        "carName", "brand", "model", "category", "manufactureYear", "licensePlate", "price", "count",
        "status", "color", "specifications", "imageUrl", "warrantyPeriod", "number_of_seats",
        "fuel", "gear"
    ]]

    # Lưu dữ liệu đã xử lý
    output_file = os.path.join(DATA_PROCESSED_DIR, "cars_cleaned.csv")
    df_car.to_csv(output_file, index=False, encoding="utf-8")
    print(f"Dữ liệu đã được xử lý và lưu tại {output_file}")

    return df_car


if __name__ == "__main__":
    transform_data()