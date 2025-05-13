import pandas as pd
from sqlalchemy import create_engine
import os
from config.config import MYSQL_CONFIG, DATA_PROCESSED_DIR


def load_to_mysql():
    # Đọc dữ liệu đã xử lý
    input_file = os.path.join(DATA_PROCESSED_DIR, "cars_cleaned.csv")
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"File {input_file} không tồn tại. Hãy chạy transform_data trước.")

    df_car = pd.read_csv(input_file)

    # Tạo bảng Fact_Sales giả định
    if df_car.empty:
        print("Dữ liệu rỗng, không tạo Fact_Sales.")
        return
    df_sales = pd.DataFrame({
        "carId": range(1, len(df_car) + 1),
        "quantity_sold": df_car["count"],
        "revenue": df_car["price"] * df_car["count"],
        "date": "2025-05-13"
    })

    # Tạo bảng Dim_Cars
    df_car["carId"] = range(1, len(df_car) + 1)

    # Kết nối MySQL
    engine = create_engine(
        f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
    )

    # Tải dữ liệu vào bảng Dim_Cars
    df_car.to_sql("Dim_Cars", engine, if_exists="replace", index=False)

    # Tải dữ liệu vào bảng Fact_Sales
    df_sales.to_sql("Fact_Sales", engine, if_exists="replace", index=False)

    print("Dữ liệu đã được tải vào MySQL")


if __name__ == "__main__":
    load_to_mysql()