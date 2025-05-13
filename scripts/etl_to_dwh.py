import pandas as pd
from sqlalchemy import create_engine, text
import os
from config.config import MYSQL_CONFIG, DWH_CONFIG

def etl_to_dwh():
    # Kết nối với CarManagement (nguồn dữ liệu)
    source_engine = create_engine(
        f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
    )

    # Kết nối với DWH (đích)
    dwh_engine = create_engine(
        f"mysql+mysqlconnector://{DWH_CONFIG['user']}:{DWH_CONFIG['password']}@"
        f"{DWH_CONFIG['host']}:{DWH_CONFIG['port']}/{DWH_CONFIG['database']}"
    )

    # Kiểm tra xem bảng Dim_Cars có tồn tại không
    with source_engine.connect() as connection:
        query = text("SELECT 1 FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table")
        table_exists = connection.execute(
            query, {"schema": MYSQL_CONFIG['database'], "table": "Dim_Cars"}
        ).fetchone()

    if not table_exists:
        print("Bảng 'Dim_Cars' không tồn tại trong database CarManagement. Hãy chạy load_to_mysql trước.")
        return

    # Trích xuất dữ liệu
    df_car = pd.read_sql("SELECT * FROM Dim_Cars", source_engine)
    df_sales = pd.read_sql("SELECT * FROM Fact_Sales", source_engine)
    if df_car.empty or df_sales.empty:
        print("Dữ liệu rỗng. Hãy chạy load_to_mysql trước.")
        return

    # Tải dữ liệu vào DWH
    df_car.to_sql("Dim_Car", dwh_engine, if_exists="replace", index=False)
    df_sales.to_sql("Fact_Sales", dwh_engine, if_exists="replace", index=False)

    print("Dữ liệu đã được tải vào Data Warehouse")

if __name__ == "__main__":
    etl_to_dwh()