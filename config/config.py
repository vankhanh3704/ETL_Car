import os
from dotenv import load_dotenv

# Load biến môi trường từ .env
load_dotenv()

# Cấu hình database
MYSQL_CONFIG = {
    "user": os.getenv("MYSQL_USER", "default_user"),
    "password": os.getenv("MYSQL_PASSWORD", "default_password"),
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": os.getenv("MYSQL_PORT", "3306"),
    "database": "CarManagement"
}

DWH_CONFIG = {
    "user": os.getenv("MYSQL_USER", "default_user"),
    "password": os.getenv("MYSQL_PASSWORD", "default_password"),
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": os.getenv("MYSQL_PORT", "3306"),
    "database": "CarDWH"
}

# URL cào dữ liệu
SCRAPE_URL = os.getenv("SCRAPE_URL", "https://api-ecom.carpla.vn/app-server/search/car")

# Đường dẫn lưu trữ dữ liệu
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

# Đảm bảo thư mục tồn tại
os.makedirs(DATA_RAW_DIR, exist_ok=True)
os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)