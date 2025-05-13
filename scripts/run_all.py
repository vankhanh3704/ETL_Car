from scripts.scrape_data_dynamic import scrape_data_dynamic
from scripts.transform_data import transform_data
from scripts.load_to_mysql import load_to_mysql
from scripts.etl_to_dwh import etl_to_dwh

if __name__ == "__main__":
    print("Starting ETL process...")
    scrape_data_dynamic()
    transform_data()
    load_to_mysql()
    etl_to_dwh()
    print("ETL process completed!")