# pip install pandas sqlalchemy psycopg2-binary python-dotenv

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time

def fetch_data_for_training():
    """
    Kéo dữ liệu từ Supabase sử dụng Connection String trực tiếp.
    Đây là cách hiệu quả cho các tác vụ Data Science.
    """
    print("--- DEMO TEAM MODEL: KẾT NỐI TRỰC TIẾP ---")
    
    # 1. Tải thông tin kết nối từ file .env một cách an toàn
    load_dotenv()
    db_string = os.getenv("DB_CONNECTION_STRING")
    if not db_string:
        raise ValueError("DB_CONNECTION_STRING không được tìm thấy trong file .env")

    try:
        # 2. Tạo một "engine" kết nối đến database
        engine = create_engine(db_string)

        # 3. Viết một câu lệnh SQL để lấy dữ liệu cần thiết
        # Đây là sức mạnh của kết nối trực tiếp: toàn quyền sử dụng SQL
        query = text("""
            SELECT 
                datetime, 
                location_id, 
                pm2_5_cams, 
                temperature_2m, 
                relative_humidity_2m,
                wind_speed_10m
            FROM 
                public.air_quality_forecast_data
            WHERE 
                location_id = :loc_id AND pm2_5_cams IS NOT NULL
            ORDER BY 
                datetime DESC
            LIMIT 10;
        """)

        print("Đang thực thi query...")
        start_time = time.time()
        
        # 4. Thực thi query và tải kết quả vào một DataFrame của Pandas
        with engine.connect() as connection:
            df = pd.read_sql(query, connection, params={'loc_id': 2539})
            
        duration = time.time() - start_time
        print(f"Query hoàn tất trong {duration:.4f} giây.")

        # 5. Hiển thị kết quả
        print("\nKết quả (10 dòng dữ liệu gần nhất của trạm 2539):")
        print(df)
        print("\nKiểu dữ liệu của cột datetime:")
        print(df.info()) # Cột datetime sẽ có múi giờ đúng

    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")

if __name__ == "__main__":
    fetch_data_for_training()