# Code mẫu cho các teammate

import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Hướng dẫn cho team:
# 1. Cài đặt các thư viện cần thiết: pip install pandas sqlalchemy psycopg2-binary python-dotenv
# 2. Tạo file .env và thêm dòng sau, dán chuỗi kết nối "chỉ đọc" vào:
#    DATABASE_URL_READER="postgresql://readonly_user:..."

def get_readonly_db_engine():
    """Tải chuỗi kết nối CHỈ ĐỌC và tạo engine."""
    load_dotenv()
    # Sử dụng một tên biến khác để tránh nhầm lẫn với key của pipeline
    db_url = os.getenv("DATABASE_URL_READER") 
    if not db_url:
        raise ValueError("Lỗi: Không tìm thấy DATABASE_URL_READER trong file .env.")
    return create_engine(db_url)

# --- VÍ DỤ SỬ DỤNG ---
try:
    print("Đang kết nối đến database với quyền chỉ đọc...")
    engine = get_readonly_db_engine()

    # Ví dụ 1: Đọc 100 dòng dữ liệu gần đây nhất của một trạm cụ thể
    location_id_to_query = 7441 # Thay đổi ID này
    query = f"""
    SELECT * 
    FROM public.air_quality_forecast_data
    WHERE location_id = {location_id_to_query}
    ORDER BY datetime DESC
    LIMIT 100;
    """
    
    print(f"\nĐang thực thi query để lấy 100 dòng cuối của trạm {location_id_to_query}...")
    df = pd.read_sql(query, engine)
    
    print("Query thành công!")
    print("5 dòng dữ liệu đầu tiên:")
    print(df.head())

    # Ví dụ 2: Thử ghi dữ liệu (SẼ THẤT BẠI - để chứng minh quyền chỉ đọc)
    print("\nThử thực hiện một hành động ghi (sẽ gây ra lỗi)...")
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM public.air_quality_forecast_data WHERE location_id = 7441;"))
    
except Exception as e:
    print(f"\nĐÃ XẢY RA LỖI: {e}")