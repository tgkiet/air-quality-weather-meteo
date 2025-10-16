# Mục đích: Nạp dữ liệu lịch sử từ file CSV (data air quality và data weather & meteo) vào database Supabase.
# CHẠY MỘT LẦN DUY NHẤT (VÌ CHỈ LÀ MỤC ĐÍCH BACKFILL DỮ LIỆU LỊCH SỬ)

import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import time

def run_backfill(): 
    """
    Hàm chính thực hiện toàn bộ quá trình backfill history data
    Tất cả logic backfill nằm trong hàm này
    """
    # bắt đầu bấm giờ để đo thời gian backfill mất bao lâu
    start_time = time.time()
    print("=============================================")
    print(" BẮT ĐẦU QUÁ TRÌNH BACKFILL DỮ LIỆU LỊCH SỬ ")
    print("=============================================")    
    
    # GIAI ĐOẠN 1. Load biến môi trường từ file .env
    print("\n [Buớc 1/4]: Đang load biến môi trường từ file .env...")
    load_dotenv() # Load biến môi trường từ file .env
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        # Dừng chương trình ngay lập tức nếu không tìm thấy chuỗi kết nối
        raise ValueError("Lỗi: Không tìm thấy DATABASE_URL trong file .env. Vui lòng kiểm tra lại.")
    print(" -> Tải cấu hình thành công.")
    
    # GIAI ĐOẠN 2. Extract & Transform: Đọc và chuẩn bị dữ liệu từ file CSV
    print("\n [Buớc 2/4]: Đang đọc và chuẩn bị dữ liệu từ file CSV...")
    
    historical_csv_file = "openmeteoDatasetCombined.csv"
    
    if not os.path.isfile(historical_csv_file):
        raise FileNotFoundError(f"Lỗi: Không tìm thấy file {historical_csv_file}. Vui lòng kiểm tra lại.")
    
    df_historical = pd.read_csv(historical_csv_file)
    print(f" -> Đã đọc thành công {len(df_historical)} dòng dữ liệu từ file CSV.")
    
    # Biến đổi (Transform) nhỏ: chuẩn hóa cột datetime
    # utc=True rất quan trọng để khớp với kiểu TIMESTAMPTZ của PostgreSQL
    df_historical['datetime'] = pd.to_datetime(df_historical['datetime'], utc=True)
    print(" -> Đã chuẩn hóa cột 'datetime'.")
    
    # GIAI ĐOẠN 3: Load dữ liệu vào database Supabase
    print("\n [Buớc 3/4]: Đang kết nối và nạp dữ liệu vào database Supabase...")
    try:
        engine = create_engine(db_url) # Tạo engine kết nối database
        
        table_name = "air_quality_forecast_data" # ten phai khop voi ten table trong database supabase
        
        print(f" -> Kết nối thành công. Bắt đầu tải dữ liệu lên bảng '{table_name}' .....")
        print("vui lòng kiên nhẫn chờ đợi, quá trình này có thể mất vài phút")        
        
        # Sử dụng phương thức to_sql của pandas để nạp dữ liệu vào bảng
        df_historical.to_sql(
            table_name, 
            engine, 
            if_exists='append', # Nạp thêm dữ liệu vào bảng nếu bảng đã tồn tại
            index=False, # Không nạp cột index của DataFrame vào bảng
            chunksize = 10000, # Gửi dữ liệu theo từng khối 10,000 dòng để tránh quá tải bộ nhớ
            method='multi' # Gửi nhiều dòng trong một lệnh INSERT để tăng tốc độ
        )
        
        print(" -> Tải dữ liệu lên database supabase thành công.")
        
    except Exception as e:
        print(f"\n ĐÃ XẢY RA LỖI TRONG QUÁ TRÌNH GHI VÀO DATABASE !!!")
        print(f" Chi tiết lỗi: {e}")
        # Dừng chương trình ngay lập tức nếu có lỗi
        return # thoát khỏi hàm run_backfill
    
    # GIAI ĐOẠN 4: Hoàn tất
    end_time = time.time() # Kết thúc bấm giờ
    duration = end_time - start_time
    
    print ("\n [Buớc 4/4]: Hoàn tất quá trình backfill.")
    print("\n=============================================")
    print("      BACKFILL THÀNH CÔNG!                  ")
    print("=============================================")
    print(f" -> Tổng thời gian thực hiện: {duration:.2f} giây.")

# Điểm bắt đầu thực thi của script này
# Cấu trúc `if __name__ == "__main__":` là một quy ước trong Python.
# Nó có nghĩa là: "Chỉ khi nào file này được chạy trực tiếp (bằng lệnh `python backfill_database.py`), thì mới thực thi đoạn code bên trong."
# Điều này ngăn không cho code tự chạy nếu nó được import bởi một file khác.

if __name__ == "__main__":
    run_backfill() # Goi hàm chính để thực thi backfill
    print("Hoàn tất quá trình backfill dữ liệu lịch sử vào database Supabase.")