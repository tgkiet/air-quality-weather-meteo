# pip install supabase python-dotenv

import os
from supabase import create_client, Client
from dotenv import load_dotenv
import time
from datetime import datetime, timezone, timedelta

def fetch_and_process_data_for_api():
    """
    Lấy dữ liệu từ Supabase thông qua API chính thức.
    Đây là cách làm chuẩn và an toàn cho các ứng dụng (web/mobile).
    """
    print("--- DEMO TEAM BACKEND: SỬ DỤNG API KEY ---")
    
    # 1. Tải thông tin API từ file .env một cách an toàn
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL hoặc SUPABASE_ANON_KEY không được tìm thấy trong file .env")

    try:
        # 2. Tạo một client để tương tác với API của Supabase
        supabase: Client = create_client(url, key)

        print("Đang gọi API...")
        start_time = time.time()
        
        # 3. Sử dụng các hàm của thư viện để xây dựng query
        # Nó sẽ được dịch thành một request API an toàn đến Supabase
        response = supabase.from_('air_quality_forecast_data')\
                           .select('datetime, location_id, pm2_5_cams, temperature_2m, relative_humidity_2m, wind_speed_10m')\
                           .eq('location_id', 2539)\
                           .not_.is_('pm2_5_cams', 'null')\
                           .order('datetime', desc=True)\
                           .limit(10)\
                           .execute()
                           
        duration = time.time() - start_time
        print(f"API call hoàn tất trong {duration:.4f} giây.")

        # 4. Dữ liệu trả về nằm trong `response.data`
        # Đây là một list các dictionary, một định dạng chuẩn cho API (JSON)
        raw_data = response.data

        # === CODE CHUYỂN ĐỔI GIỜ TỪ UTC SANG GIỜ VIỆT NAM (BƯỚC 2) ===
        print("\nĐang xử lý dữ liệu: Chuyển đổi múi giờ từ UTC sang giờ Việt Nam...")
        
        processed_data = []
        vn_timezone = timezone(timedelta(hours=7)) # Định nghĩa múi giờ Việt Nam (UTC+7)

        for row in raw_data:
            # Lấy chuỗi thời gian UTC từ dữ liệu thô
            utc_time_str = row['datetime']
            
            # Parse chuỗi thành một đối tượng datetime (có nhận biết múi giờ UTC)
            utc_time_obj = datetime.fromisoformat(utc_time_str)
            
            # Chuyển đổi đối tượng datetime đó sang múi giờ Việt Nam
            vietnam_time_obj = utc_time_obj.astimezone(vn_timezone)
            
            # Cập nhật lại giá trị 'datetime' trong dictionary bằng đối tượng đã chuyển đổi
            # Hoặc bạn có thể định dạng nó thành chuỗi nếu muốn
            row['datetime'] = vietnam_time_obj.strftime('%Y-%m-%d %H:%M:%S') # Giờ Việt Nam dạng chuỗi
            
            processed_data.append(row)

        print("Xử lý dữ liệu hoàn tất.")
        # =================================================================
        

        # 3. HIỂN THỊ KẾT QUẢ ĐÃ QUA XỬ LÝ (Bước này thay đổi để dùng dữ liệu mới)
        print("\nKết quả đã xử lý (hiển thị giờ Việt Nam):")
        for final_row in processed_data:
            print(final_row)

    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")

if __name__ == "__main__":
    fetch_and_process_data_for_api()