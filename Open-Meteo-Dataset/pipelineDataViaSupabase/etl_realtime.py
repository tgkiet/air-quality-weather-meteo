# Tự động cập nhật dữ liệu mới nhất từ Open-Meteo API và lưu trữ vào Supabase
# Lịch chạy là mỗi giờ

# --- 1. Import thư viện ---
import pandas as pd
import os
from datetime import datetime, timezone
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import openmeteo_requests
from retry_requests import retry
import time

# --- 2. Định nghĩa các hằng số toàn cục ---
METADATA_FILE_PATH = "../stations_metadata.csv"
DB_TABLE_NAME = "air_quality_forecast_data"


# -- 3. Định nghĩa các hàm chức năng ---

def get_db_engine():
    """
    Hàm này đọc chuỗi kết nối từ .env và tạo một SQLAlchemy engine
    Nhiệm vụ duy nhất của function này là tạo kết nối.
    """
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Lỗi: Khôgn tìm thấy DATABASE_URL trong file .env")
    return create_engine(db_url)

def fetch_recent_data(stations_df):
    """
    Gọi API Open-Meteo để lấy dữ liệu 3 ngày gần nhất.
    Thực hiện hai lệnh gọi API riêng biệt, cả hai đều dùng `past_days`.
    """
    print("LOG: Bắt đầu hàm fetch_recent_data...")
    
    retry_session = retry(requests.Session(), retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    all_station_dfs = []
    num_past_days = 3

    for index, station in stations_df.iterrows():
        loc_id = station['location_id']
        lat = station['lat']
        lon = station['lon']
        
        print(f"  -> Đang xử lý vị trí trạm ID: {loc_id} cho {num_past_days} ngày qua...")
        
        df_weather = pd.DataFrame()
        df_aq = pd.DataFrame()

        try:
            # === 1. LỆNH GỌI API THỜI TIẾT (WEATHER FORECAST) ===
            weather_url = "https://api.open-meteo.com/v1/forecast"
            weather_params = {
                "latitude": lat, "longitude": lon,
                "hourly": [
                    "temperature_2m", "relative_humidity_2m", "precipitation", "rain", 
                    "wind_speed_10m", "wind_direction_10m", "pressure_msl", "boundary_layer_height"
                ],
                "past_days": num_past_days,
                "forecast_days": 1
            }
            weather_responses = openmeteo.weather_api(weather_url, params=weather_params)
            weather_response = weather_responses[0]

            hourly = weather_response.Hourly()
            # Dùng pd.date_range để đảm bảo chuỗi thời gian luôn chính xác
            df_weather = pd.DataFrame(data={"datetime": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )})
            for i, var_name in enumerate(weather_params["hourly"]):
                values = hourly.Variables(i).ValuesAsNumpy()
                df_weather[var_name] = values[:len(df_weather)]
            print("     - Lấy dữ liệu thời tiết thành công.")

        except Exception as e:
            print(f"     - Cảnh báo: Lỗi khi lấy dữ liệu THỜI TIẾT cho trạm {loc_id}: {e}")
            # Nếu lỗi, chúng ta vẫn tiếp tục để thử lấy dữ liệu chất lượng không khí

        try:
            # === 2. LỆNH GỌI API CHẤT LƯỢNG KHÔNG KHÍ (AIR QUALITY) ===
            aq_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
            aq_params = {
                "latitude": lat, "longitude": lon,
                "hourly": ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone"],
                "past_days": num_past_days,
                "forecast_days": 1
            }
            aq_responses = openmeteo.weather_api(aq_url, params=aq_params)
            aq_response = aq_responses[0]
            
            hourly = aq_response.Hourly()
            df_aq = pd.DataFrame(data={"datetime": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )})
            for i, var_name in enumerate(aq_params["hourly"]):
                values = hourly.Variables(i).ValuesAsNumpy()
                df_aq[f"{var_name}_cams"] = values[:len(df_aq)]
            print("     - Lấy dữ liệu chất lượng không khí thành công.")

        except Exception as e:
            print(f"     - Cảnh báo: Lỗi khi lấy dữ liệu CHẤT LƯỢNG KHÔNG KHÍ cho trạm {loc_id}: {e}")
            # Nếu lỗi, chúng ta vẫn có thể có dữ liệu thời tiết

        # === 3. GỘP (MERGE) HAI DATAFRAME LẠI ===
        # Chỉ gộp nếu có ít nhất một trong hai DataFrame không rỗng
        if not df_weather.empty or not df_aq.empty:
            if not df_weather.empty and not df_aq.empty:
                df_station_combined = pd.merge(df_weather, df_aq, on='datetime', how='outer')
            elif not df_weather.empty:
                df_station_combined = df_weather
            else:
                df_station_combined = df_aq
                
            df_station_combined['location_id'] = loc_id
            all_station_dfs.append(df_station_combined)
            print(f"    -> Thành công. Đã xử lý trạm {loc_id}.")
        else:
            print(f"    -> Thất bại: Không lấy được cả hai loại dữ liệu cho trạm {loc_id}.")


    if not all_station_dfs:
        print("LOG: Không lấy được bất kỳ dữ liệu mới nào từ API.")
        return None

    final_df = pd.concat(all_station_dfs, ignore_index=True)
    
    final_df = final_df[final_df['datetime'] <= datetime.now(timezone.utc)].copy()
    
    print(f"LOG: Hoàn tất fetch_recent_data. Tổng cộng {len(final_df)} dòng được lấy về.")
    return final_df
        
def upsert_data(engine, df, table_name):
    """
    Hàm này nhận vào một DataFrame và chèn hoặc cập nhật nó vào bảng trong cơ sở dữ liệu.
    Nhiệm vụ duy nhất: Ghi DataFrame vào database với logic "Update and/or Insert".
    """
    print("LOG: Bắt đầu hàm upsert_data...")
    
    pass

# --- 4. Hàm điều phối chính (Main orchestrator function) --- 
def run_realtime_etl():
    """
    Hàm chính để điều phối quá trình ETL.
    """
    print("==================================================")
    print(f"BẮT ĐẦU ETL PIPELINE LÚC: {datetime.now()}")
    print("==================================================")
    start_time = time.time()
    
    try: 
        # Bước A: Đọc metadata
        print(f"\n [Bước 1/3] Đang đọc metadata từ '{METADATA_FILE_PATH}'...")
        if not os.path.exists(METADATA_FILE_PATH):
            raise FileNotFoundError(f"Lỗi: Không tìm thấy file metadata '{METADATA_FILE_PATH}'.")
        df_metadata = pd.read_csv(METADATA_FILE_PATH)
        print(f" -> Đọc thành công thông tin của {len(df_metadata)} trạm.")
        
        # Bước B: Lấy dữ liệu mới (Extract & Transform)
        print("\n [Bước 2/3] Đang lấy dữ liệu gần đây từ Open-Meteo...")
        recent_data_df = fetch_recent_data(df_metadata)
        
        # Bước C: Tải dữ liệu vào DB (Load)
        print("\n [Bước 3/3] Đang tải dữ liệu lên database...")
        if recent_data_df is not None and not recent_data_df.empty:
            db_engine = get_db_engine()
            upsert_data(db_engine, recent_data_df, DB_TABLE_NAME)
        else:
            print(" -> Không có dữ liệu mới để tải lên.")
    
    except Exception as e:
        print("\n ETL JOB THẤT BẠI !!!")
        print(f"Lỗi: {e}")
    
    finally:
        end_time = time.time()
        print("\n==================================================")
        print(f"KẾT THÚC ETL JOB. TỔNG THỜI GIAN: {end_time - start_time:.2f} GIÂY.")
        print("==================================================")
    
#--- 5. Điểm bắt đầu thực thi của script ---
if __name__ == "__main__":
    run_realtime_etl()