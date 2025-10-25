import logging
import pandas as pd
import os
from datetime import datetime, timezone, timedelta
import requests
import openmeteo_requests
from retry_requests import retry
import time

# --- Logging setup (Giữ nguyên) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE_PATH = os.path.join(BASE_DIR, "etl_to_csv.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("etl_to_csv")


# --- Hằng số toàn cục (Đã sửa đổi) ---
METADATA_FILE_PATH = os.path.join(BASE_DIR, "../stations_metadata.csv")
# Tên file CSV sẽ được tạo ra hoặc cập nhật
OUTPUT_CSV_FILE = os.path.join(BASE_DIR, "hanoi_realtime_data_updated.csv")


# --- CÁC HÀM CHỨC NĂNG ---

# Hàm fetch_recent_data giữ nguyên 100% vì nó đã làm rất tốt
def fetch_recent_data(stations_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Gọi API Open-Meteo để lấy dữ liệu 7 ngày gần nhất.
    Trả về một DataFrame duy nhất chứa dữ liệu đã được gộp và xử lý timezone.
    """
    logger.info("Bắt đầu hàm fetch_recent_data...")
    
    retry_session = retry(requests.Session(), retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    all_station_dfs = []
    num_past_days = 5

    for index, station in stations_df.iterrows():
        loc_id = station['location_id']
        lat = station['lat']
        lon = station['lon']
        
        logger.info(f"  -> Đang xử lý vị trí trạm ID: {loc_id} cho {num_past_days} ngày qua...")
        
        df_weather = pd.DataFrame()
        df_aq = pd.DataFrame()

        try:
            # === 1. LỆNH GỌI API THỜI TIẾT ===
            weather_url = "https://api.open-meteo.com/v1/forecast"
            weather_params = {
                "latitude": lat, "longitude": lon,
                "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "wind_speed_10m", "wind_direction_10m", "pressure_msl", "boundary_layer_height"],
                "timezone": "Asia/Bangkok",
                "past_days": num_past_days,
                "forecast_days": 1
            }
            weather_response = openmeteo.weather_api(weather_url, params=weather_params)[0]
            hourly = weather_response.Hourly()
            df_weather = pd.DataFrame(data={"datetime": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True), end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()), inclusive="left"
            )})
            df_weather["datetime"] = df_weather["datetime"].dt.tz_convert("Asia/Bangkok")
            for i, var_name in enumerate(weather_params["hourly"]):
                df_weather[var_name] = hourly.Variables(i).ValuesAsNumpy()[:len(df_weather)]
            logger.info("   - Lấy dữ liệu thời tiết thành công.")
        except Exception as e:
            logger.warning(f"  - Cảnh báo: Lỗi khi lấy dữ liệu THỜI TIẾT cho trạm {loc_id}: {e}")

        try:
            # === 2. LỆNH GỌI API CHẤT LƯỢNG KHÔNG KHÍ ===
            aq_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
            aq_params = {
                "latitude": lat, "longitude": lon,
                "hourly": ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone"],
                "timezone": "Asia/Bangkok",
                "past_days": num_past_days,
                "forecast_days": 1
            }
            aq_response = openmeteo.weather_api(aq_url, params=aq_params)[0]
            hourly = aq_response.Hourly()
            df_aq = pd.DataFrame(data={"datetime": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True), end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()), inclusive="left"
            )})
            df_aq["datetime"] = df_aq["datetime"].dt.tz_convert("Asia/Bangkok")
            for i, var_name in enumerate(aq_params["hourly"]):
                df_aq[f"{var_name}_cams"] = hourly.Variables(i).ValuesAsNumpy()[:len(df_aq)]
            logger.info("     - Lấy dữ liệu chất lượng không khí thành công.")
        except Exception as e:
            logger.warning(f"     - Cảnh báo: Lỗi khi lấy dữ liệu CHẤT LƯỢNG KHÔNG KHÍ cho trạm {loc_id}: {e}")
            
        # === 3. MERGE DATAFRAME ===
        if not df_weather.empty or not df_aq.empty:
            if not df_weather.empty and not df_aq.empty:
                df_station_combined = pd.merge(df_weather, df_aq, on='datetime', how='outer')
            else:
                df_station_combined = df_weather if not df_weather.empty else df_aq
                
            df_station_combined['location_id'] = loc_id
            df_station_combined['lat'] = lat
            df_station_combined['lon'] = lon
            all_station_dfs.append(df_station_combined)
            logger.info(f"    -> Thành công. Đã xử lý trạm {loc_id} ({len(df_station_combined)} dòng).")
        else:
            logger.warning(f"    -> Thất bại: Không lấy được cả hai loại dữ liệu cho trạm {loc_id}.")

    if not all_station_dfs:
        logger.info("Không lấy được bất kỳ dữ liệu mới nào từ API.")
        return None

    final_df = pd.concat(all_station_dfs, ignore_index=True)
    vn_tz = timezone(timedelta(hours=7))
    vn_now = datetime.now(vn_tz)
    final_df = final_df[final_df["datetime"] <= vn_now].copy()
    logger.info(f"Hoàn tất fetch_recent_data. Tổng cộng {len(final_df)} dòng được lấy về.")
    return final_df

def append_to_csv(df_new: pd.DataFrame, csv_filepath: str) -> int:
    """
    Nối DataFrame mới vào file CSV đã có, xử lý trùng lặp và sắp xếp.
    Nếu file chưa tồn tại, tạo file mới.
    Trả về số lượng dòng mới thực sự được thêm vào.
    """
    if df_new is None or df_new.empty:
        logger.info("Không có dữ liệu mới để ghi vào CSV. Bỏ qua.")
        return 0

    # Kiểm tra sự tồn tại của file CSV
    if os.path.exists(csv_filepath):
        logger.info(f"File '{csv_filepath}' đã tồn tại. Đang đọc dữ liệu cũ...")
        df_old = pd.read_csv(csv_filepath)
        # Quan trọng: Chuyển đổi cột datetime để có thể gộp và so sánh đúng
        df_old['datetime'] = pd.to_datetime(df_old['datetime'])
        
        # Gộp dữ liệu cũ và mới
        combined_df = pd.concat([df_old, df_new], ignore_index=True)
        
        logger.info("Đang loại bỏ các dòng trùng lặp, giữ lại dữ liệu mới nhất...")
        # Loại bỏ các dòng trùng lặp dựa trên cặp (location_id, datetime)
        final_df = combined_df.drop_duplicates(subset=['location_id', 'datetime'], keep='last')
        
        rows_added = len(final_df) - len(df_old)
    else:
        logger.info(f"File '{csv_filepath}' chưa tồn tại. Sẽ tạo file mới.")
        final_df = df_new
        rows_added = len(final_df)

    # Sắp xếp lại dữ liệu để file luôn có trật tự
    logger.info("Đang sắp xếp lại dữ liệu...")
    final_df = final_df.sort_values(by=['location_id', 'datetime'])

    # Ghi lại toàn bộ dữ liệu đã được cập nhật
    logger.info(f"Đang ghi {len(final_df)} dòng vào '{csv_filepath}'...")
    final_df.to_csv(csv_filepath, index=False, encoding='utf-8-sig')
    
    return rows_added


# --- Hàm điều phối chính (Main orchestrator function) (Đã sửa đổi) ---
def run_etl_to_csv():
    """
    Hàm chính để điều phối quá trình ETL và lưu vào file CSV.
    """
    logger.info("==================================================")
    logger.info(f"BẮT ĐẦU ETL PIPELINE (LƯU RA CSV) LÚC: {datetime.now()}")
    logger.info("==================================================")
    start_time = time.time()
    
    total_rows_added = 0
    
    try: 
        # Bước A: Đọc metadata (Giữ nguyên)
        logger.info(f"\n [Bước 1/3] Đang đọc metadata từ '{METADATA_FILE_PATH}'...")
        if not os.path.exists(METADATA_FILE_PATH):
            raise FileNotFoundError(f"Lỗi: Không tìm thấy file metadata '{METADATA_FILE_PATH}'.")
        df_metadata = pd.read_csv(METADATA_FILE_PATH)
        logger.info(f" -> Đọc thành công thông tin của {len(df_metadata)} trạm.")
        
        # Bước B: Lấy dữ liệu mới (Giữ nguyên)
        logger.info("\n [Bước 2/3] Đang lấy dữ liệu gần đây từ Open-Meteo...")
        recent_data_df = fetch_recent_data(df_metadata)
        
        # Bước C: Tải dữ liệu vào file CSV (Đã thay đổi)
        logger.info(f"\n [Bước 3/3] Đang ghi/cập nhật dữ liệu vào file CSV...")
        rows_added = append_to_csv(recent_data_df, OUTPUT_CSV_FILE)
        if rows_added is not None:
            total_rows_added = rows_added
    
    except Exception as e:
        logger.exception("ETL JOB THẤT BẠI !!!")
        logger.warning(f"Lỗi: {e}")
    
    finally:
        end_time = time.time()
        logger.info("\n==================================================")
        logger.info(f"KẾT THÚC ETL JOB. TỔNG THỜI GIAN: {end_time - start_time:.2f} GIÂY.")
        # Log ra con số chính xác
        logger.info(f" -> Đã thêm thành công {total_rows_added} bản ghi mới vào '{OUTPUT_CSV_FILE}'.")
        logger.info("==================================================")
    
#--- Điểm bắt đầu thực thi của script ---
if __name__ == "__main__":
    run_etl_to_csv()