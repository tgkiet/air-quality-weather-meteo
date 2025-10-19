# Tự động cập nhật dữ liệu mới nhất từ Open-Meteo API và lưu trữ vào Supabase
# Lịch chạy là mỗi giờ

# --- 1. Import thư viện ---
import logging
import random
import uuid
import pandas as pd
import os
from datetime import datetime, timezone
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import openmeteo_requests
from retry_requests import retry
import time

# --- Logging setup ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE_PATH = os.path.join(BASE_DIR, "etl_realtime.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("etl_realtime")


# --- Hằng số toàn cục ---
METADATA_FILE_PATH = os.path.join(BASE_DIR, "../stations_metadata.csv") # Đường dẫn an toàn hơn
DB_TABLE_NAME = "air_quality_forecast_data"


# -- 3. Định nghĩa các hàm chức năng ---

def get_db_engine():
    """
    Hàm này đọc chuỗi kết nối từ .env và tạo một SQLAlchemy engine
    Nhiệm vụ duy nhất của function này là tạo  kết nối.
    """
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("Lỗi: Không tìm thấy DATABASE_URL trong file .env")
    logger.info(" Kết nối database được khởi tạo thành công.")
    # pool_pre_ping giúp phát hiện connection dead và reconnect tự động
    return create_engine(db_url, pool_pre_ping=True)

def retry_execute(conn, query, retries=3, delay_base=1.0):
    """
    Thực thi truy vấn sql với cơ chế retry nếu gặp deadlock
    """
    last_exception = None
    for attempt in range(retries):
        try:
            conn.execute(text(query))
            return
        except Exception as e:
            last_exception = e
            msg = str(e).lower()
            if any(err in msg for err in ["deadlock detected", "could not obtain lock", "serialization failure"]):
                wait = delay_base * (2 ** attempt) + random.random()
                logger.warning(f"  Phát hiện Deadlock/lock - retry sau {wait:.1f}s (lần {attempt + 1}/{retries})...")
                time.sleep(wait)
            else:
                logger.error(f"Lỗi SQL không thể retry: {e}")
                raise
    raise RuntimeError(f"Quá số lần retry do deadlock/lock. Lỗi cuối cùng: {last_exception}")

def fetch_recent_data(stations_df):
    """
    Gọi API Open-Meteo để lấy dữ liệu 3 ngày gần nhất.
    Thực hiện hai lệnh gọi API riêng biệt, cả hai đều dùng `past_days`.
    """
    logger.info("Bắt đầu hàm fetch_recent_data...")
    
    retry_session = retry(requests.Session(), retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    all_station_dfs = []
    num_past_days = 3

    for index, station in stations_df.iterrows():
        loc_id = station['location_id']
        lat = station['lat']
        lon = station['lon']
        
        logger.info(f"  -> Đang xử lý vị trí trạm ID: {loc_id} cho {num_past_days} ngày qua...")
        
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
            logger.info("   - Lấy dữ liệu thời tiết thành công.")

        except Exception as e:
            logger.warning(f"  - Cảnh báo: Lỗi khi lấy dữ liệu THỜI TIẾT cho trạm {loc_id}: {e}")
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
            logger.info("     - Lấy dữ liệu chất lượng không khí thành công.")

        except Exception as e:
            logger.warning(f"     - Cảnh báo: Lỗi khi lấy dữ liệu CHẤT LƯỢNG KHÔNG KHÍ cho trạm {loc_id}: {e}")
            # Nếu lỗi, chúng ta vẫn có thể có dữ liệu thời tiết
            
        # === 3. MERGE HAI DATAFRAME LẠI ===
        # Chỉ gộp nếu có ít nhất một trong hai DataFrame không rỗng
        if not df_weather.empty or not df_aq.empty:
            if not df_weather.empty and not df_aq.empty:
                df_station_combined = pd.merge(df_weather, df_aq, on='datetime', how='outer')
            elif not df_weather.empty:
                df_station_combined = df_weather
            else:
                df_station_combined = df_aq
                
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
    
    final_df = final_df[final_df['datetime'] <= datetime.now(timezone.utc)].copy()
    
    logger.info(f"Hoàn tất fetch_recent_data. Tổng cộng {len(final_df)} dòng được lấy về.")
    return final_df
        

def upsert_data(engine, df: pd.DataFrame, table_name: str, pipeline_id: str = None):
    """
    Ghi DataFrame vào PostgreSQL một cách nguyên tử (atomic), an toàn và hiệu quả,
    sử dụng một transaction duy nhất. Tương thích với Supabase.
    """
    
    if df is None or df.empty:
        logger.warning(" Không có dữ liệu để thực hiện UpSert. Bỏ qua.")
        return
    
    # Chuẩn bị tên bảng tạm duy nhất
    # Bao bọc bằng ngoặc kép để đảm bảo an toàn trong các câu lệnh SQL thô
    temp_table_name_quoted = f'"temp_{table_name}_{uuid.uuid4().hex[:8]}"'
    # pandas.to_sql cần tên không có ngoặc kép
    temp_table_name_unquoted = temp_table_name_quoted.strip('"')

    batch_id = pipeline_id or uuid.uuid4().hex[:6]
    logger.info(f" [Pipeline {batch_id}] bắt đầu upsert {len(df)} dòng vào bảng '{table_name}' ...")
    
    # Mở kết nối một lần duy nhất cho toàn bộ tác vụ
    with engine.connect() as conn:
        try:
            # --- BẮT ĐẦU MỘT TRANSACTION DUY NHẤT ---
            # Toàn bộ logic nghiệp vụ sẽ nằm trong khối này.
            # Nó sẽ tự động COMMIT khi kết thúc thành công, hoặc ROLLBACK nếu có lỗi.
            with conn.begin():
                
                # Bước A: Ghi dữ liệu vào bảng tạm
                logger.info(f"  A. Ghi dữ liệu vào bảng tạm '{temp_table_name_unquoted}'...")
                df.to_sql(
                    temp_table_name_unquoted,
                    conn, # Sử dụng connection của transaction hiện tại
                    if_exists="replace",
                    index=False,
                    method='multi',
                    chunksize=5000
                )
                logger.info("     -> Ghi vào bảng tạm thành công.")

                # Bước B: Thực thi logic Upsert từ bảng tạm
                logger.info("  B. Thực thi lệnh UPSERT...")
                
                # Lấy danh sách cột từ DataFrame để đảm bảo khớp 100%
                cols_quoted = ", ".join([f'"{c.lower()}"' for c in df.columns])
                
                # Upsert với RETURNING
                upsert_query = f"""
                INSERT INTO public."{table_name}" ({cols_quoted})
                SELECT {cols_quoted} FROM {temp_table_name_quoted}
                ON CONFLICT (location_id, datetime) DO NOTHING
                RETURNING 1;
                """
                result = conn.execute(text(upsert_query))
                rows_inserted = result.rowcount

                logger.info(f" -> Lệnh Upsert đã được thực thi thành công, thực sự insert {rows_inserted} dòng.")

                # Lưu ý: Bảng tạm (không phải là TEMP TABLE) được tạo trong transaction này
                # sẽ bị rollback và biến mất nếu transaction thất bại.
                # Nếu thành công, nó vẫn tồn tại cho đến khi bị dọn dẹp.
                
            # Transaction kết thúc, COMMIT đã được gọi tự động.
            logger.info("  ✅ Giao dịch Upsert hoàn tất và đã được COMMIT.")

        except Exception:
            # Log lỗi và thông báo về việc rollback tự động
            logger.error("\n❌ Lỗi trong quá trình Upsert. Transaction đã được tự động ROLLBACK.", exc_info=True)

        finally:
            # --- BƯỚC C: DỌN DẸP ---
            # Khối `finally` đảm bảo việc dọn dẹp luôn được thực thi,
            # dù transaction ở trên thành công hay thất bại.
            logger.info(f"  -> C. Dọn dẹp bảng tạm {temp_table_name_quoted}...")
            try:
                # Thực thi lệnh DROP TABLE trên cùng một connection
                # Không cần transaction riêng cho lệnh này trong SQLAlchemy 2.x
                conn.execute(text(f'DROP TABLE IF EXISTS {temp_table_name_quoted};'))
                conn.commit() # Cần commit tường minh cho lệnh chạy ngoài `with conn.begin()`
                logger.info("     -> Dọn dẹp bảng tạm thành công.")
            except Exception as cleanup_e:
                logger.warning(f"     -> Cảnh báo: Lỗi khi dọn dẹp bảng tạm: {cleanup_e}")
                            
        logger.info(f"🏁 [Pipeline {batch_id}] Hoàn tất upsert cho bảng '{table_name}'.\n")


# --- 4. Hàm điều phối chính (Main orchestrator function) --- 
def run_realtime_etl():
    """
    Hàm chính để điều phối quá trình ETL.
    """
    logger.info("==================================================")
    logger.info(f"BẮT ĐẦU ETL PIPELINE LÚC: {datetime.now()}")
    logger.info("==================================================")
    start_time = time.time()
    
    try: 
        # Bước A: Đọc metadata
        logger.info(f"\n [Bước 1/3] Đang đọc metadata từ '{METADATA_FILE_PATH}'...")
        if not os.path.exists(METADATA_FILE_PATH):
            raise FileNotFoundError(f"Lỗi: Không tìm thấy file metadata '{METADATA_FILE_PATH}'.")
        df_metadata = pd.read_csv(METADATA_FILE_PATH)
        logger.info(f" -> Đọc thành công thông tin của {len(df_metadata)} trạm.")
        
        # Bước B: Lấy dữ liệu mới (Extract & Transform)
        logger.info("\n [Bước 2/3] Đang lấy dữ liệu gần đây từ Open-Meteo...")
        recent_data_df = fetch_recent_data(df_metadata)
        
        # Bước C: Tải dữ liệu vào DB (Load)
        logger.info("\n [Bước 3/3] Đang tải dữ liệu lên database...")
        if recent_data_df is not None and not recent_data_df.empty:
            db_engine = get_db_engine()
            upsert_data(db_engine, recent_data_df, DB_TABLE_NAME)
        else:
            logger.info(" -> Không có dữ liệu mới để tải lên.")
    
    except Exception as e:
        logger.exception("ETL JOB THẤT BẠI !!!")
        logger.warning(f"Lỗi: {e}")
    
    finally:
        end_time = time.time()
        logger.info("\n==================================================")
        logger.info(f"KẾT THÚC ETL JOB. TỔNG THỜI GIAN: {end_time - start_time:.2f} GIÂY.")
        logger.info("==================================================")
    
#--- 5. Điểm bắt đầu thực thi của script ---
if __name__ == "__main__":
    run_realtime_etl()