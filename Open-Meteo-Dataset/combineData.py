"""
MERGE AIR QUALITY + WEATHER + STATION COORDINATES
Phiên bản đã thêm kiểm tra timezone, duplicate, dtype
"""
import pandas as pd
import os
import sys
from datetime import datetime

# CONFIGURATION
CONFIG = {
    'air_quality_file': 'hanoi_air_quality_from04Aug_CAMS_COMBINED.csv',
    'weather_file': 'hanoi_weathermeteo_from04Aug_COMBINED.csv',
    'metadata_file': 'stations_metadata.csv',
    'output_file': 'hanoi_aq_weather_MERGED.csv',
    'merge_type': 'outer',  # 'outer' để giữ tất cả dữ liệu, 'inner' để chỉ giữ khớp
    'expected_columns': {
        'datetime': ['datetime', 'time'],
        'location_id': ['location_id', 'station_id', 'id'],
        'lat': ['lat', 'latitude'],
        'lon': ['lon', 'longitude', 'long']
    }
}

FINAL_COLUMN_ORDER = [
    'datetime',
    'pm10_cams', 'pm2_5_cams', 'carbon_monoxide_cams',
    'nitrogen_dioxide_cams', 'sulphur_dioxide_cams', 'ozone_cams',
    'location_id',
    'temperature_2m', 'relative_humidity_2m', 'precipitation', 'rain',
    'wind_speed_10m', 'wind_direction_10m', 'pressure_msl', 'boundary_layer_height',
    'lat', 'lon'
]

# UTILITY FUNCTIONS

def print_header(text):
    print("\n" + "="*60)
    print(f"  {text}")
    print("="*60)

def print_step(step_num, text):
    print(f"\n[Bước {step_num}] {text}")

def print_info(text, indent=1):
    prefix = "  " * indent + "→ "
    print(f"{prefix}{text}")

def check_file_exists(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f" LỖI: Không tìm thấy file '{filepath}'\n"
            f"   Vui lòng đảm bảo file tồn tại trong thư mục: {os.getcwd()}"
        )
    file_size = os.path.getsize(filepath) / (1024 * 1024)
    print_info(f" File '{filepath}' tồn tại ({file_size:.2f} MB)")
    return True

def find_column(df, possible_names):
    df_cols_lower = {col.lower(): col for col in df.columns}
    for name in possible_names:
        if name.lower() in df_cols_lower:
            return df_cols_lower[name.lower()]
    return None

def standardize_columns(df, df_name):
    print_info(f"Chuẩn hóa cột cho {df_name}...")
    renamed = {}
    for standard_name, possible_names in CONFIG['expected_columns'].items():
        actual_col = find_column(df, possible_names)
        if actual_col and actual_col != standard_name:
            renamed[actual_col] = standard_name
    if renamed:
        df = df.rename(columns=renamed)
        print_info(f"Đã đổi tên: {renamed}", indent=2)
    else:
        print_info("Không cần đổi tên cột", indent=2)
    return df

def validate_dataframe(df, df_name, required_cols):
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f" LỖI: File {df_name} thiếu các cột: {missing_cols}\n"
            f"   Các cột hiện có: {df.columns.tolist()}"
        )
    print_info(f" {df_name}: Đủ các cột cần thiết")

def parse_datetime_column(df, col_name='datetime'):
    """Parse cột datetime với timezone kiểm soát"""
    try:
        df[col_name] = pd.to_datetime(df[col_name], utc=True)
        print_info("Parse datetime với UTC timezone")
    except:
        try:
            df[col_name] = pd.to_datetime(df[col_name])
            print_info("Parse datetime không timezone (tz-naive)")
        except Exception as e:
            raise ValueError(f" Không thể parse cột '{col_name}': {e}")
    return df

def check_data_quality(df, df_name):
    print_info(f"Kiểm tra chất lượng dữ liệu {df_name}:")
    print_info(f"Số dòng: {len(df):,}", indent=2)
    n_duplicates = df.duplicated(subset=['location_id', 'datetime']).sum()
    if n_duplicates > 0:
        print_info(f"⚠️  Có {n_duplicates} dòng trùng (location_id + datetime)", indent=2)
    else:
        print_info(" Không có dòng trùng", indent=2)
    missing_pct = (df.isnull().sum() / len(df) * 100).round(2)
    cols_with_missing = missing_pct[missing_pct > 0]
    if len(cols_with_missing) > 0:
        print_info("Cột có giá trị thiếu:", indent=2)
        for col, pct in cols_with_missing.items():
            print_info(f"  • {col}: {pct}%", indent=3)
    else:
        print_info(" Không có giá trị thiếu", indent=2)
    if 'datetime' in df.columns:
        date_range = f"{df['datetime'].min()} → {df['datetime'].max()}"
        print_info(f"Khoảng thời gian: {date_range}", indent=2)

def merge_datasets(df1, df2, merge_cols, how='outer', names=('DF1', 'DF2')):
    print_info(f"Merge {names[0]} + {names[1]} trên: {merge_cols}")
    print_info(f"Phương thức: '{how}'", indent=2)
    before_1, before_2 = len(df1), len(df2)
    df_merged = pd.merge(df1, df2, on=merge_cols, how=how, suffixes=('', '_duplicate'))
    after = len(df_merged)
    print_info(f"{names[0]}={before_1:,}, {names[1]}={before_2:,} → Sau merge: {after:,}", indent=2)
    # FIX: xử lý các cột duplicate
    dup_cols = [c for c in df_merged.columns if c.endswith('_duplicate')]
    if dup_cols:
        print_info(f"⚠️ Có {len(dup_cols)} cột trùng tên: {dup_cols}", indent=2)
        df_merged = df_merged.drop(columns=dup_cols)
        print_info("→ Đã drop cột duplicate", indent=3)
    return df_merged

# MAIN PROCESSING PIPELINE

def main():
    start_time = datetime.now()
    print_header("BẮT ĐẦU QUÁ TRÌNH MERGE DỮ LIỆU")
    print(f"Thời gian: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Thư mục: {os.getcwd()}")

    try:
        # 1. Kiểm tra file
        print_step(1, "Kiểm tra file tồn tại")
        check_file_exists(CONFIG['air_quality_file'])
        check_file_exists(CONFIG['weather_file'])
        check_file_exists(CONFIG['metadata_file'])

        # 2. Đọc dữ liệu
        print_step(2, "Đọc dữ liệu CSV")
        df_air = pd.read_csv(CONFIG['air_quality_file'])
        df_weather = pd.read_csv(CONFIG['weather_file'])
        df_stations = pd.read_csv(CONFIG['metadata_file'])
        print_info(f"Air: {len(df_air):,} dòng | Weather: {len(df_weather):,} dòng | Stations: {len(df_stations)} trạm")

        # 3. Chuẩn hóa tên cột
        print_step(3, "Chuẩn hóa tên cột")
        df_air = standardize_columns(df_air, "Air Quality")
        df_weather = standardize_columns(df_weather, "Weather")
        df_stations = standardize_columns(df_stations, "Stations")

        # 4 Validate
        print_step(4, "Validate dữ liệu")
        validate_dataframe(df_air, "Air Quality", ['datetime', 'location_id'])
        validate_dataframe(df_weather, "Weather", ['datetime', 'location_id'])
        validate_dataframe(df_stations, "Stations", ['location_id', 'lat', 'lon'])

        # 5 Parse datetime
        print_step(5, "Xử lý cột datetime")
        df_air = parse_datetime_column(df_air)
        df_weather = parse_datetime_column(df_weather)

        # 6. Kiểm tra chất lượng
        print_step(6, "Kiểm tra chất lượng dữ liệu")
        check_data_quality(df_air, "Air Quality")
        check_data_quality(df_weather, "Weather")

        # 7. Merge Air + Weather
        print_step(7, "Merge Air Quality + Weather")
        df_merged = merge_datasets(df_air, df_weather, ['location_id', 'datetime'], CONFIG['merge_type'], ('Air', 'Weather'))

        # 8 Thêm tọa độ
        print_step(8, "Thêm tọa độ trạm từ metadata")
        df_final = merge_datasets(df_merged, df_stations[['location_id', 'lat', 'lon']], ['location_id'], 'left', ('Merged', 'Stations'))

        # FIX: Kiểm tra timezone sau merge
        if df_final['datetime'].dtype.tz is None:
            print_info("⚠️ Cảnh báo: datetime không có timezone (tz-naive)", indent=2)

        missing_coords = df_final[df_final['lat'].isnull()]['location_id'].unique()
        if len(missing_coords) > 0:
            print_info(f"⚠️ Thiếu tọa độ ở {len(missing_coords)} trạm: {missing_coords}", indent=2)
        else:
            print_info("✓ Tất cả trạm có tọa độ", indent=2)

        # 9. Chọn và sắp xếp cột
        print_step(9, "Sắp xếp & chọn cột cuối cùng")
        available_cols = [c for c in FINAL_COLUMN_ORDER if c in df_final.columns]
        df_final = df_final[available_cols].sort_values(['location_id', 'datetime']).reset_index(drop=True)
        print_info(f" Giữ {len(available_cols)} cột hợp lệ", indent=2)

        # FIX: Cast numeric
        numeric_cols = df_final.select_dtypes(include='number').columns
        df_final[numeric_cols] = df_final[numeric_cols].astype('float64')

        # 10. Kiểm tra kết quả
        print_step(10, "Kiểm tra kết quả cuối cùng")
        check_data_quality(df_final, "Final Dataset")

        # 11. Lưu file
        print_step(11, "Lưu file CSV")
        
        # --- THÊM DÒNG NÀY ĐỂ ĐẢM BẢO 100% LÀ GIỜ VIỆT NAM KHI HIỂN THỊ ---
        print_info("Chuyển đổi datetime sang múi giờ Asia/Bangkok để lưu file", indent=2)
        df_final['datetime'] = df_final['datetime'].dt.tz_convert('Asia/Bangkok')

        df_final.to_csv(CONFIG['output_file'], index=False, encoding='utf-8-sig')
        print_info(f" Lưu thành công → {CONFIG['output_file']} ({os.path.getsize(CONFIG['output_file'])/1024/1024:.2f} MB)", indent=2)

        # Tổng kết
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print_header("✅ HOÀN TẤT THÀNH CÔNG")
        print(f"""
                THỐNG KÊ:
              • Tổng dòng: {len(df_final):,}
              • Cột: {len(df_final.columns)}
              • Trạm: {df_final['location_id'].nunique()}
              • Khoảng thời gian: {df_final['datetime'].min()} → {df_final['datetime'].max()}
              • Thời gian xử lý: {duration:.2f} giây
              """)
        return df_final

    except Exception as e:
        print("\n" + "="*60)
        print("❌ LỖI XẢY RA")
        print("="*60)
        print(str(e))
        import traceback; traceback.print_exc()
        sys.exit(1)

# RUN
if __name__ == "__main__":
    df_result = main()
