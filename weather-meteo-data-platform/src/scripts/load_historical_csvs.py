import os
import sys

# Đảm bảo root của project nằm trong python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.loaders.csv_loader import CSVLoader
from src.utils.logger import get_logger

logger = get_logger("LoadHistoricalScript")

def main():
    logger.info("=== BẮT ĐẦU TIẾN TRÌNH NẠP DỮ LIỆU LỊCH SỬ TỪ CSV ===")
    
    # Tự động phát hiện đường dẫn thích hợp (chạy trên Host hay chạy trong Docker)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    
    # Phương án 1: File nằm ngay trong src (khi mount vào Docker)
    merged_csv_docker = os.path.join(project_root, "src", "hanoi_aq_weather_MERGED.csv")
    realtime_csv_docker = os.path.join(project_root, "src", "hanoi_realtime_data_updated.csv")
    
    # Phương án 2: File nằm ngoài project root (khi chạy trên Host)
    merged_csv_host = "/home/kiet/gkinhere/air-quality-pipeline/air-quality-weather-meteo/Open-Meteo-Dataset/hanoi_aq_weather_MERGED.csv"
    realtime_csv_host = "/home/kiet/gkinhere/air-quality-pipeline/air-quality-weather-meteo/Open-Meteo-Dataset/hanoi_realtime_data_updated.csv"
    
    merged_csv = merged_csv_docker if os.path.exists(merged_csv_docker) else merged_csv_host
    realtime_csv = realtime_csv_docker if os.path.exists(realtime_csv_docker) else realtime_csv_host
    
    loader = CSVLoader()
    try:
        # Kết nối tới database
        loader.connect()
        
        # Khởi tạo bảng nếu chưa có
        loader.create_table_if_not_exists()
        
        # Nạp file 1: Hanoi AQ & Weather Merged (Historical)
        if os.path.exists(merged_csv):
            logger.info(f"Nạp dữ liệu lịch sử Hanoi Merged từ: {merged_csv}")
            rows_loaded = loader.load_csv(merged_csv)
            logger.info(f"-> Nạp thành công {rows_loaded} dòng từ Hanoi Merged.")
        else:
            logger.error(f"Không tìm thấy file: {merged_csv}")
            sys.exit(1)
            
        # Nạp file 2: Hanoi Realtime Data Updated
        if os.path.exists(realtime_csv):
            logger.info(f"Nạp dữ liệu Hanoi Realtime từ: {realtime_csv}")
            rows_loaded = loader.load_csv(realtime_csv)
            logger.info(f"-> Nạp thành công {rows_loaded} dòng từ Hanoi Realtime.")
        else:
            logger.warning(f"Không tìm thấy file: {realtime_csv}. Bỏ qua nạp file này.")
            
        logger.info("=== HOÀN THÀNH TIẾN TRÌNH NẠP DỮ LIỆU LỊCH SỬ TỪ CSV ===")
        
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng trong quá trình nạp dữ liệu: {e}")
        sys.exit(1)
    finally:
        # Luôn đóng connection để giải phóng tài nguyên hệ thống
        loader.close()

if __name__ == "__main__":
    main()
