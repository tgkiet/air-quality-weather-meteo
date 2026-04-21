import logging
import sys
import os

def get_logger(name: str) -> logging.Logger:
    """
    Tạo và cấu hình một logger dùng chung cho toàn bộ pipeline.
    Output sẽ được in ra console và ghi vào thư mục logs/ của project.
    """
    logger = logging.getLogger(name)
    
    # Chỉ cấu hình nếu logger chưa có handler nào (tránh duplicate logs)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Format log: [Thời gian] [Tên file/module] [Mức độ lỗi] - Nội dung
        formatter = logging.Formatter(
            '%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 1. Bắn log ra Console (Terminal)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 2. Ghi log ra file
        log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
        os.makedirs(log_dir, exist_ok=True)
        
        file_handler = logging.FileHandler(os.path.join(log_dir, 'pipeline.log'))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
