import logging
import sys
import os


def get_logger(name: str) -> logging.Logger:
    """
    Tạo và cấu hình một logger dùng chung cho toàn bộ pipeline.

    Chiến lược logging:
    - Trong Docker/Airflow: Chỉ log ra stdout. Airflow tự bắt stdout và
      lưu vào Task Log của nó. Không cần tự ghi file trong Container.
    - Khi chạy Local (ngoài Docker): Ghi thêm vào file logs/pipeline.log
      để tiện debug.
    """
    logger = logging.getLogger(name)

    # Chỉ cấu hình nếu logger chưa có handler nào (tránh duplicate logs)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Format log: [Thời gian] [Tên module] [Mức độ] - Nội dung
        formatter = logging.Formatter(
            '%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Handler 1: Luôn bắn log ra stdout (Airflow bắt stdout này)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Handler 2: Ghi file — chỉ khi chạy local (không phải trong Docker)
        # QUALITY-6 FIX: Dùng biến rõ ràng RUNNING_IN_DOCKER="true" thay vì suy luận
        # từ AIRFLOW_HOME (AIRFLOW_HOME cũng được set khi cài Airflow local, gây nhầm).
        # Để kích hoạt: thêm `RUNNING_IN_DOCKER: "true"` vào environment trong docker-compose.yml.
        is_docker = os.getenv("RUNNING_IN_DOCKER", "false").lower() == "true"
        if not is_docker:
            log_dir = os.path.abspath(
                os.path.join(os.path.dirname(__file__), '..', '..', 'logs')
            )
            os.makedirs(log_dir, exist_ok=True)
            file_handler = logging.FileHandler(
                os.path.join(log_dir, 'pipeline.log'),
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger
