# ☁️ Weather & Air Quality Data Platform

> Pipeline dữ liệu tự động theo chuẩn **Data Engineering Enterprise** — thu thập, lưu trữ và xử lý dữ liệu thời tiết & chất lượng không khí TP.HCM mỗi giờ, hoàn toàn tự động.

---

## Kiến Trúc Tổng Quan (Medallion Architecture)

```
[Open-Meteo API (Forecast 7 Days)] 
       │
       ▼ Extract (Fail-fast OOP Python)
[PostgreSQL Bronze] ──► Raw JSONB 
       │
       ▼ Transform (dbt LATERAL + Idempotency)
[PostgreSQL Silver] ──► Cleansed & Deduplicated Tables
       │
       ▼ Transform (dbt JOIN)
[PostgreSQL Gold]   ──► Data Marts (Analytics-Ready)
```

| Tầng | Công Cụ | Vai Trò |
|---|---|---|
| **Orchestration** | Apache Airflow 3.2.0 | Tự động trigger pipeline mỗi đầu giờ, truyền `execution_date` |
| **Extract & Load** | Python (OOP) | Gọi API, ghi raw JSON vào PostgreSQL theo nguyên lý Lũy đẳng |
| **Storage** | PostgreSQL 16 | Data Warehouse chứa 3 tầng kiến trúc (Bronze/Silver/Gold) |
| **Transform** | dbt | Xử lý logic phức tạp (Flatten JSON, Timezone shift, Deduplication) |
| **Infrastructure** | Docker Compose | Toàn bộ hệ thống chạy trong container|
---

## Mục Tiêu & Bài Toán Ứng Dụng (Business Value)

Pipeline này không chỉ thu thập dữ liệu mà được thiết kế để giải quyết 3 bài toán lớn:

1. **Hệ thống Cảnh báo Sức khỏe Chủ động (Proactive Health Alert System):** Sử dụng dữ liệu dự báo 7 ngày tới (Real-time Pipeline) để đưa ra các cảnh báo sớm về chỉ số PM2.5, UV, Nhiệt độ.
2. **Thiết lập Đường cơ sở (Historical Baselines):** Kết hợp với 800.000 dòng dữ liệu lịch sử để tạo bối cảnh (Ví dụ: "PM2.5 ngày mai cao gấp đôi trung bình của 3 năm trước"). (OPTIONAL CHOICE)
3. **Đánh giá Độ chính xác của Mô hình (Forecast vs. Actuals):** Lưu trữ lại các bản dự báo theo từng giờ để đối chiếu với dữ liệu thực tế, phục vụ việc đánh giá chất lượng của API hoặc làm input cho mô hình Machine Learning.(OPTIONAL CHOICE)

---

## Cấu Trúc Thư Mục

```
weather-meteo-data-platform/
├── 📄 docker-compose.yml        # Hạ tầng: Postgres + Airflow
├── 📄 Dockerfile                # Custom Airflow image
├── 📄 .env                      # ⚠️ Credentials (KHÔNG commit lên Git)
│
├── 📁 src/                      # Extract & Load Layer  →  src/README.md
├── 📁 airflow/                  # Orchestration Layer   →  airflow/README.md
├── 📁 dbt-transform/            # Transform Layer       →  dbt-transform/README.md
└── 📁 docs/                     # Tài liệu chi tiết
    ├── SETUP.md                 # Hướng dẫn cài đặt & cấu hình
    ├── ARCHITECTURE.md          # Kiến trúc dữ liệu & luồng xử lý
    └── TROUBLESHOOTING.md       # Xử lý sự cố
```

---

## Quick Start

```bash
# 1. Tạo file .env (xem docs/SETUP.md để biết cấu trúc)
# 2. Khởi chạy toàn bộ hệ thống
docker compose up -d --build

# 3. Truy cập Airflow UI
open http://localhost:8080
# Username/Password: xem _AIRFLOW_WWW_USER_* trong .env
```

> ⚠️ **Reset hoàn toàn (xóa sạch data):** `docker compose down -v && docker compose up -d`

---

## 📚 Tài Liệu

| Tài Liệu | Nội Dung |
|---|---|
| [docs/SETUP.md](./docs/SETUP.md) | Yêu cầu hệ thống, cấu hình `.env`, giải thích biến môi trường |
| [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) | Medallion Architecture, luồng dữ liệu, quyết định kỹ thuật |
| [docs/TROUBLESHOOTING.md](./docs/TROUBLESHOOTING.md) | Chẩn đoán và khắc phục lỗi thường gặp |
| [src/README.md](./src/README.md) | Chi tiết Extract & Load Layer (Python modules) |
| [airflow/README.md](./airflow/README.md) | Chi tiết Orchestration (DAG, schedule, auth) |
| [dbt-transform/README.md](./dbt-transform/README.md) | Chi tiết Transform Layer (dbt models, SQL) |

---

## Nguyên Tắc Kiến Trúc

- **OOP & Separation of Concerns** — Extract, Load, Config mỗi class một trách nhiệm duy nhất
- **No Hardcoding** — Cấu hình API trong `config.json`, credentials trong `.env`
- **Proper Error Propagation** — Dùng `raise` thay vì `return` để Airflow nhận đúng tín hiệu FAILED
- **SQL Injection Prevention** — `psycopg2.sql.Identifier` cho tên bảng động
- **Infrastructure as Code** — Toàn bộ hạ tầng trong `docker-compose.yml`
