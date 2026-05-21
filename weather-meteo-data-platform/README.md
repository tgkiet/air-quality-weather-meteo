# ☁️ Vietnam Weather & Air Quality Data Platform

> Pipeline dữ liệu tự động quy mô quốc gia theo chuẩn **Data Engineering Enterprise** — tích hợp và đồng bộ hóa dữ liệu thời tiết & chất lượng không khí của **31 trạm quan trắc tại Hà Nội** và **22 quận/huyện tại TP.HCM**

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
       ▼ Transform (dbt LEFT JOIN)
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

## Luồng Tự Động Hóa Toàn Diện (Airflow + dbt)

Hệ thống đã đạt đến cấp độ tự động hóa hoàn toàn (End-to-End ELT Pipeline). Mỗi đầu giờ, DAG sẽ kích hoạt chuỗi 3 Tasks:
1. `fetch_data`: **Python** Extract & Load (Kéo API đẩy thẳng vào Bronze).
2. `dbt_run`: **Airflow** kích hoạt trực tiếp **dbt CLI** (không dùng Docker Socket rủi ro bảo mật). Chạy tuần tự `Staging` ➜ `Silver` (Clean/Dedup) ➜ `Gold` (Denormalize/Derived Metrics).
3. `dbt_test`: **Data Quality Gate** — Chạy 17 bài Test kiểm duyệt chất lượng. Chỉ khi dữ liệu hoàn toàn sạch sẽ không bị NULL sai quy định, toàn bộ Pipeline mới được đánh dấu là SUCCESS.

## Mục Tiêu & Bài Toán Ứng Dụng (Business Value)

Pipeline này không chỉ thu thập dữ liệu mà được thiết kế để giải quyết 3 bài toán lớn:

1. **Hệ thống Cảnh báo Sức khỏe Chủ động (Proactive Health Alert System):** Sử dụng dữ liệu dự báo 7 ngày tới (Real-time Pipeline) để đưa ra các cảnh báo sớm về chỉ số PM2.5, UV, Nhiệt độ.
2. **Thiết lập Đường cơ sở (Historical Baselines):** Tích hợp hơn 900.000 dòng dữ liệu lịch sử từ các tệp CSV để tạo bối cảnh phân tích xu hướng lâu dài (từ năm 2022 đến nay).
3. **Đánh giá Độ chính xác của Mô hình (Forecast vs. Actuals):** Lưu trữ lại các bản dự báo theo từng giờ để đối chiếu với dữ liệu thực tế, phục vụ việc đánh giá chất lượng của API hoặc làm input cho mô hình Machine Learning.(OPTIONAL CHOICE)

---

## Cấu Trúc Thư Mục

```
weather-meteo-data-platform/
├── 📄 docker-compose.yml        # Hạ tầng: Postgres + Airflow
├── 📄 Dockerfile                # Custom Airflow image
├── 📄 .env                      # ⚠️ Credentials (KHÔNG commit lên Git)
├── 📄 hanoi_aq_weather_MERGED.csv  # Dữ liệu CSV lịch sử gốc (được copy vào src/ khi nạp)
├── 📄 hanoi_realtime_data_updated.csv # Dữ liệu CSV realtime gốc
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

# 3. Nạp dữ liệu lịch sử từ file CSV vào Database (Backfill Hà Nội)
docker exec -i airflow_container python3 /opt/airflow/src/scripts/load_historical_csvs.py

# 4. Nạp dữ liệu lịch sử từ Open-Meteo API vào Database (Backfill TP.HCM)
docker exec -i airflow_container python3 /opt/airflow/src/scripts/backfill_hcm_history.py

# 5. Truy cập Airflow UI
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
