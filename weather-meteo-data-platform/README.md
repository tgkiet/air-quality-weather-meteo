# ☁️ Weather & Air Quality Data Platform

> Pipeline dữ liệu tự động theo chuẩn **Data Engineering Enterprise** — thu thập, lưu trữ và xử lý dữ liệu thời tiết & chất lượng không khí TP.HCM mỗi giờ, hoàn toàn tự động.

---

## Kiến Trúc Tổng Quan

```
[Open-Meteo API] ──► [Extract] ──► [Load: PostgreSQL Bronze] ──► [dbt Transform: Silver → Gold]
                          ↑
                 [Airflow Scheduler @hourly]
```

| Tầng | Công Cụ | Vai Trò |
|---|---|---|
| **Orchestration** | Apache Airflow 3.2.0 | Tự động trigger pipeline mỗi đầu giờ |
| **Extract & Load** | Python (OOP) | Gọi API, ghi raw JSON vào PostgreSQL |
| **Storage** | PostgreSQL 16 | Data Warehouse (Bronze/Silver/Gold) |
| **Transform** | dbt | SQL transform, dedup, parse JSON |
| **Infrastructure** | Docker Compose | Toàn bộ hệ thống chạy trong container |

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
