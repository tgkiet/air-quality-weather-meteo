# 🌤️ Vietnam Weather & Air Quality Data Platform 🍃

> **Enterprise-grade Data Engineering pipeline** - tự động thu thập, chuẩn hóa và phân tích dữ liệu **thời tiết & chất lượng không khí** cho **52 khu vực quan trắc** trên 2 thành phố: **30 Quận/Huyện Hà Nội** và **22 Quận/Huyện TP.HCM**, cập nhật mỗi giờ, lịch sử từ năm 2022.

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.13-blue?logo=python" />
  <img src="https://img.shields.io/badge/Apache%20Airflow-3.2.0-017CEE?logo=apacheairflow" />
  <img src="https://img.shields.io/badge/dbt-1.9.0-FF694B?logo=dbt" />
  <img src="https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql" />
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker" />
</p>

---

## 🌟 Giới Thiệu
Dự án này là một **Data Platform hoàn chỉnh (End-to-End)**, sử dụng Modern Data Stack để xây dựng luồng xử lý dữ liệu chuẩn mức Enterprise. Nền tảng được tối ưu hóa để thu thập, xử lý và lưu trữ dữ liệu thời tiết và chất lượng không khí với độ bao phủ cao.

Hệ thống cung cấp giải pháp toàn diện với:
  - **Medallion Architecture** (Bronze → Silver → Gold)
  - **Idempotency** đầy đủ tại mọi tầng (UPSERT everywhere)
  - **52 Vùng quan trắc (30 Quận/Huyện HN + 22 Quận/Huyện HCM)** lấy trực tiếp 100% từ API, dữ liệu hoàn toàn độc lập và chính xác về mặt địa lý.
  - **29 Data Quality Tests** tự động qua dbt
  - **Dockerized** hoàn toàn với healthcheck và RBAC-ready schema
---

## Kiến Trúc Tổng Quan

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                            │
│  Open-Meteo Forecast API    Open-Meteo Archive API         │
│  (52 locations/batch)       (Full History Backfill)        │
└──────────────┬──────────────────────┬──────────────────────┘
               ▼                      ▼
┌─────────────────────────────────────────────────────────────┐
│                   BRONZE LAYER (Raw)                      │
│  api_openmeteo_raw_data          bronze_historical_weather  │
│  JSONB · UPSERT · Idempotency    1 row/giờ/location · 2022+│
└──────────────────────────┬──────────────────────────────────┘
                           ▼  dbt (LATERAL unnest + UNION ALL)
┌─────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Cleaned)                  │
│  slv_weather_hourly              slv_air_quality_hourly     │
│  INCREMENTAL · DISTINCT ON       INCREMENTAL · DISTINCT ON  │
└──────────────────────────┬──────────────────────────────────┘
                           ▼  dbt (LEFT JOIN + Derived Metrics)
┌─────────────────────────────────────────────────────────────┐
│                   GOLD LAYER (Business-Ready)             │
│          gold_layer.mart_hourly_conditions (TABLE)           │
│   location_name · forecast_time · weather · AQ · alerts     │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  CONSUMPTION / SERVING LAYER              │
│  Apache Superset (Trực quan hóa Dashboard)                  │
│  Telegram Interactive Bot (Pull: Tra cứu /weather, /aqi)    │
│  Alert Job Broadcast:                                       │
│   ├─ 06:00 Bản tin Sáng: Quét toàn bộ ngày hôm nay          │
│   ├─ 20:00 Bản tin Tối: Quét toàn bộ ngày mai               │
│   └─ Khẩn cấp (giờ khác): Quét đột xuất 6 giờ tới           │
└─────────────────────────────────────────────────────────────┘

**Stack:** Python · Apache Airflow 3.2.0 · dbt 1.9.0 · PostgreSQL 16 · Docker Compose

---

## 🤖 Consumption Layer (Telegram Bot & Alert)

Hệ thống giao tiếp với người dùng qua 3 kịch bản Push Notification chính:
- **Bản tin Sáng (06:00 - AQI):** Cảnh báo ô nhiễm không khí (bụi mịn PM2.5) cho **toàn bộ ngày hôm nay**, giúp người dùng chuẩn bị khẩu trang trước khi đi làm.
- **Bản tin Tối (20:00 - Mưa Lớn):** Tổng hợp rủi ro mưa lớn cho **toàn bộ ngày mai**, giúp người dùng lên kế hoạch lịch trình.
- **Cảnh báo Đột xuất (Mưa Khẩn Cấp):** Các giờ còn lại, hệ thống liên tục quét trước **cửa sổ 6 giờ tới (từ thời điểm hiện tại)** để phát hiện mưa bất chợt. Tích hợp cơ chế **Stateful Deduplication**, đảm bảo cảnh báo khẩn cấp chỉ kích hoạt **đúng 1 lần** cho 1 cơn mưa, chống spam tuyệt đối.

---

## Phạm Vi Dữ Liệu

| | Hà Nội | TP.HCM |
|---|---|---|
| **Locations** | 30 Quận/Huyện | 22 Quận/Huyện |
| **Lịch sử** | 2022-08-02 → Hiện tại (Archive API) | 2022-08-02 → 2026-05-27 (Archive API) |
| **Realtime** | Dự báo 7 ngày tới (cập nhật mỗi giờ) | Dự báo 7 ngày tới (cập nhật mỗi giờ) |
| **Nguồn** | Open-Meteo Archive API (100% API-driven) | Open-Meteo Forecast + Archive API |

>  **Kiến trúc API-Driven:** Hệ thống lấy dữ liệu trực tiếp từ Open-Meteo Archive API cho toàn bộ 52 Quận/Huyện chuẩn hành chính của 2 thành phố, đảm bảo dữ liệu đồng nhất từ 2022 đến nay.

---

## Cấu Trúc Repository

```
air-quality-weather-meteo/
│
├──  README.md                        ← Bạn đang đọc file này
├──  requirements.txt                 ← Dependencies Python local dev
├──  .gitignore
│
└──  weather-meteo-data-platform/     ← Core Platform (toàn bộ hệ thống)
    ├──  README.md                    ← Platform overview & Quick Start
    ├──  docker-compose.yml           ← Infra: Postgres + Airflow
    ├──  Dockerfile                   ← Airflow + dbt image
    ├──  .env                         ← không commit
    │
    ├──  src/                         ← Extract & Load Layer
    │   ├──  README.md
    │   ├── config/                     ← config.json & config_runtime_constant.json
    │   ├── extractors/open_meteo.py    ← Session + Retry + Data Contract
    │   ├── loaders/                    ← PostgresLoader
    │   ├── scripts/                    ← telegram_bot.py, alert_job.py, backfill_history.py
    │   ├── utils/                      ← ConfigManager (Hybrid Fail-Fast/Resilient)
    │   └── main.py                     ← ELT entrypoint
    │
    ├──  airflow/                     ← Orchestration Layer
    │   ├──  README.md
    │   └── dags/orchestrator.py        ← DAG @hourly, 4 tasks
    │
    └──  dbt-transform/               ← Transform Layer
        ├──  README.md
        └── models/
            ├── staging/                ← VIEW: flatten JSON + timezone
            ├── silver/                 ← INCREMENTAL: dedup + union
            └── marts/                  ← TABLE (gold_layer): flat mart
```

---

## Quick Start

```bash
# Clone
git clone <repo-url>
cd air-quality-weather-meteo/weather-meteo-data-platform

# Cấu hình environment
cp .env.example .env   # Chỉnh sửa credentials

# Khởi động toàn bộ hệ thống
docker compose up -d --build
# Postgres healthcheck xong → Airflow tự động start (không race condition)

# Truy cập Airflow UI
open http://localhost:8080


# Nạp dữ liệu lịch sử hoàn chỉnh bằng API (100% API-Driven, không dùng CSV)
# Archive API hỗ trợ kéo dữ liệu từ 2022 đến hiện tại (tự động ghép ERA5 + IFS)

# Backfill toàn bộ 30 Quận/Huyện Hà Nội
docker exec airflow_container \
    python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HN \
    --start-date 2022-08-02 --end-date 2026-05-27

# Backfill toàn bộ 22 Quận/Huyện TP.HCM
docker exec airflow_container \
    python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HCM \
    --start-date 2022-08-02 --end-date 2026-05-27
```

>  **Reset hoàn toàn:** `docker compose down -v && docker compose up -d --build`

---

## 📚 Tài Liệu Chi Tiết

| Tài Liệu | Nội Dung |
|---|---|
| [weather-meteo-data-platform/README.md](./weather-meteo-data-platform/README.md) | Platform architecture, data flow, Gold layer columns |
| [src/README.md](./weather-meteo-data-platform/src/README.md) | Extract & Load modules, APIs, backfill strategy |
| [airflow/README.md](./weather-meteo-data-platform/airflow/README.md) | DAG config, tasks, CLI commands |
| [dbt-transform/README.md](./weather-meteo-data-platform/dbt-transform/README.md) | Models, materializations, 29 data quality tests |

---

## Thiết Kế Kỹ Thuật Nổi Bật

| Nguyên Tắc | Giải Pháp |
|---|---|
| **Idempotency** | UPSERT + `execution_date` từ Airflow `logical_date` |
| **Strict index matching** | Map API response → config location theo index, chống lỗi Grid Snapping |
| **52 Vùng quan trắc** | Tọa độ chuẩn hành chính từ Nominatim (OpenStreetMap), độc lập về mặt không gian |
| **Time alignment** | AQ/Weather ghép theo `time_str` dict key, không theo array index |
| **UNION ALL safety** | Explicit column list (đúng thứ tự) ở cả hai bên |
| **NULL safety** | `IS NULL` guard tường minh trước mọi CASE comparison |
| **Fail-fast** | `raise` thay vì `return` → Airflow nhận đúng FAILED signal |
| **Zero Hardcode** | Configurations nằm toàn bộ trong JSON (Hybrid: Fail-Fast cho cấu trúc, Resilient cho mạng) |

---

## Liên Hệ

- **Email:** giakiet.work@gmail.com