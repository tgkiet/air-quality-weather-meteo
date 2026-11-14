# 🌤️ Vietnam Weather & Air Quality Data Platform 🍃

> Dự báo thời tiết và PM2.5 theo giờ cho 52 điểm tra cứu ở Hà Nội và TP.HCM. Tra cứu nhanh trên Telegram, xem xu hướng và độ phủ dữ liệu trong Superset.

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.13-blue?logo=python" />
  <img src="https://img.shields.io/badge/Apache%20Airflow-3.4.0-017CEE?logo=apacheairflow" />
  <img src="https://img.shields.io/badge/dbt-1.12.7-FF694B?logo=dbt" />
  <img src="https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql" />
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker" />
</p>

---

## Dự án dùng để làm gì?

- Tra cứu mưa, nhiệt độ và bụi mịn trong các giờ tiếp theo để lên kế hoạch trong ngày.
- Theo dõi độ phủ theo ngày và so sánh dự báo nhiệt độ đã lưu với dữ liệu tham chiếu.
- Chạy một pipeline Python → PostgreSQL → dbt với Airflow điều phối và kiểm tra chất lượng dữ liệu.

Các điểm tra cứu là tọa độ yêu cầu tới Open-Meteo. Dữ liệu PM2.5 là dự báo mô hình CAMS Global với lưới khoảng 45 km. Nhiều điểm có thể cùng dùng một ô lưới. Chỉ số PM2.5 được hiển thị theo µg/m³. Nguồn: [Open-Meteo](https://open-meteo.com/en/docs/air-quality-api) và [CAMS Global](https://ads.atmosphere.copernicus.eu/datasets/cams-global-atmospheric-composition-forecasts).

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

**Stack:** Python · Apache Airflow 3.4.0 · dbt 1.12.7 · PostgreSQL 16 · Docker Compose

```
---

## Bot và dashboard

- Trong chat riêng, dùng `/location` để lưu điểm, `/briefing` để xem mưa trong 6 giờ và nhiệt độ/PM2.5 trong 24 giờ, `/subscribe` hoặc `/unsubscribe` để quản lý bản tin sáng. Dữ liệu quá 3 giờ được hiển thị là cũ hoặc thiếu theo từng nguồn.
- [Dashboard Superset mẫu](./weather-meteo-data-platform/superset/demo/README.md) có bộ lọc thành phố và điểm tra cứu, dùng dữ liệu giả lập để trình diễn.
- [Runbook](./weather-meteo-data-platform/RUNBOOK.md) có lệnh báo cáo độ phủ và sai số nhiệt độ trên dữ liệu sẵn có trong PostgreSQL.

## Phạm vi dữ liệu

Danh mục có 30 điểm Hà Nội và 22 điểm TP.HCM. Tên địa điểm là nhãn tra cứu đang dùng trong dự án. API thời tiết mặc định trả 7 ngày dự báo và API chất lượng không khí mặc định trả 5 ngày. Pipeline lấy dữ liệu mỗi giờ khi Airflow hoạt động. Độ phủ lịch sử phụ thuộc các đợt backfill đã chạy, vì vậy hãy xem báo cáo độ phủ trước khi dùng dữ liệu để phân tích một giai đoạn.

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
# Mở Airflow ở localhost:8080, kiểm tra health rồi bật DAG khi sẵn sàng.

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

> 💡 **Mẹo Vá Dữ Liệu (Gap Filling):** Script có tính năng **Smart Skip** tự động bỏ qua nếu data trong khoảng `[start-date, end-date]` đã có sẵn >95%. Nếu bạn bị lủng dữ liệu ở một vài ngày gần đây do tắt máy, hãy **thu hẹp khoảng thời gian** sát với phần bị thiếu (VD: `--start-date 2026-06-01`). Đừng để `start-date` từ tận 2022, vì lượng data cũ quá lớn sẽ làm tỷ lệ >95% và script sẽ skip (bỏ qua) phần bị thiếu của bạn!

> Xem [Runbook](./weather-meteo-data-platform/RUNBOOK.md) để nâng cấp DB hiện có và kiểm tra bản sao lưu trước khi thay đổi volume.

---

## 📚 Tài Liệu Chi Tiết

| Tài Liệu | Nội Dung |
|---|---|
| [weather-meteo-data-platform/README.md](./weather-meteo-data-platform/README.md) | Platform architecture, data flow, Gold layer columns |
| [src/README.md](./weather-meteo-data-platform/src/README.md) | Extract & Load modules, APIs, backfill strategy |
| [airflow/README.md](./weather-meteo-data-platform/airflow/README.md) | DAG config, tasks, CLI commands |
| [dbt-transform/README.md](./weather-meteo-data-platform/dbt-transform/README.md) | Models, materializations, 32 data quality tests |

---

## Thiết Kế Kỹ Thuật Nổi Bật

| Nguyên Tắc | Giải Pháp |
|---|---|
| **Idempotency** | Hai nguồn Bronze ghi cùng transaction theo khóa batch. Silver ưu tiên dữ liệu đúng nguồn và batch mới hơn. |
| **Strict index matching** | Map API response → config location theo index, chống lỗi Grid Snapping |
| **52 Vùng quan trắc** | Tọa độ tra cứu cố định. Giữ riêng tọa độ mô hình trả về để giải thích độ phân giải. |
| **Time alignment** | AQ/Weather ghép theo `time_str` dict key, không theo array index |
| **UNION ALL safety** | Explicit column list (đúng thứ tự) ở cả hai bên |
| **NULL safety** | `IS NULL` guard tường minh trước mọi CASE comparison |
| **Fail-fast** | `raise` thay vì `return` → Airflow nhận đúng FAILED signal |
| **Cấu hình** | Danh mục điểm và ngưỡng bot nằm trong JSON. Quy tắc phân loại trong mart nằm trong dbt. |

---

## Liên Hệ

- **Email:** giakiet.work@gmail.com