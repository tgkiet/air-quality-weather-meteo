# ☁️ Vietnam Weather & Air Quality Data Platform ☁️

> **Enterprise-grade Data Engineering pipeline** - tự động thu thập, chuẩn hóa và phân tích dữ liệu **thời tiết & chất lượng không khí** cho **20 khu vực quan trắc** trên 2 thành phố: **10 grid cells Hà Nội** và **10 grid cells TP.HCM**, cập nhật mỗi giờ, lịch sử từ năm 2022.

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.13-blue?logo=python" />
  <img src="https://img.shields.io/badge/Apache%20Airflow-3.2.0-017CEE?logo=apacheairflow" />
  <img src="https://img.shields.io/badge/dbt-1.9.0-FF694B?logo=dbt" />
  <img src="https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql" />
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker" />
</p>

---

## Giới Thiệu
    - Dự án này trước kia chỉ đơn giản là CALL API và dùng SCRIPT để DOWNLOAD DATA về với dạng CSV từ API Open-Meteo cho toạ độ của 31 toạ độ ở Hà Nội.
    - Sau đó thực hiện update && upgrade lên DATA PIPELINE hoàn chỉnh, dùng các modern data stack, đồng thời cũng là một DATA PIPELINE END TO END tâm huyết của tôi để đưa vào CV. Luồng xử lý dữ liệu cho 1 toạ độ là TPHCM
    - NHƯNG, sau khi backfill dữ liệu lịch sử từ file csv tôi mới nhận ra thật chất 31 location Hà Nội đó được api open-meteo gộp grid cell, nên vì thế tôi quyết định nâng cấp cả project này thành một **Data Platform hoàn chỉnh** với:
      - **Medallion Architecture** (Bronze → Silver → Gold)
      - **Idempotency** đầy đủ tại mọi tầng (UPSERT everywhere)
      - **20 grid cells** (10 HN + 10 HCM) sau khi prune config khỏp với API grid resolution
      - **29 Data Quality Tests** tự động qua dbt
      - **Dockerized** hoàn toàn với healthcheck và RBAC-ready schema
---

## Kiến Trúc Tổng Quan

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                            │
│  Open-Meteo Forecast API    Open-Meteo Archive API   CSV   │
│  (20 locations/batch)       (HCM backfill)       (Hà Nội) │
└──────────────┬──────────────────────┬───────────────┬───────┘
               ▼                      ▼               ▼
┌─────────────────────────────────────────────────────────────┐
│                   🥉 BRONZE LAYER (Raw)                      │
│  api_openmeteo_raw_data          bronze_historical_weather  │
│  JSONB · UPSERT · Idempotency    1 row/giờ/location · 2022+│
└──────────────────────────┬──────────────────────────────────┘
                           ▼  dbt (LATERAL unnest + UNION ALL)
┌─────────────────────────────────────────────────────────────┐
│                   🥈 SILVER LAYER (Cleaned)                  │
│  slv_weather_hourly              slv_air_quality_hourly     │
│  INCREMENTAL · DISTINCT ON       INCREMENTAL · DISTINCT ON  │
└──────────────────────────┬──────────────────────────────────┘
                           ▼  dbt (LEFT JOIN + Derived Metrics)
┌─────────────────────────────────────────────────────────────┐
│                   🥇 GOLD LAYER (Business-Ready)             │
│          gold_layer.mart_hourly_conditions (TABLE)           │
│   location_name · forecast_time · weather · AQ · alerts     │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
                 Apache Superset Dashboard
```

**Stack:** Python · Apache Airflow 3.2.0 · dbt 1.9.0 · PostgreSQL 16 · Docker Compose

---

## Phạm Vi Dữ Liệu

| | Hà Nội | TP.HCM |
|---|---|---|
| **Locations** | 10 grid cells |	10 grid cells |
| **Lịch sử** | 2022-08-02 → 2026-05-19 (CSV + API gap) | 2022-08-02 → 2026-05-19 (Archive API) |
| **Realtime** | Dự báo 7 ngày tới (cập nhật mỗi giờ) | Dự báo 7 ngày tới (cập nhật mỗi giờ) |
| **Nguồn** | OpenAQ CSV (2022→11/2025) + Open-Meteo Archive | Open-Meteo Forecast + Archive API |

> 📌 Config ban đầu có 53 locations (31 HN + 22 HCM). Sau khi kiểm tra thực tế, Open-Meteo API chỉ trả về 20 grid cells độc lập (các quận nội thành gần nhau bị merge). Config đã được prune xuống **20 locations** để tránh duplicate và lãng phí API calls.

---

## Cấu Trúc Repository

```
air-quality-weather-meteo/
│
├── 📄 README.md                        ← Bạn đang đọc file này
├── 📄 requirements.txt                 ← Dependencies Python local dev
├── 📄 .gitignore
│
├── 📁 Open-Meteo-Dataset/              ← Raw CSV lịch sử Hà Nội 2022-2025
│   └── hanoi_aq_weather_MERGED.csv     ← (gitignore — liên hệ để nhận file)
│
└── 📁 weather-meteo-data-platform/     ← Core Platform (toàn bộ hệ thống)
    ├── 📄 README.md                    ← Platform overview & Quick Start
    ├── 📄 docker-compose.yml           ← Infra: Postgres + Airflow
    ├── 📄 Dockerfile                   ← Airflow + dbt image
    ├── 📄 .env                         ← không commit
    │
    ├── 📁 src/                         ← Extract & Load Layer
    │   ├── 📄 README.md
    │   ├── config/config.json          ← 20 locations + API config
    │   ├── extractors/open_meteo.py    ← Session + Retry + Data Contract
    │   ├── loaders/                    ← PostgresLoader + CSVLoader
    │   ├── scripts/                    ← init_dbs.sh, backfill, CSV load
    │   ├── utils/                      ← Logger + ConfigManager
    │   └── main.py                     ← ELT entrypoint
    │
    ├── 📁 airflow/                     ← Orchestration Layer
    │   ├── 📄 README.md
    │   └── dags/orchestrator.py        ← DAG @hourly, 3 tasks
    │
    └── 📁 dbt-transform/               ← Transform Layer
        ├── 📄 README.md
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


# Nạp dữ liệu lịch sử (chạy 1 lần, theo thứ tự sau)

# Bước 1: Nạp 2 file CSV Hà Nội (2022-08-02 → 2025-11-29 · ~900k dòng · nhanh)
# CSV được tự động tìm tại /opt/airflow/csv-data/ (Docker volume mount từ Open-Meteo-Dataset/)
docker exec airflow_container \
    python3 /opt/airflow/src/scripts/load_historical_csvs.py

# Bước 2: Backfill HCM toàn bộ từ Archive API (10 grid cells × ~3.8 năm · ~30-60 phút)
# Archive API hỗ trợ đến ngày hiện tại (dữ liệu gần nhất tự động ghep ERA5 + IFS)
docker exec airflow_container \
    python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HCM \
    --start-date 2022-08-02 --end-date 2026-05-21

# Bước 3: Backfill HN gap (10 grid cells × ~6 tháng · ~5-10 phút)
# CSV chỉ đến 2025-11-29 → cần kéo thêm phần còn thiếu
docker exec airflow_container \
    python3 /opt/airflow/src/scripts/backfill_history.py \
    --location-prefix HN \
    --start-date 2025-11-30 --end-date 2026-05-21
```

> ⚠️ **Reset hoàn toàn:** `docker compose down -v && docker compose up -d --build`

---

## Tài Liệu Chi Tiết

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
| **Nearest-neighbor matching** | Map API response → config location theo toạ độ (tolerance 0.15°) |
| **API Grid Resolution** | Open-Meteo merge nearby coords — 53 configs → ~20 grid cells thực tế |
| **Time alignment** | AQ/Weather ghép theo `time_str` dict key, không theo array index |
| **UNION ALL safety** | Explicit column list (đúng thứ tự) ở cả hai bên |
| **NULL safety** | `IS NULL` guard tường minh trước mọi CASE comparison |
| **Fail-fast** | `raise` thay vì `return` → Airflow nhận đúng FAILED signal |
| **No hardcoding** | Locations → `config.json`, credentials → `.env` |

---

## Liên Hệ

- **Email:** giakiet.work@gmail.com
- **Dataset:** Dữ liệu CSV gitignore do dung lượng lớn - liên hệ để nhận file data