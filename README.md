# Air Quality & Weather Data Pipeline

> Hệ thống Data Engineering end-to-end — thu thập, xử lý và phân tích dữ liệu Thời tiết & Chất lượng không khí tại TP.HCM theo chuẩn **Modern Data Stack**.

---

## Mục Tiêu Dự Án

Xây dựng một Data Platform tự động hóa hoàn toàn quy trình từ thu thập dữ liệu thô từ API → lưu trữ → biến đổi → phục vụ BI & Analytics.

---

## Kiến Trúc Hệ Thống

Dự án áp dụng mô hình **ELT** kết hợp **Medallion Architecture**:

```
[Open-Meteo API]
      │
      ▼
🥉 Bronze  →  Raw JSONB (api_openmeteo_raw_data)
      │
      ▼ dbt
🥈 Silver  →  Parsed & Deduplicated (stg_weather_hourly, stg_air_quality_hourly)
      │
      ▼ dbt
🥇 Gold    →  Analytics-ready (mart_hourly_meteo_report)
```

**Công nghệ:** Python · Apache Airflow 3.2.0 · PostgreSQL 16 · dbt · Docker Compose

---

## Cấu Trúc Thư Mục

```
air-quality-weather-meteo/
│
└── weather-meteo-data-platform/     # ← Core của toàn bộ hệ thống
    ├── src/                         # Python: Extract & Load
    ├── airflow/                     # DAG: Orchestration
    ├── dbt-transform/               # SQL: Transform
    └── docs/                        # Tài liệu chi tiết
```

---

## 📚 Tài Liệu

| Tài Liệu | Nội Dung |
|---|---|
| [weather-meteo-data-platform/README.md](./weather-meteo-data-platform/README.md) | Tổng quan platform, Quick Start |
| [docs/SETUP.md](./weather-meteo-data-platform/docs/SETUP.md) | Cài đặt, cấu hình `.env`, biến môi trường |
| [docs/ARCHITECTURE.md](./weather-meteo-data-platform/docs/ARCHITECTURE.md) | Kiến trúc dữ liệu, luồng xử lý chi tiết |
| [docs/TROUBLESHOOTING.md](./weather-meteo-data-platform/docs/TROUBLESHOOTING.md) | Xử lý sự cố thường gặp |

---

## Dataset

- **`Open-Meteo-Dataset/`**: Bộ dữ liệu crawl mẫu (air quality & weather từ 02/8/2022 đến 29/11/2025 tại 31 locations từ OpenAQ).
  > Dữ liệu được gitignore do dung lượng lớn. Liên hệ để nhận file.

---

## Liên Hệ

- **Email:** giakiet.work@gmail.com

## Cấu trúc Documentation
air-quality-weather-meteo/
├── README.md                          ← Cấp 1: Cổng vào dự án
│
└── weather-meteo-data-platform/
    ├── README.md                      ← Cấp 2: Landing page
    │
    ├── docs/                          
    │   ├── SETUP.md                   Env vars, quick start, cấu hình
    │   ├── ARCHITECTURE.md            Medallion arch, data flow, Docker diagram
    │   └── TROUBLESHOOTING.md         Bảng lỗi + lệnh chẩn đoán
    │
    ├── src/README.md                  Chi tiết Extract & Load modules
    ├── airflow/README.md              Chi tiết DAG, schedule, auth
    └── dbt-transform/README.md        Chi tiết Transform layer
