#  src/ — Extract & Load Layer

> Toàn bộ logic Python cho bước **E (Extract)** và **L (Load)** trong chuỗi ELT. Tuân thủ **OOP**, **Twelve-Factor App**, **Separation of Concerns**, và **Idempotency**.

---

## Cấu Trúc Module

```
src/
├── main.py                          # Entrypoint — điều phối Extract & Load
│
├── config/
│   ├── config.json                  # Cấu hình 52 locations + API params
│   └── config_runtime_constant.json # Bot UI/UX config, thresholds, districts
│
├── extractors/
│   └── open_meteo.py                # OpenMeteoExtractor (Session, Retry, Contract)
│
├── loaders/
│   ├── base_loader.py               # BasePostgresLoader (connect, close, retry)
│   ├── postgres_loader.py           # UPSERT realtime data → Bronze
│
├── scripts/
│   ├── init_dbs.sh                  # Khởi tạo DB, tables, UNIQUE constraints
│   ├── backfill_history.py          # Backfill lịch sử từ Archive API
│   ├── telegram_bot.py              # Controller: Lõi Interactive Pull Bot
│   ├── bot_services.py              # Service/Formatter: Xử lý UX & Formatting
│   └── alert_job.py                 # Lõi Push: Broadcast & Stateful Deduplication
│
└── utils/
    ├── config_manager.py            # ConfigManager Singleton
    └── logger.py                    # Console (Docker) / File (local)
```

---

## main.py — ELT Entrypoint

Nhận `--execution_date` từ Airflow BashOperator (Jinja template `{{ logical_date | ts }}`). **Tuyệt đối không** dùng `datetime.now()` — vi phạm Idempotency.

```bash
# Giả lập chạy cron job của ngày hôm nay (Execution Date)
python src/main.py --execution_date "2026-05-27T04:00:00+00:00"
```

**Luồng xử lý:**
1. Load config → build batch params (52 lats, 52 lons joined bằng dấu phẩy)
2. Extract Weather API → `_inject_location_metadata()` → Strict Index Matching (zip)
3. Extract AQ API → `_inject_location_metadata()`
4. UPSERT cả 2 vào `api_openmeteo_raw_data` (Bronze)

---

## extractors/open_meteo.py

| Feature | Chi Tiết |
|---|---|
| **Session Pooling** | `requests.Session` tái dùng TCP — tránh handshake mỗi request |
| **Exponential Backoff** | Retry 3 lần: 0s → 2s → 4s. Chỉ retry 429/5xx, không retry 4xx |
| **Data Contract** | `get_open_meteo_data(params, expected_keys)` — validate schema trước khi return |
| **Fail-Fast** | `raise` nếu contract fail → Airflow nhận FAILED, không load rác |

---

## loaders/

### base_loader.py
- `BasePostgresLoader` — base class cho tất cả loaders
- `connect()` với retry 3 lần, dùng biến môi trường từ `.env`
- `close()` — đảm bảo không rò rỉ connection (gọi trong `finally`)

### postgres_loader.py
- UPSERT vào `api_openmeteo_raw_data` với `ON CONFLICT (source_type, execution_date) DO UPDATE`
- Tên bảng dùng `psycopg2.sql.Identifier` → tránh SQL Injection
- Serialize `raw_json` với `json.dumps()` → lưu nguyên vẹn JSONB


---

## scripts/

### init_dbs.sh
Chạy tự động khi Postgres container khởi tạo lần đầu. Tạo:
- `api_openmeteo_raw_data` + `UNIQUE(source_type, execution_date)`
- `bronze_historical_weather` + `UNIQUE(datetime, lat, lon)` + cột `location_name`

### Kiến trúc Dual-Core Telegram Bot
- **`telegram_bot.py`**: Lõi Pull (Interactive). Quản lý Keyboard, Handler lệnh `/weather`, `/aqi`. Tích hợp cơ chế **State Editing** (giữ 1 tin nhắn duy nhất) và **Phân trang (Pagination)** 6h/12h/24h. Tách biệt hoàn toàn DB logic.
- **`bot_services.py`**: Chứa `BotDatabaseManager` (với Threaded Connection Pooling) và `BotFormatter`. Định dạng số liệu thô thành ngôn ngữ tự nhiên. Xử lý "0.0mm Paradox" và giới hạn 4096 ký tự của Telegram.
- **`alert_job.py`**: Lõi Push (Cronjob). Chạy bản tin Đa rủi ro (Holistic Briefing: Mưa, UV, PM2.5, Nhiệt độ) lúc 06:00, 20:00 và khẩn cấp (6H window). Có cơ chế **Stateful Deduplication** (lưu `silver_layer.alert_history`) để chống spam và **System Heartbeat**.

### backfill_history.py
```bash
# Backfill Hà Nội toàn bộ (30 Quận/Huyện từ 2022 đến nay)
python3 backfill_history.py \
    --location-prefix HN \
    --start-date 2022-08-02 --end-date <YYYY-MM-DD>

# Backfill TP.HCM toàn bộ (22 Quận/Huyện từ 2022 đến nay)
python3 backfill_history.py \
    --location-prefix HCM \
    --start-date 2022-08-02 --end-date <YYYY-MM-DD>
```
- `--location-prefix` là **bắt buộc** (`HCM` hoặc `HN`)
- Filter locations có prefix tương ứng từ `config.json`
- Gọi Archive API riêng biệt cho Weather + AQ
- **Align AQ/Weather theo `time_str` dict key** (không phải positional index)
- Validate date format và thứ tự (start ≤ end) trước khi kết nối DB
- UPSERT vào `bronze_historical_weather`

