# 🔄 dbt-transform/ — Transform Layer (Silver & Gold)

> Toàn bộ logic **Transform** trong chuỗi ELT. Biến đổi JSON thô từ Bronze thành bảng phẳng, sạch, sẵn sàng cho BI. Sử dụng **dbt 1.9.0** với `dbt-postgres` tích hợp trực tiếp vào Airflow container.

---

## Cấu Trúc

```
dbt-transform/
├── dbt_project.yml                  # Config project + materialization theo folder
├── profiles.yml  (mount read-only)  # DB connection (env_var, không hardcode)
│
├── macros/
│   └── generate_schema_name.sql     # Override schema naming (gold_layer)
│
└── models/
    ├── staging/
    │   ├── sources.yml              # Khai báo Bronze sources + tests
    │   ├── stg_weather_hourly.sql   # Realtime weather: flatten JSONB → rows
    │   ├── stg_air_quality_hourly.sql
    │   ├── stg_historical_weather.sql  # Historical Hanoi/HCM data
    │   └── stg_historical_air_quality.sql
    │
    ├── silver/
    │   ├── schema.yml               # Tests + descriptions
    │   ├── slv_weather_hourly.sql   # INCREMENTAL + DISTINCT ON + UNION ALL
    │   └── slv_air_quality_hourly.sql
    │
    └── marts/
        ├── schema.yml               # Tests + accepted_values
        └── mart_hourly_conditions.sql  # Gold: LEFT JOIN + derived metrics
```

---

## Materialization Strategy

| Folder | Materialization | Lý Do |
|---|---|---|
| `staging/` | `view` | Không lưu vật lý, chỉ là SQL alias. Không tốn storage. |
| `silver/` | `incremental` | Chỉ process batch mới → nhanh hơn full refresh ×10+ |
| `marts/` | `table` (schema: `gold_layer`) | Flat table cho BI — read-heavy, write-once-per-run |

---

## 1. Staging Layer (VIEWs)

**Nhiệm vụ:** Flatten JSONB array của Open-Meteo thành bảng tabular.

**Kỹ thuật chính:**
```sql
-- LATERAL unnest cùng lúc nhiều mảng JSON song song
LATERAL jsonb_array_elements_text(raw_json->'hourly'->'time') WITH ORDINALITY AS t(val, idx)
LATERAL jsonb_array_elements_text(raw_json->'hourly'->'temperature_2m') WITH ORDINALITY AS temp(val, idx)
ON t.idx = temp.idx  -- sync theo ordinality index
```

**CRIT-2 FIX:** Mọi staging model phải **explicit list** đúng thứ tự cột để UNION ALL trong Silver không bị positional mismatch. Không được dùng `SELECT *` trong UNION ALL.

---

## 2. Silver Layer (INCREMENTAL)

```sql
-- DISTINCT ON lấy bản forecast mới nhất cho mỗi giờ/location
SELECT DISTINCT ON (forecast_time, latitude, longitude)
    *
FROM (
    SELECT * FROM {{ ref('stg_weather_hourly') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_historical_weather') }}
)
{% if is_incremental() %}
WHERE execution_date > (
    SELECT COALESCE(MAX(execution_date), '1900-01-01'::timestamptz)
    FROM {{ this }}
)
{% endif %}
ORDER BY forecast_time, latitude, longitude, execution_date DESC
```

**Logic giải thích:**
- `execution_date DESC` → DISTINCT ON giữ bản dự báo **mới nhất** cho mỗi (giờ, vị trí)
- `> max(execution_date)` thay vì `>=` → không reprocess batch cuối mỗi giờ
- dbt `unique_key = ['forecast_time', 'latitude', 'longitude', 'location_name']` → MERGE đúng

---

## 3. Gold Layer — `mart_hourly_conditions`

**Flat table** gộp Weather + AQ với derived metrics:

```sql
-- Weather (7 ngày) LEFT JOIN Air Quality (5 ngày)
-- → Giữ tối đa dữ liệu, AQ NULL ở ngày 6-7 là expected
FROM weather w
LEFT JOIN air_quality aq
    ON  w.forecast_time = aq.forecast_time
    AND w.latitude      = aq.latitude
    AND w.longitude     = aq.longitude
```

**Derived Metrics:**

| Cột | Logic |
|---|---|
| `temperature_level` | CASE: Mát mẻ(<20) / Dễ chịu(20-28) / Nóng(28-35) / Rất nóng(35-38) / Nguy hiểm(≥38) |
| `uv_level` | CASE: Thấp(<3) / Trung bình(3-6) / Cao(6-8) / Rất cao(8-11) / Cực kỳ nguy hiểm(≥11) |
| `pm2_5_level` | AQI Mỹ: Tốt(<12) / Trung bình(12-35) / Không lành mạnh(35-55) / Rất không lành mạnh(55-150) / Nguy hiểm |
| `is_weather_alert` | TRUE nếu temp≥38°C HOẶC (UV IS NOT NULL AND UV≥8). **KHÔNG BAO GIỜ NULL.** |
| `is_air_quality_alert` | TRUE nếu PM2.5≥55. NULL khi ngoài tầm CAMS forecast (~ngày 6-7). |

---

## Data Quality Gates — 29 Tests

| Layer | Số Tests | Coverage |
|---|---|---|
| Bronze Sources | 7 | `not_null` id/source_type/execution_date/datetime/lat/lon; `unique` id |
| Silver | 8 | `not_null` forecast_time/lat/lon/location_name (×2 models); warn cho location_name |
| Gold | 14 | `not_null` key dims + is_weather_alert + temperature_2m; `accepted_values` cho 3 level columns |

```bash
# Chạy toàn bộ tests
docker exec airflow_container bash -c \
    "dbt test --project-dir /opt/airflow/dbt-transform \
               --profiles-dir /home/airflow/.dbt"

# Kết quả mong đợi
# Done. PASS=29 WARN=0 ERROR=0 SKIP=0 TOTAL=29
```

---

## Chạy Thủ Công

```bash
# Vào container
docker exec -it airflow_container bash

# Build tất cả models
dbt run --project-dir /opt/airflow/dbt-transform \
        --profiles-dir /home/airflow/.dbt

# Full refresh (khi thay đổi schema)
dbt run --full-refresh \
        --project-dir /opt/airflow/dbt-transform \
        --profiles-dir /home/airflow/.dbt

# Chỉ build Gold
dbt run --select mart_hourly_conditions \
        --project-dir /opt/airflow/dbt-transform \
        --profiles-dir /home/airflow/.dbt

# Test một model cụ thể
dbt test --select slv_weather_hourly \
         --project-dir /opt/airflow/dbt-transform \
         --profiles-dir /home/airflow/.dbt
```

---

## Các Quyết Định Kỹ Thuật Quan Trọng

| Quyết Định | Lý Do |
|---|---|
| dbt trong Airflow container (không có dbt container riêng) | Tránh Environment Drift — dev/prod cùng môi trường |
| `profiles.yml` mount read-only (`ro`) | Bảo mật — không để Airflow container ghi đè credentials |
| `generate_schema_name.sql` macro override | Tránh schema xấu kiểu `silver_layer_gold_layer` |
| Silver `location_name` warn (không fail) | Hanoi CSV lịch sử có thể NULL — không muốn block pipeline |
| Gold `is_air_quality_alert` nullable | NULL ≠ FALSE — phân biệt "không có data" với "không alert" |
