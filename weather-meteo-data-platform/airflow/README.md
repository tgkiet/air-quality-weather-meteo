# airflow/ — Orchestration Layer

> **Apache Airflow 3.2.0** điều phối toàn bộ pipeline theo lịch `@hourly`, đảm bảo Idempotency qua `logical_date`, tự động retry và báo lỗi.

---

## Cấu Trúc

```
airflow/
├── dags/
│   └── orchestrator.py      # DAG duy nhất — 4 tasks nối tiếp
└── logs/                    # Airflow tự ghi log task
```

---

## DAG: `open_meteo_api_pipeline_orchestrator`

### Cấu Hình

| Thuộc Tính | Giá Trị | Lý Do |
|---|---|---|
| `schedule` | `@hourly` (`0 * * * *`) | Chạy đúng phút 0 mỗi giờ — chuẩn Time Series |
| `catchup` | `False` | Không chạy bù các giờ bị bỏ lỡ |
| `retries` | `3` | Tự retry khi API timeout hoặc DB tạm thời lỗi |
| `retry_delay` | `5 phút` | Không retry ngay — cho API/DB thời gian recover |
| `start_date` | `2026-05-01` | Ngày bắt đầu có hiệu lực |

> **Tại sao `@hourly` thay vì `timedelta(hours=1)`?**
> `timedelta` đếm từ lúc Airflow start → chạy vào giờ lẻ (10:13, 11:13...). `@hourly` luôn chạy vào phút 0 (10:00, 11:00...) — chuẩn để join/aggregate time series.

---

### Task Flow

```
[fetch_data]  ──►  [dbt_run]  ──►  [dbt_test]  ──►  [send_alert]
  BashOperator     BashOperator     BashOperator     BashOperator
  Extract+Load     Transform        Quality Gate     Telegram Alert
```

**Task 1 — `fetch_data`:**
```bash
python3 /opt/airflow/src/main.py --execution_date "{{ logical_date | ts }}"
```
- Gọi Open-Meteo Batch API (52 locations/call)
- UPSERT vào Bronze: `api_openmeteo_raw_data`

**Task 2 — `dbt_run`:**
```bash
dbt run --project-dir /opt/airflow/dbt-transform \
        --profiles-dir /home/airflow/.dbt \
        --vars '{"execution_date": "{{ logical_date | ts }}"}'
```
- Staging VIEW → Silver INCREMENTAL → Gold TABLE
- 7 models, ~2-3 giây

**Task 3 — `dbt_test`:**
```bash
dbt test --project-dir /opt/airflow/dbt-transform \
         --profiles-dir /home/airflow/.dbt
```
- 32 data quality tests
- Pipeline FAIL nếu bất kỳ test nào fail → không publish dữ liệu xấu

**Task 4 — `send_alert`:**
```bash
python3 /opt/airflow/src/scripts/alert_job.py
```
- Lõi Push của kiến trúc Dual-Core Telegram Bot. Tự động quét Data Mart để phát thanh:
  1. **Bản tin Định kỳ (06:00 & 20:00)**: Tổng hợp toàn bộ rủi ro (Mưa, Bụi mịn PM2.5, Tia UV, Nắng gắt) cho Ngày/Ngày mai (Holistic Briefing).
  2. **System Heartbeat**: Gửi báo cáo "Trời đẹp" nếu không có rủi ro nào.
  3. **Cảnh báo Đột xuất (Khung giờ còn lại)**: Nhìn trước 6H, kích hoạt Stateful Deduplication để chống Spam.
- Tự động phân lớp lượng mưa: Mưa vừa (>=3.0mm) và Mưa lớn (>=5.0mm).
- Phụ thuộc vào `dbt_test`: Chỉ gửi cảnh báo nếu dữ liệu qua được kiểm định.
- **Zero Hardcode**: Toàn bộ mốc cảnh báo, ngưỡng UV/Nhiệt độ, và lịch phát thanh được nạp động từ `config_runtime_constant.json`.

---

## Idempotency via `logical_date`

```python
# Airflow Jinja template → execution_date cố định cho mỗi scheduled run
bash_command='python3 /opt/airflow/src/main.py --execution_date "{{ logical_date | ts }}"'
```

- `logical_date` = thời điểm lên lịch, **KHÔNG** phải `datetime.now()`
- Chạy lại cùng run → cùng `execution_date` → Bronze UPSERT → không tạo duplicate
- Silver INCREMENTAL filter: `execution_date == var('execution_date')` → Chỉ process chính xác lô dữ liệu của Airflow Run đó (Orchestrator-Driven).

---

## Cơ Chế Phát Hiện Lỗi Thực

```python
# Sai — Airflow đánh SUCCESS giả, không biết có lỗi
except Exception as e:
    logger.error(e)
    return  # exit code = 0

# Đúng — Airflow nhận FAILED, trigger retry
except Exception as e:
    logger.error(e)
    raise   # exit code ≠ 0
```

---

## CLI Commands

```bash
# Xem DAGs đang chạy
docker exec airflow_container airflow dags list

# Trigger thủ công (test ngay)
docker exec airflow_container airflow dags trigger \
    open_meteo_api_pipeline_orchestrator

# Xem lịch sử runs
docker exec airflow_container airflow dags list-runs \
    -d open_meteo_api_pipeline_orchestrator

# Xem trạng thái tasks của một run
docker exec airflow_container airflow tasks states-for-dag-run \
    open_meteo_api_pipeline_orchestrator <run_id>

# Quản lý User
docker exec airflow_container airflow users list
docker exec airflow_container airflow users reset-password \
    -u <username> -p <new_password>
```

---

## Auth Manager

Hệ thống dùng **FabAuthManager** (Flask AppBuilder) thay vì `SimpleAuthManager` mặc định của Airflow 3:

| | SimpleAuthManager | FabAuthManager |
|---|---|---|
| User management | 1 user admin cố định | Nhiều users, RBAC |
| Password | Random, không custom | Custom qua CLI |
| OAuth | Không | Hỗ trợ |

```yaml
# docker-compose.yml
AIRFLOW__CORE__AUTH_MANAGER: 'airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager'
```