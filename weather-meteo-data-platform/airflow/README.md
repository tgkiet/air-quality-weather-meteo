# ✈️ Airflow — Orchestration Layer

Thư mục `airflow/` chứa toàn bộ cấu hình cho **Apache Airflow** — bộ não điều phối (Orchestrator) tự động hóa việc chạy pipeline theo lịch.

---

## Cấu Trúc

```
airflow/
├── dags/
│   └── orchestrator.py      # DAG duy nhất định nghĩa lịch chạy pipeline
└── logs/                    # Airflow tự động ghi log task vào đây
    └── dag_processor/       # Log của DAG processor
```

---

## DAG: `open_meteo_api_pipeline_orchestrator`

### Cấu Hình

| Thuộc Tính | Giá Trị | Mô Tả |
|---|---|---|
| `dag_id` | `open_meteo_api_pipeline_orchestrator` | Tên định danh DAG |
| `owner` | `gkinhere-airflow` | Người sở hữu |
| `schedule` | `@hourly` | Chạy vào phút 0 mỗi giờ |
| `catchup` | `False` | Không chạy bù các lần đã bỏ lỡ |
| `retries` | `3` | Tự động retry 3 lần khi task lỗi |
| `start_date` | `2026-05-01` | Ngày bắt đầu hiệu lực |

### Tasks

```
[fetch_data]
     │
     └── BashOperator
         bash_command: "python3 /opt/airflow/src/main.py"
```

Hiện tại DAG chỉ có **1 task duy nhất** (`fetch_data`) để giữ sự đơn giản. Khi tích hợp dbt, sẽ thêm task `transform_data` chạy sau:

```
[fetch_data] ──► [transform_data]
(Extract + Load)   (dbt run)
```

### Tại Sao Dùng `@hourly` Thay Vì `timedelta(hours=1)`?

- `timedelta(hours=1)`: Đếm khoảng cách 1 giờ **từ lúc bật hệ thống** → chạy vào giờ lẻ (10:13, 11:13...)
- `@hourly` (tương đương `0 * * * *`): Chạy vào **đúng phút 0** mỗi giờ (10:00, 11:00, 12:00...)

Chuẩn DE yêu cầu dữ liệu phải được chuẩn hóa theo khung giờ tròn để dễ join, aggregate và so sánh theo time series.

---

## Cách Quản Lý DAG Qua CLI

```bash
# Xem danh sách DAGs
docker exec airflow_container airflow dags list

# Trigger chạy thủ công
docker exec airflow_container airflow dags trigger open_meteo_api_pipeline_orchestrator

# Xem lịch sử chạy
docker exec airflow_container airflow dags list-runs -d open_meteo_api_pipeline_orchestrator

# Xem trạng thái tasks của một run cụ thể
docker exec airflow_container airflow tasks states-for-dag-run \
    open_meteo_api_pipeline_orchestrator <run_id>

# Quản lý User
docker exec airflow_container airflow users list
docker exec airflow_container airflow users create --help
docker exec airflow_container airflow users reset-password -u <username> -p <new_password>
```

---

## Cơ Chế Phát Hiện Lỗi Thực

Trước đây, pipeline dùng `return` khi gặp lỗi:

```python
# ❌ Sai — Airflow không biết có lỗi, vẫn đánh dấu SUCCESS giả tạo
except Exception as e:
    logger.error(e)
    return  # exit code = 0 → Airflow thấy "thành công"
```

Sau khi được fix, tất cả exception đều dùng `raise`:

```python
# ✅ Đúng — Airflow nhận exit code ≠ 0, đánh dấu FAILED và trigger retry
except Exception as e:
    logger.error(e)
    raise  # exit code ≠ 0 → Airflow biết task thất bại
```

---

## Phiên Bản & Auth Manager

Hệ thống dùng **Airflow 3.2.0** với **FabAuthManager** (Flask AppBuilder Auth Manager).

Lý do không dùng `SimpleAuthManager` mặc định của Airflow 3:
- `SimpleAuthManager` chỉ có 1 tài khoản `admin` với password ngẫu nhiên, không hỗ trợ custom credentials
- `FabAuthManager` hỗ trợ đầy đủ RBAC, tạo nhiều user, reset password qua CLI, tích hợp OAuth

Cấu hình trong `docker-compose.yml`:
```yaml
AIRFLOW__CORE__AUTH_MANAGER: 'airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager'
```