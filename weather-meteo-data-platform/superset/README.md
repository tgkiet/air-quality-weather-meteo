# superset/ — Visualization Layer (Production Grade)

> **Apache Superset 6.1.0** đóng vai trò là tầng BI (Business Intelligence) trong mô hình Data Platform, trực tiếp kết nối và visualize dữ liệu từ `gold_layer`. 

Thư mục này chứa các file cấu hình quan trọng nhất để khởi chạy Superset theo chuẩn **5-Container Architecture** của kho lưu trữ chính thức Apache, đảm bảo tính ổn định, bảo mật và khả năng mở rộng.

---

## 1. Cấu Trúc Thư Mục

```text
superset/
├── superset_config.py      # Cấu hình lõi (Database, Redis, Celery, Security)
├── requirements-local.txt  # Khai báo dependency bổ sung (psycopg2)
└── README.md               # Tài liệu bạn đang đọc
```

---

## 2. Tại Sao Lại Là 5-Container Architecture?

Thay vì nhồi nhét toàn bộ hệ sinh thái Superset vào một container duy nhất (chỉ phù hợp để test local), hệ thống này được triển khai với 5 containers chuyên biệt. Điều này mô phỏng chính xác cách một hệ thống Enterprise vận hành:

1. **`superset_app`**: Web UI chính phục vụ người dùng, chạy qua Web Server Gunicorn.
2. **`superset_init`**: Container vòng đời ngắn (chạy 1 lần lúc startup). Đảm nhiệm việc migrate database, khởi tạo Admin user và thiết lập các Roles/Permissions mặc định.
3. **`superset_worker`**: Xử lý các tác vụ nền (Async Tasks) thông qua Celery. Giúp UI không bị treo khi người dùng chạy các câu query SQL nặng, xuất file CSV lớn hoặc render ảnh thumbnail.
4. **`superset_worker_beat`**: Trình lập lịch (Scheduler) của Celery, dùng để trigger các tác vụ định kỳ (ví dụ: gửi email báo cáo hàng ngày).
5. **`redis`**: Message Broker trung gian. `superset_app` gửi task vào Redis, `worker` nhận task từ Redis. Đồng thời Redis đóng vai trò là bộ đệm Caching siêu tốc cho các biểu đồ.

**Kiến trúc này giúp UI luôn mượt mà, các tác vụ nặng được đẩy sang Worker xử lý ngầm.**
---

## 3. Quản Lý Dependency: "Cái Bẫy" Của Official Image

**Vấn đề:** 
Official Image của Apache Superset (`apache/superset:6.1.0`) **KHÔNG** cài sẵn driver PostgreSQL (`psycopg2`) cho môi trường Production. Đáng chú ý, script khởi tạo nội bộ của Apache cố tình bỏ qua việc cài đặt driver này cho các tiến trình `worker`. Nếu không xử lý, Celery Worker sẽ lập tức crash khi cố gắng kết nối tới Data Warehouse (PostgreSQL) để chạy query nền.

**Giải pháp thông minh (Không cần build Custom Image):**
Thay vì viết một Dockerfile mới (làm tăng thời gian deploy và bảo trì), chúng ta tận dụng cơ chế Override của Apache:
- Khai báo `psycopg2-binary==2.9.9` trong file `requirements-local.txt`.
- Dùng Docker Compose mount file này vào đúng đường dẫn `/app/docker/requirements-local.txt` bên trong container.
- Khi container khởi chạy, script `docker-bootstrap.sh` sẽ tự động phát hiện file này và cài đặt driver `psycopg2-binary==2.9.9` cho **TẤT CẢ** các containers một cách an toàn và nhất quán.

---

## 4. Quản Lý Cấu Hình & Bảo Mật

### Single Source of Truth
File `superset_config.py` làm nhiệm vụ "bắc cầu" giữa các biến môi trường và ứng dụng. 

- Đọc các biến từ `.env` (được Docker Compose truyền vào).
- Cấu hình chuỗi kết nối `SQLALCHEMY_DATABASE_URI`.
- Cấu hình kết nối Redis cho Celery (`REDIS_CELERY_DB`) và Cache (`REDIS_RESULTS_DB`).
- Khai báo các tính năng mở rộng (`FEATURE_FLAGS`).

**Nguyên tắc:** KHÔNG hardcode bất kỳ credential nào (như `SUPERSET_SECRET_KEY` hay DB Password) vào trong file `superset_config.py`. Tất cả được quản lý tập trung ở file `.env` root.

### Phân Quyền (Role-Based Access Control - RBAC)
Superset không được cấp quyền `postgres` (Admin). Thay vào đó, nó kết nối qua một user bị giới hạn quyền hạn: `superset_user`.

Quyền hạn của user này được thiết lập chặt chẽ trong `src/scripts/init_dbs.sh`:
- **Chỉ có quyền CONNECT** tới `air_quality_db`.
- **Chỉ có quyền SELECT (Đọc)** trên duy nhất schema `gold_layer`.
- **Cơ chế Idempotency Vĩnh Cửu:** Thông qua lệnh `ALTER DEFAULT PRIVILEGES`, khi công cụ `dbt` tự động tạo ra một bảng mới vào ngày mai, `superset_user` vẫn TỰ ĐỘNG có quyền đọc bảng đó mà không cần Quản trị viên can thiệp thủ công.

---

## 5. Câu Hỏi Thường Gặp (FAQ)

**Q: Username và Password đăng nhập vào giao diện Superset là gì?**
> **A:** Mặc định username là `admin`. Password được cấu hình tại biến `SUPERSET_ADMIN_PASSWORD` trong file `.env` root của dự án. 
> *(Lý do username là `admin` vì script khởi tạo nội bộ `docker-init.sh` của Apache Superset đã hardcode giá trị này, không thể thay đổi thông qua `.env`).*

**Q: Tại sao thư mục này được đặt tên là `superset/` thay vì `docker/` như repo gốc của Apache?**
> **A:** Repo gốc của Apache đặt tên là `docker/` vì repo đó chỉ chứa duy nhất ứng dụng Superset. Tuy nhiên, kiến trúc của chúng ta là một **Data Platform Monorepo** bao gồm rất nhiều thành phần: Airflow, PostgreSQL, dbt-core (và tất cả đều chạy qua Docker). Việc đặt tên là `superset/` giúp duy trì tính đối xứng rành mạch (`airflow/`, `dbt-transform/`, `superset/`, `src/`) và ngăn chặn nhầm lẫn cho kỹ sư vận hành.

**Q: Làm sao để Xóa Sạch (Hard Reset) toàn bộ hệ thống và bắt đầu lại từ đầu?**
> **A:** Nếu bạn muốn dọn sạch Database và tải lại Image mới nhất (Clean Slate), hãy đứng ở thư mục gốc của dự án và chạy chuỗi lệnh sau:
> ```bash
> # 1. Tắt hệ thống và xóa toàn bộ Data Volumes (⚠️ MẤT DỮ LIỆU CŨ)
> docker compose down -v
> 
> # 2. Xóa toàn bộ Docker Images (buộc Docker tải lại bản mới nhất)
> docker rmi apache/superset:6.1.0 postgres:16-alpine redis:7
> 
> # 3. Khởi động và Build lại từ đầu
> docker compose up -d --build
> ```

---

## 6. Hướng Dẫn Khai Thác & Trực Quan Hóa

File README này tập trung giải thích các quyết định về **Kiến trúc Hạ tầng (Infrastructure)**. 

Sau khi hệ thống Superset đã khởi chạy thành công, để biết cách kết nối Database, sửa lỗi lệch múi giờ (Timezone) và vẽ các biểu đồ phân tích đúng chuẩn (tránh các lỗi dùng sai hàm `SUM`/`AVG`), vui lòng chuyển sang đọc tài liệu nghiệp vụ Data Analyst tại:
👉 **[Sổ tay Khai thác Dữ liệu & Trực quan hóa (Superset Playbook)](../docs/superset_visualization_guide.md)**
