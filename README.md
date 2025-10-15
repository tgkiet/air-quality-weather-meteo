# ATTENTION 
- Data output nằm trong folder Open-Meteo-Dataset. Dataset bao gồm 1 bộ data về air quality, và 1 bộ data về weather & meteo.
- Thời gian dataset: Từ ngày 02/8/2022 đến 10/10/2025. (Nhưng thực tế là dữ liệu air quality ở Open-Meteo chỉ có từ ngày 4/8/2022)
- Dữ liệu được crawl theo các location (31 location): Location này dựa vào toạ độ crawl được từ OpenAQ. Và data về location nằm ở file `stations_metadata.csv`
- 2 File data crawl về được và 1 file data gộp lại đã được gitignore do dung lượng quá lớn, nếu cần, hãy liên hệ tôi thông qua gmail:
giakiettran14102005@gmail.com


\Open-Meteo-Dataset\pipelineDataViaSupabase:
Folder này dùng để thực hiện logic tạo pipeline ETL với DBMS là supabase, và với tương tác như postgresql.

### CONTACT ME FOR DOCUMENTATION OF DATASET IN THIS PROJECT