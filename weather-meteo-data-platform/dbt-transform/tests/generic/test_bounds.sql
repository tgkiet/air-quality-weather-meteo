{% test bounds(model, column_name, min_value, max_value) %}
-- Test biên độ giá trị (Bounds Test) để kiểm soát chất lượng dữ liệu thô
-- Fail-fast nếu toạ độ nằm ngoài giới hạn hợp lệ
select *
from {{ model }}
where {{ column_name }} < {{ min_value }} 
   or {{ column_name }} > {{ max_value }}
{% endtest %}
