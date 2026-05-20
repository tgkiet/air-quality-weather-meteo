{# 
    MACRO: generate_schema_name
    ===========================
    MỤC ĐÍCH:
        Ghi đè hành vi mặc định của dbt khi tạo tên schema.
    
    HÀNH VI MẶC ĐỊNH CỦA DBT (mà chúng ta KHÔNG muốn):
        Schema = default_schema + "_" + custom_schema
               = "silver_layer" + "_" + "gold_layer"
               = "silver_layer_gold_layer"  ← Xấu!
    
    HÀNH VI SAU KHI OVERRIDE (mà chúng ta MUỐN):
        - Không có custom_schema → dùng default ("silver_layer")
        - Có custom_schema       → dùng đúng tên đó ("gold_layer")
    
    TÀI LIỆU THAM KHẢO:
        https://docs.getdbt.com/docs/build/custom-schemas
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {# Không có schema riêng → dùng schema mặc định từ profiles.yml #}
        {{ default_schema }}

    {%- else -%}
        {# Có schema riêng (ví dụ: gold_layer) → dùng đúng tên đó, bỏ qua default #}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
