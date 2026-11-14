{% macro get_incremental_forecast_freshness_sql(args) %}
    {%- set target = args['target_relation'] -%}
    {%- set source = args['temp_relation'] -%}
    {%- set keys = args['unique_key'] -%}
    {%- set columns = args['dest_columns'] | map(attribute='quoted') | list -%}
    INSERT INTO {{ target }} AS current_row ({{ columns | join(', ') }})
    SELECT {{ columns | join(', ') }}
    FROM {{ source }}
    ON CONFLICT ({{ keys | join(', ') }})
    DO UPDATE SET
        {% for column in args['dest_columns'] if column.name not in keys -%}
        {{ column.quoted }} = EXCLUDED.{{ column.quoted }}{{ ',' if not loop.last }}
        {% endfor -%}
    WHERE CASE
        -- A later archive import must not replace an existing forecast.
        WHEN current_row.data_source IS NOT NULL
         AND current_row.data_source <> EXCLUDED.data_source
            THEN EXCLUDED.data_source = 'forecast'
        -- Existing Silver rows may have been built before provenance was recorded.
        WHEN current_row.data_source IS NULL AND EXCLUDED.data_source = 'historical'
            THEN EXCLUDED.raw_id = current_row.raw_id
             AND EXCLUDED.execution_date = current_row.execution_date
        ELSE EXCLUDED.execution_date > current_row.execution_date
          OR (EXCLUDED.execution_date = current_row.execution_date
              AND (current_row.ingested_at IS NULL OR EXCLUDED.ingested_at >= current_row.ingested_at))
    END
{% endmacro %}

{% macro latest_forecast_rows(forecast_model, historical_model) %}
WITH candidates AS (
    SELECT * FROM {{ ref(forecast_model) }}
    UNION ALL
    SELECT * FROM {{ ref(historical_model) }}
)
SELECT DISTINCT ON (forecast_time, latitude, longitude) *
FROM candidates
{% if is_incremental() %}
    {% if var('execution_date', none) %}
    WHERE execution_date = '{{ var("execution_date") | replace("'", "''") }}'::timestamptz
    {% else %}
    WHERE execution_date >= (SELECT COALESCE(max(execution_date), '-infinity'::timestamptz) FROM {{ this }})
    {% endif %}
{% endif %}
ORDER BY forecast_time, latitude, longitude, (data_source = 'forecast') DESC,
         execution_date DESC, ingested_at DESC, raw_id DESC
{% endmacro %}
