CREATE TABLE api_openmeteo_raw_data (
    id SERIAL PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL,
    raw_json JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);