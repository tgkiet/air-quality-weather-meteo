BEGIN;

CREATE TABLE IF NOT EXISTS silver_layer.bot_user_preferences (
    chat_id BIGINT PRIMARY KEY,
    language VARCHAR(2) NOT NULL DEFAULT 'en' CHECK (language IN ('en', 'vi')),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE silver_layer.bot_user_preferences
    ADD COLUMN IF NOT EXISTS home_location TEXT,
    ADD COLUMN IF NOT EXISTS daily_briefing BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS silver_layer.telegram_delivery (
    delivery_id TEXT PRIMARY KEY,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ
);

ALTER TABLE silver_layer.alert_history
    ADD COLUMN IF NOT EXISTS delivery_id TEXT REFERENCES silver_layer.telegram_delivery (delivery_id);

CREATE INDEX IF NOT EXISTS alert_history_delivery_id_idx
    ON silver_layer.alert_history (delivery_id)
    WHERE delivery_id IS NOT NULL;

COMMIT;
