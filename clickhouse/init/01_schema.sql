CREATE DATABASE IF NOT EXISTS azki;


CREATE TABLE IF NOT EXISTS azki.raw_user_events
(
    event_time     DateTime,
    user_id        Int32,
    session_id     String,
    event_type     LowCardinality(String),
    channel        LowCardinality(String),
    premium_amount Int64,
    city           LowCardinality(String),
    device_type    LowCardinality(String),
    signup_date    Date,
    event_hash     FixedString(40),
    ingested_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, event_hash);


CREATE TABLE IF NOT EXISTS azki.revenue_rollup_hourly
(
    window_start   DateTime,
    channel        LowCardinality(String),
    city           LowCardinality(String),
    purchase_count UInt32,
    total_premium  UInt64,
    avg_premium    Float64,
    updated_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, channel, city);
