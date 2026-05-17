CREATE TABLE IF NOT EXISTS azki.dq_results
(
    check_name   LowCardinality(String),
    check_type   LowCardinality(String),
    severity     LowCardinality(String),
    status       LowCardinality(String),
    value        Nullable(Float64),
    expected     String,
    message      String,
    duration_ms  Float32,
    ts           DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (check_name, ts)
TTL ts + INTERVAL 30 DAY;
