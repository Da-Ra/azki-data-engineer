-- =============================================================
-- third_order
-- =============================================================
CREATE TABLE IF NOT EXISTS azki.third_order_queue
(
    order_id              UInt64,
    user_id               Int32,
    session_id            String,
    event_time            Int64,
    vehicle_type          String,
    vehicle_usage         String,
    no_claim_discount_pct Int32,
    coverage_amount       Int64,
    insurer               String,
    created_at            Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'mysql.azki.third_order',
         kafka_group_name  = 'ch_third_order_consumer',
         kafka_format      = 'JSONEachRow',
         kafka_max_block_size = 1000,
         kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS azki.third_order_mirror
(
    order_id              UInt64,
    user_id               Int32,
    session_id            String,
    event_time            DateTime,
    vehicle_type          LowCardinality(String),
    vehicle_usage         LowCardinality(String),
    no_claim_discount_pct UInt8,
    coverage_amount       UInt64,
    insurer               LowCardinality(String),
    created_at            DateTime,
    _ingested_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY order_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS azki.third_order_mv
TO azki.third_order_mirror AS
SELECT
    order_id,
    user_id,
    session_id,
    fromUnixTimestamp64Milli(event_time) AS event_time,
    vehicle_type,
    vehicle_usage,
    no_claim_discount_pct,
    coverage_amount,
    insurer,
    fromUnixTimestamp64Milli(created_at) AS created_at
FROM azki.third_order_queue;

-- =============================================================
-- body_order
-- =============================================================
CREATE TABLE IF NOT EXISTS azki.body_order_queue
(
    order_id                   UInt64,
    user_id                    Int32,
    session_id                 String,
    event_time                 Int64,
    vehicle_make               String,
    vehicle_model              String,
    vehicle_year               Int32,
    coverage_amount            Int64,
    has_natural_disaster_cover Int32,
    insurer                    String,
    created_at                 Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'mysql.azki.body_order',
         kafka_group_name  = 'ch_body_order_consumer',
         kafka_format      = 'JSONEachRow',
         kafka_max_block_size = 1000,
         kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS azki.body_order_mirror
(
    order_id                   UInt64,
    user_id                    Int32,
    session_id                 String,
    event_time                 DateTime,
    vehicle_make               LowCardinality(String),
    vehicle_model              LowCardinality(String),
    vehicle_year               UInt16,
    coverage_amount            UInt64,
    has_natural_disaster_cover UInt8,
    insurer                    LowCardinality(String),
    created_at                 DateTime,
    _ingested_at               DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY order_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS azki.body_order_mv
TO azki.body_order_mirror AS
SELECT
    order_id,
    user_id,
    session_id,
    fromUnixTimestamp64Milli(event_time) AS event_time,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    coverage_amount,
    has_natural_disaster_cover,
    insurer,
    fromUnixTimestamp64Milli(created_at) AS created_at
FROM azki.body_order_queue;

-- =============================================================
-- medical_order
-- =============================================================
CREATE TABLE IF NOT EXISTS azki.medical_order_queue
(
    order_id     UInt64,
    user_id      Int32,
    session_id   String,
    event_time   Int64,
    plan_tier    String,
    num_insured  Int32,
    has_dental   Int32,
    has_vision   Int32,
    annual_limit Int64,
    insurer      String,
    created_at   Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'mysql.azki.medical_order',
         kafka_group_name  = 'ch_medical_order_consumer',
         kafka_format      = 'JSONEachRow',
         kafka_max_block_size = 1000,
         kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS azki.medical_order_mirror
(
    order_id     UInt64,
    user_id      Int32,
    session_id   String,
    event_time   DateTime,
    plan_tier    LowCardinality(String),
    num_insured  UInt8,
    has_dental   UInt8,
    has_vision   UInt8,
    annual_limit UInt64,
    insurer      LowCardinality(String),
    created_at   DateTime,
    _ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY order_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS azki.medical_order_mv
TO azki.medical_order_mirror AS
SELECT
    order_id,
    user_id,
    session_id,
    fromUnixTimestamp64Milli(event_time) AS event_time,
    plan_tier,
    num_insured,
    has_dental,
    has_vision,
    annual_limit,
    insurer,
    fromUnixTimestamp64Milli(created_at) AS created_at
FROM azki.medical_order_queue;

-- =============================================================
-- fire_order
-- =============================================================
CREATE TABLE IF NOT EXISTS azki.fire_order_queue
(
    order_id         UInt64,
    user_id          Int32,
    session_id       String,
    event_time       Int64,
    property_type    String,
    property_area_m2 Int32,
    building_value   Int64,
    contents_value   Int64,
    insurer          String,
    created_at       Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'mysql.azki.fire_order',
         kafka_group_name  = 'ch_fire_order_consumer',
         kafka_format      = 'JSONEachRow',
         kafka_max_block_size = 1000,
         kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS azki.fire_order_mirror
(
    order_id         UInt64,
    user_id          Int32,
    session_id       String,
    event_time       DateTime,
    property_type    LowCardinality(String),
    property_area_m2 UInt32,
    building_value   UInt64,
    contents_value   UInt64,
    insurer          LowCardinality(String),
    created_at       DateTime,
    _ingested_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY order_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS azki.fire_order_mv
TO azki.fire_order_mirror AS
SELECT
    order_id,
    user_id,
    session_id,
    fromUnixTimestamp64Milli(event_time) AS event_time,
    property_type,
    property_area_m2,
    building_value,
    contents_value,
    insurer,
    fromUnixTimestamp64Milli(created_at) AS created_at
FROM azki.fire_order_queue;

-- =============================================================
-- financial_order
-- =============================================================
CREATE TABLE IF NOT EXISTS azki.financial_order_queue
(
    financial_id       UInt64,
    order_id           UInt64,
    product_type       String,
    user_id            Int32,
    session_id         String,
    event_time         Int64,
    premium_amount     Int64,
    discount_amount    Int64,
    tax_amount         Int64,
    final_amount       Int64,
    payment_method     String,
    payment_status     String,
    installments_count Int32,
    created_at         Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'mysql.azki.financial_order',
         kafka_group_name  = 'ch_financial_order_consumer',
         kafka_format      = 'JSONEachRow',
         kafka_max_block_size = 1000,
         kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS azki.financial_order_mirror
(
    financial_id       UInt64,
    order_id           UInt64,
    product_type       LowCardinality(String),
    user_id            Int32,
    session_id         String,
    event_time         DateTime,
    premium_amount     UInt64,
    discount_amount    UInt64,
    tax_amount         UInt64,
    final_amount       UInt64,
    payment_method     LowCardinality(String),
    payment_status     LowCardinality(String),
    installments_count UInt8,
    created_at         DateTime,
    _ingested_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY (order_id, product_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS azki.financial_order_mv
TO azki.financial_order_mirror AS
SELECT
    financial_id,
    order_id,
    product_type,
    user_id,
    session_id,
    fromUnixTimestamp64Milli(event_time) AS event_time,
    premium_amount,
    discount_amount,
    tax_amount,
    final_amount,
    payment_method,
    payment_status,
    installments_count,
    fromUnixTimestamp64Milli(created_at) AS created_at
FROM azki.financial_order_queue;
