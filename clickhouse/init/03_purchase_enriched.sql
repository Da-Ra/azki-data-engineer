CREATE TABLE IF NOT EXISTS azki.purchase_enriched_denorm
(
    -- Event fields (from raw_user_events)
    event_time              DateTime,
    user_id                 Int32,
    session_id              String,
    channel                 LowCardinality(String),
    premium_amount          Int64,
    event_hash              FixedString(40),

    city                    LowCardinality(String),
    device_type             LowCardinality(String),
    signup_date             Date,

    order_id                UInt64,
    product_type            LowCardinality(String),
    financial_id            UInt64,
    discount_amount         UInt64,
    tax_amount              UInt64,
    final_amount            UInt64,
    payment_method          LowCardinality(String),
    payment_status          LowCardinality(String),
    installments_count      UInt8,
    insurer                 LowCardinality(String),

    third_vehicle_type              LowCardinality(String) DEFAULT '',
    third_vehicle_usage             LowCardinality(String) DEFAULT '',
    third_no_claim_discount_pct     UInt8                  DEFAULT 0,
    third_coverage_amount           UInt64                 DEFAULT 0,

    body_vehicle_make               LowCardinality(String) DEFAULT '',
    body_vehicle_model              LowCardinality(String) DEFAULT '',
    body_vehicle_year               UInt16                 DEFAULT 0,
    body_coverage_amount            UInt64                 DEFAULT 0,
    body_has_natural_disaster_cover UInt8                  DEFAULT 0,

    medical_plan_tier               LowCardinality(String) DEFAULT '',
    medical_num_insured             UInt8                  DEFAULT 0,
    medical_has_dental              UInt8                  DEFAULT 0,
    medical_has_vision              UInt8                  DEFAULT 0,
    medical_annual_limit            UInt64                 DEFAULT 0,

    fire_property_type              LowCardinality(String) DEFAULT '',
    fire_property_area_m2           UInt32                 DEFAULT 0,
    fire_building_value             UInt64                 DEFAULT 0,
    fire_contents_value             UInt64                 DEFAULT 0,

    _ingested_at            DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, event_hash);



CREATE MATERIALIZED VIEW IF NOT EXISTS azki.purchase_enrichment_mv
TO azki.purchase_enriched_denorm AS
SELECT
    e.event_time     AS event_time,
    e.user_id        AS user_id,
    e.session_id     AS session_id,
    e.channel        AS channel,
    e.premium_amount AS premium_amount,
    e.event_hash     AS event_hash,
    e.city           AS city,
    e.device_type    AS device_type,
    e.signup_date    AS signup_date,

    f.order_id            AS order_id,
    f.product_type        AS product_type,
    f.financial_id        AS financial_id,
    f.discount_amount     AS discount_amount,
    f.tax_amount          AS tax_amount,
    f.final_amount        AS final_amount,
    f.payment_method      AS payment_method,
    f.payment_status      AS payment_status,
    f.installments_count  AS installments_count,

    coalesce(t.insurer, b.insurer, m.insurer, fi.insurer)         AS insurer,

    coalesce(t.vehicle_type, '')                                  AS third_vehicle_type,
    coalesce(t.vehicle_usage, '')                                 AS third_vehicle_usage,
    coalesce(t.no_claim_discount_pct, 0)                          AS third_no_claim_discount_pct,
    coalesce(t.coverage_amount, 0)                                AS third_coverage_amount,

    coalesce(b.vehicle_make, '')                                  AS body_vehicle_make,
    coalesce(b.vehicle_model, '')                                 AS body_vehicle_model,
    coalesce(b.vehicle_year, 0)                                   AS body_vehicle_year,
    coalesce(b.coverage_amount, 0)                                AS body_coverage_amount,
    coalesce(b.has_natural_disaster_cover, 0)                     AS body_has_natural_disaster_cover,

    coalesce(m.plan_tier, '')                                     AS medical_plan_tier,
    coalesce(m.num_insured, 0)                                    AS medical_num_insured,
    coalesce(m.has_dental, 0)                                     AS medical_has_dental,
    coalesce(m.has_vision, 0)                                     AS medical_has_vision,
    coalesce(m.annual_limit, 0)                                   AS medical_annual_limit,

    coalesce(fi.property_type, '')                                AS fire_property_type,
    coalesce(fi.property_area_m2, 0)                              AS fire_property_area_m2,
    coalesce(fi.building_value, 0)                                AS fire_building_value,
    coalesce(fi.contents_value, 0)                                AS fire_contents_value
FROM azki.raw_user_events AS e
INNER JOIN azki.financial_order_mirror AS f
        ON e.user_id    = f.user_id
       AND e.session_id = f.session_id
LEFT JOIN  azki.third_order_mirror   AS t  ON f.order_id = t.order_id  AND f.product_type = 'third'
LEFT JOIN  azki.body_order_mirror    AS b  ON f.order_id = b.order_id  AND f.product_type = 'body'
LEFT JOIN  azki.medical_order_mirror AS m  ON f.order_id = m.order_id  AND f.product_type = 'medical'
LEFT JOIN  azki.fire_order_mirror    AS fi ON f.order_id = fi.order_id AND f.product_type = 'fire'
WHERE e.event_type = 'purchase';


CREATE MATERIALIZED VIEW IF NOT EXISTS azki.purchase_enrichment_from_financial_mv
TO azki.purchase_enriched_denorm AS
SELECT
    e.event_time     AS event_time,
    e.user_id        AS user_id,
    e.session_id     AS session_id,
    e.channel        AS channel,
    e.premium_amount AS premium_amount,
    e.event_hash     AS event_hash,
    e.city           AS city,
    e.device_type    AS device_type,
    e.signup_date    AS signup_date,

    f.order_id            AS order_id,
    f.product_type        AS product_type,
    f.financial_id        AS financial_id,
    f.discount_amount     AS discount_amount,
    f.tax_amount          AS tax_amount,
    f.final_amount        AS final_amount,
    f.payment_method      AS payment_method,
    f.payment_status      AS payment_status,
    f.installments_count  AS installments_count,

    coalesce(t.insurer, b.insurer, m.insurer, fi.insurer)         AS insurer,

    coalesce(t.vehicle_type, '')                                  AS third_vehicle_type,
    coalesce(t.vehicle_usage, '')                                 AS third_vehicle_usage,
    coalesce(t.no_claim_discount_pct, 0)                          AS third_no_claim_discount_pct,
    coalesce(t.coverage_amount, 0)                                AS third_coverage_amount,

    coalesce(b.vehicle_make, '')                                  AS body_vehicle_make,
    coalesce(b.vehicle_model, '')                                 AS body_vehicle_model,
    coalesce(b.vehicle_year, 0)                                   AS body_vehicle_year,
    coalesce(b.coverage_amount, 0)                                AS body_coverage_amount,
    coalesce(b.has_natural_disaster_cover, 0)                     AS body_has_natural_disaster_cover,

    coalesce(m.plan_tier, '')                                     AS medical_plan_tier,
    coalesce(m.num_insured, 0)                                    AS medical_num_insured,
    coalesce(m.has_dental, 0)                                     AS medical_has_dental,
    coalesce(m.has_vision, 0)                                     AS medical_has_vision,
    coalesce(m.annual_limit, 0)                                   AS medical_annual_limit,

    coalesce(fi.property_type, '')                                AS fire_property_type,
    coalesce(fi.property_area_m2, 0)                              AS fire_property_area_m2,
    coalesce(fi.building_value, 0)                                AS fire_building_value,
    coalesce(fi.contents_value, 0)                                AS fire_contents_value
FROM azki.financial_order_mirror AS f
INNER JOIN azki.raw_user_events AS e
        ON e.user_id    = f.user_id
       AND e.session_id = f.session_id
LEFT JOIN  azki.third_order_mirror   AS t  ON f.order_id = t.order_id  AND f.product_type = 'third'
LEFT JOIN  azki.body_order_mirror    AS b  ON f.order_id = b.order_id  AND f.product_type = 'body'
LEFT JOIN  azki.medical_order_mirror AS m  ON f.order_id = m.order_id  AND f.product_type = 'medical'
LEFT JOIN  azki.fire_order_mirror    AS fi ON f.order_id = fi.order_id AND f.product_type = 'fire'
WHERE e.event_type = 'purchase';


INSERT INTO azki.purchase_enriched_denorm
(
    event_time, user_id, session_id, channel, premium_amount, event_hash,
    city, device_type, signup_date,
    order_id, product_type, financial_id,
    discount_amount, tax_amount, final_amount,
    payment_method, payment_status, installments_count,
    insurer,
    third_vehicle_type, third_vehicle_usage, third_no_claim_discount_pct, third_coverage_amount,
    body_vehicle_make, body_vehicle_model, body_vehicle_year, body_coverage_amount, body_has_natural_disaster_cover,
    medical_plan_tier, medical_num_insured, medical_has_dental, medical_has_vision, medical_annual_limit,
    fire_property_type, fire_property_area_m2, fire_building_value, fire_contents_value
)
SELECT
    e.event_time     AS event_time,
    e.user_id        AS user_id,
    e.session_id     AS session_id,
    e.channel        AS channel,
    e.premium_amount AS premium_amount,
    e.event_hash     AS event_hash,
    e.city           AS city,
    e.device_type    AS device_type,
    e.signup_date    AS signup_date,

    f.order_id            AS order_id,
    f.product_type        AS product_type,
    f.financial_id        AS financial_id,
    f.discount_amount     AS discount_amount,
    f.tax_amount          AS tax_amount,
    f.final_amount        AS final_amount,
    f.payment_method      AS payment_method,
    f.payment_status      AS payment_status,
    f.installments_count  AS installments_count,

    coalesce(t.insurer, b.insurer, m.insurer, fi.insurer)         AS insurer,

    coalesce(t.vehicle_type, '')                                  AS third_vehicle_type,
    coalesce(t.vehicle_usage, '')                                 AS third_vehicle_usage,
    coalesce(t.no_claim_discount_pct, 0)                          AS third_no_claim_discount_pct,
    coalesce(t.coverage_amount, 0)                                AS third_coverage_amount,

    coalesce(b.vehicle_make, '')                                  AS body_vehicle_make,
    coalesce(b.vehicle_model, '')                                 AS body_vehicle_model,
    coalesce(b.vehicle_year, 0)                                   AS body_vehicle_year,
    coalesce(b.coverage_amount, 0)                                AS body_coverage_amount,
    coalesce(b.has_natural_disaster_cover, 0)                     AS body_has_natural_disaster_cover,

    coalesce(m.plan_tier, '')                                     AS medical_plan_tier,
    coalesce(m.num_insured, 0)                                    AS medical_num_insured,
    coalesce(m.has_dental, 0)                                     AS medical_has_dental,
    coalesce(m.has_vision, 0)                                     AS medical_has_vision,
    coalesce(m.annual_limit, 0)                                   AS medical_annual_limit,

    coalesce(fi.property_type, '')                                AS fire_property_type,
    coalesce(fi.property_area_m2, 0)                              AS fire_property_area_m2,
    coalesce(fi.building_value, 0)                                AS fire_building_value,
    coalesce(fi.contents_value, 0)                                AS fire_contents_value
FROM azki.raw_user_events AS e
INNER JOIN azki.financial_order_mirror AS f
        ON e.user_id    = f.user_id
       AND e.session_id = f.session_id
LEFT JOIN  azki.third_order_mirror   AS t  ON f.order_id = t.order_id  AND f.product_type = 'third'
LEFT JOIN  azki.body_order_mirror    AS b  ON f.order_id = b.order_id  AND f.product_type = 'body'
LEFT JOIN  azki.medical_order_mirror AS m  ON f.order_id = m.order_id  AND f.product_type = 'medical'
LEFT JOIN  azki.fire_order_mirror    AS fi ON f.order_id = fi.order_id AND f.product_type = 'fire'
WHERE e.event_type = 'purchase';
