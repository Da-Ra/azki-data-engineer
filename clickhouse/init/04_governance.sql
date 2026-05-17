ALTER TABLE azki.raw_user_events
    ADD INDEX IF NOT EXISTS idx_user_id    user_id    TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE azki.raw_user_events
    ADD INDEX IF NOT EXISTS idx_event_type event_type TYPE set(8)             GRANULARITY 4;

ALTER TABLE azki.raw_user_events MATERIALIZE INDEX idx_user_id;
ALTER TABLE azki.raw_user_events MATERIALIZE INDEX idx_event_type;


ALTER TABLE azki.purchase_enriched_denorm
    ADD PROJECTION IF NOT EXISTS p_by_product (
        SELECT *
        ORDER BY (product_type, event_time)
    );

ALTER TABLE azki.purchase_enriched_denorm MATERIALIZE PROJECTION p_by_product;

-- ========================================================================
-- Role-based access control
-- ========================================================================

CREATE ROLE IF NOT EXISTS azki_admin;
CREATE ROLE IF NOT EXISTS azki_analyst;
CREATE ROLE IF NOT EXISTS azki_service;

GRANT ALL ON azki.* TO azki_admin;

GRANT SELECT, INSERT ON azki.* TO azki_service;


GRANT SELECT ON azki.raw_user_events          TO azki_analyst;
GRANT SELECT ON azki.purchase_enriched_denorm TO azki_analyst;
GRANT SELECT ON azki.revenue_rollup_hourly    TO azki_analyst;


REVOKE SELECT(payment_method, financial_id, installments_count)
    ON azki.purchase_enriched_denorm
    FROM azki_analyst;



CREATE QUOTA IF NOT EXISTS azki_analyst_quota
    FOR INTERVAL 1 hour MAX queries = 200, errors = 20, result_rows = 100000000
    TO azki_analyst;



CREATE USER IF NOT EXISTS alice
    IDENTIFIED WITH sha256_password BY 'change_me_in_production'
    DEFAULT ROLE azki_analyst;

GRANT azki_analyst TO alice;
