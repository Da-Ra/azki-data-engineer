USE azki;

CREATE TABLE IF NOT EXISTS third_order (
    order_id              BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id               INT          NOT NULL,
    session_id            VARCHAR(64)  NOT NULL,
    event_time            DATETIME     NOT NULL,
    vehicle_type          VARCHAR(20)  NOT NULL,
    vehicle_usage         VARCHAR(20)  NOT NULL,
    no_claim_discount_pct TINYINT      NOT NULL,
    coverage_amount       BIGINT       NOT NULL,
    insurer               VARCHAR(64)  NOT NULL,
    created_at            DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_event (user_id, session_id, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS body_order (
    order_id                   BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id                    INT          NOT NULL,
    session_id                 VARCHAR(64)  NOT NULL,
    event_time                 DATETIME     NOT NULL,
    vehicle_make               VARCHAR(32)  NOT NULL,
    vehicle_model              VARCHAR(32)  NOT NULL,
    vehicle_year               SMALLINT     NOT NULL,
    coverage_amount            BIGINT       NOT NULL,
    has_natural_disaster_cover TINYINT      NOT NULL,
    insurer                    VARCHAR(64)  NOT NULL,
    created_at                 DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_event (user_id, session_id, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS medical_order (
    order_id     BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      INT          NOT NULL,
    session_id   VARCHAR(64)  NOT NULL,
    event_time   DATETIME     NOT NULL,
    plan_tier    VARCHAR(16)  NOT NULL,
    num_insured  TINYINT      NOT NULL,
    has_dental   TINYINT      NOT NULL,
    has_vision   TINYINT      NOT NULL,
    annual_limit BIGINT       NOT NULL,
    insurer      VARCHAR(64)  NOT NULL,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_event (user_id, session_id, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS fire_order (
    order_id         BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id          INT          NOT NULL,
    session_id       VARCHAR(64)  NOT NULL,
    event_time       DATETIME     NOT NULL,
    property_type    VARCHAR(20)  NOT NULL,
    property_area_m2 INT          NOT NULL,
    building_value   BIGINT       NOT NULL,
    contents_value   BIGINT       NOT NULL,
    insurer          VARCHAR(64)  NOT NULL,
    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_event (user_id, session_id, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS financial_order (
    financial_id       BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id           BIGINT       NOT NULL,
    product_type       VARCHAR(16)  NOT NULL,
    user_id            INT          NOT NULL,
    session_id         VARCHAR(64)  NOT NULL,
    event_time         DATETIME     NOT NULL,
    premium_amount     BIGINT       NOT NULL,
    discount_amount    BIGINT       NOT NULL DEFAULT 0,
    tax_amount         BIGINT       NOT NULL DEFAULT 0,
    final_amount       BIGINT       NOT NULL,
    payment_method     VARCHAR(32)  NOT NULL,
    payment_status     VARCHAR(16)  NOT NULL DEFAULT 'paid',
    installments_count TINYINT      NOT NULL DEFAULT 1,
    created_at         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_order_product (order_id, product_type),
    INDEX idx_event         (user_id, session_id, event_time),
    INDEX idx_event_time    (event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

GRANT SELECT ON azki.* TO 'azki_reader'@'%';
FLUSH PRIVILEGES;
