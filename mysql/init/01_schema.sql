USE azki;

CREATE TABLE IF NOT EXISTS users (
    user_id     INT          NOT NULL,
    signup_date DATE         NOT NULL,
    city        VARCHAR(64)  NOT NULL,
    device_type VARCHAR(32)  NOT NULL,
    PRIMARY KEY (user_id),
    INDEX idx_city (city),
    INDEX idx_signup_date (signup_date)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'azki_reader'@'%' IDENTIFIED BY 'azki_reader_pw';
GRANT SELECT ON azki.* TO 'azki_reader'@'%';
FLUSH PRIVILEGES;
