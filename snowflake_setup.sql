-- ============================================================
-- Social Media & Crypto Intelligence Platform
-- Snowflake Setup Script
-- Run this entire file in your Snowflake worksheet
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ── Warehouse ─────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS AIRFLOW_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE WAREHOUSE AIRFLOW_WH;

-- ── Database ──────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS SOCIAL_ANALYTICS;
USE DATABASE SOCIAL_ANALYTICS;

-- ── BRONZE Schema ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS BRONZE;

CREATE TABLE IF NOT EXISTS BRONZE.RAW_SOCIAL_MEDIA_DATA (
    load_id        VARCHAR(255),
    source         VARCHAR(50),
    content_type   VARCHAR(50),
    raw_data       VARIANT,
    fetched_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS BRONZE.RAW_CRYPTO_STREAM (
    symbol         VARCHAR(20),
    price          FLOAT,
    volume         FLOAT,
    high           FLOAT,
    low            FLOAT,
    event_time     TIMESTAMP,
    loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS BRONZE.PIPELINE_RUNS (
    run_id          VARCHAR(255),
    dag_id          VARCHAR(255),
    execution_date  TIMESTAMP,
    records_fetched INT,
    records_loaded  INT,
    status          VARCHAR(50),
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP
);

-- ── SILVER Schema ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS SILVER;

CREATE TABLE IF NOT EXISTS SILVER.SOCIAL_POSTS_CLEAN (
    post_id        VARCHAR(255),
    source         VARCHAR(50),
    title          VARCHAR(1000),
    url            VARCHAR(2000),
    score          INT,
    author         VARCHAR(255),
    description    VARCHAR(2000),
    published_at   TIMESTAMP,
    loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SILVER.CRYPTO_PRICES_CLEAN (
    symbol         VARCHAR(20),
    price          FLOAT,
    volume         FLOAT,
    high           FLOAT,
    low            FLOAT,
    event_time     TIMESTAMP,
    loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ── GOLD Schema ───────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS GOLD;

CREATE TABLE IF NOT EXISTS GOLD.DAILY_TOP_STORIES (
    report_date    DATE,
    source         VARCHAR(50),
    title          VARCHAR(1000),
    url            VARCHAR(2000),
    score          INT,
    author         VARCHAR(255),
    rank           INT,
    loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS GOLD.DAILY_SOURCE_SUMMARY (
    report_date    DATE,
    source         VARCHAR(50),
    total_articles INT,
    avg_score      FLOAT,
    max_score      INT,
    top_story      VARCHAR(1000),
    loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS GOLD.CRYPTO_ANALYTICS (
    symbol         VARCHAR(20),
    minute_bucket  TIMESTAMP,
    open_price     FLOAT,
    high_price     FLOAT,
    low_price      FLOAT,
    close_price    FLOAT,
    avg_price      FLOAT,
    trade_count    INT,
    loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS GOLD.COMBINED_INSIGHTS (
    report_date      DATE,
    top_hn_story     VARCHAR(1000),
    hn_max_score     INT,
    total_articles   INT,
    btc_avg_price    FLOAT,
    eth_avg_price    FLOAT,
    sol_avg_price    FLOAT,
    btc_price_change FLOAT,
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ── Grant permissions ─────────────────────────────────────────
GRANT USAGE ON WAREHOUSE AIRFLOW_WH TO USER HIMABINDUT9715;
GRANT ALL PRIVILEGES ON DATABASE SOCIAL_ANALYTICS TO USER HIMABINDUT9715;
GRANT ALL PRIVILEGES ON SCHEMA SOCIAL_ANALYTICS.BRONZE TO USER HIMABINDUT9715;
GRANT ALL PRIVILEGES ON SCHEMA SOCIAL_ANALYTICS.SILVER TO USER HIMABINDUT9715;
GRANT ALL PRIVILEGES ON SCHEMA SOCIAL_ANALYTICS.GOLD TO USER HIMABINDUT9715;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA SOCIAL_ANALYTICS.BRONZE TO USER HIMABINDUT9715;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA SOCIAL_ANALYTICS.SILVER TO USER HIMABINDUT9715;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA SOCIAL_ANALYTICS.GOLD TO USER HIMABINDUT9715;

-- ── Verify ────────────────────────────────────────────────────
SHOW TABLES IN SCHEMA SOCIAL_ANALYTICS.BRONZE;
SHOW TABLES IN SCHEMA SOCIAL_ANALYTICS.SILVER;
SHOW TABLES IN SCHEMA SOCIAL_ANALYTICS.GOLD;
