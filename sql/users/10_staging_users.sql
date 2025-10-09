-- База стейджинга (на всякий)
CREATE DATABASE IF NOT EXISTS betzo_staging ON CLUSTER 'projectk-analytics';

-- Локальная таблица с актуальными пользователями (SCD-1 по latest_activity)
CREATE TABLE IF NOT EXISTS betzo_staging.users_latest_shard ON CLUSTER 'projectk-analytics'
(
    id Int32,
    email String,
    first_name String,

    date_of_registration_dt DateTime,
    latest_activity_dt      DateTime,
    date_of_birth           Date,

    currency LowCardinality(String),
    status   LowCardinality(String),

    balance_live_real        Int64,
    hold                     Int64,
    latest_reset_password_dt DateTime,

    is_fake Bool,
    note String,
    is_banned Bool,
    gender LowCardinality(String),

    phone_number String,
    ip_address  String,

    wager_deposits  Int64,
    wager_buffer    Int64,
    referral_code   String,
    status_id       Int32,
    is_suspicious   Bool,

    balance_bonus_casino     Int64,
    balance_bonus_sport      Int64,
    balance_live_helicopter  Int64,
    wager_helicopter         Int64,

    language            LowCardinality(String),
    verification_status LowCardinality(String),

    surname String,
    host String,
    additional_phone String,
    public_id String,
    registration_bonus String,

    betting_preference Float64,
    casino_preference  Float64,

    latest_activity_at_dt DateTime,

    avatar Int32,

    balance_bilcoin            Int64,
    balance_bilcoin_potential  Int64,

    telegram_id Int64,
    username String,
    brand LowCardinality(String),

    google_id String,

    is_internal_test Bool,
    is_demo          Bool,

    line_id  String,
    steam_id String,

    _peerdb_synced_at DateTime64(9),
    _peerdb_version   Int64,

    -- версия для ReplacingMergeTree: нормализованный latest_activity (sec)
    version_latest_activity Int32
)
ENGINE = ReplacingMergeTree(version_latest_activity)
PARTITION BY toYYYYMM(latest_activity_dt)
ORDER BY id;

-- Distributed-обёртка
CREATE TABLE IF NOT EXISTS betzo_staging.users_latest ON CLUSTER 'projectk-analytics'
AS betzo_staging.users_latest_shard
ENGINE = Distributed('projectk-analytics', 'betzo_staging', 'users_latest_shard', cityHash64(id));

-- MV из RAW → STAGING с приведение времени/типов
CREATE MATERIALIZED VIEW IF NOT EXISTS betzo_staging.mv_users_latest ON CLUSTER 'projectk-analytics'
TO betzo_staging.users_latest_shard AS
SELECT
    id, email, first_name,

    toDateTime(IF(date_of_registration > 2000000000, intDiv(date_of_registration, 1000), date_of_registration), 'UTC') AS date_of_registration_dt,
    toDateTime(IF(latest_activity     > 2000000000, intDiv(latest_activity,     1000), latest_activity    ), 'UTC') AS latest_activity_dt,
    toDate(date_of_birth) AS date_of_birth,

    currency, status,

    balance_live_real,
    hold,
    toDateTime(IF(latest_reset_password > 2000000000, intDiv(latest_reset_password, 1000), latest_reset_password), 'UTC') AS latest_reset_password_dt,

    is_fake, note, is_banned, gender,
    phone_number, ip_address,
    wager_deposits, wager_buffer, referral_code, status_id, is_suspicious,

    balance_bonus_casino, balance_bonus_sport, balance_live_helicopter, wager_helicopter,

    language, verification_status,
    surname, host, additional_phone, public_id, registration_bonus,

    betting_preference, casino_preference,

    toDateTime(IF(latest_activity_at > 2000000000, intDiv(latest_activity_at, 1000), latest_activity_at), 'UTC') AS latest_activity_at_dt,

    avatar,
    balance_bilcoin, balance_bilcoin_potential,

    telegram_id, username, brand,
    google_id,
    is_internal_test, is_demo,
    line_id, steam_id,

    _peerdb_synced_at,
    _peerdb_version,

    -- версия для SCD-1
    IF(latest_activity > 2000000000, intDiv(latest_activity, 1000), latest_activity) AS version_latest_activity
FROM betzo_prod.public_users_shard
WHERE _peerdb_is_deleted = 0;

-- Вью с фильтром “eligible” (для DQ/DM)
CREATE OR REPLACE VIEW betzo_staging.v_users_eligible ON CLUSTER 'projectk-analytics' AS
SELECT *
FROM betzo_staging.users_latest
WHERE NOT is_demo
  AND NOT is_internal_test
  AND status != 'UNVERIFIED';
