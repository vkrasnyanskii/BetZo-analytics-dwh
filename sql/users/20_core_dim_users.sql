-- База CORE (на всякий)
CREATE DATABASE IF NOT EXISTS betzo_core ON CLUSTER 'projectk-analytics';

-- Локальная SCD-1 размерность
CREATE TABLE IF NOT EXISTS betzo_core.dim_users_shard ON CLUSTER 'projectk-analytics'
(
    user_id Int32,
    email String,
    first_name String,
    surname String,
    username String,

    created_at       DateTime,
    last_activity_at DateTime,
    last_activity_raw Int32,

    date_of_birth Date,
    gender LowCardinality(String),
    language LowCardinality(String),
    currency LowCardinality(String),
    verification_status LowCardinality(String),
    status LowCardinality(String),
    status_id Int32,

    is_banned Bool,
    is_fake   Bool,
    is_suspicious Bool,
    is_demo Bool,
    is_internal_test Bool,

    phone_number String,
    additional_phone String,
    ip_address String,
    host String,
    public_id String,
    referral_code String,
    registration_bonus String,
    brand LowCardinality(String),
    google_id String,
    telegram_id Int64,
    line_id String,
    steam_id String,
    avatar Int32,

    balance_live_real Int64,
    balance_bonus_casino Int64,
    balance_bonus_sport Int64,
    balance_live_helicopter Int64,
    balance_bilcoin Int64,
    balance_bilcoin_potential Int64,
    wager_deposits Int64,
    wager_buffer Int64,
    wager_helicopter Int64,

    betting_preference Float64,
    casino_preference  Float64,

    note String,
    _peerdb_synced_at DateTime64(9),
    _peerdb_version   Int64
)
ENGINE = ReplacingMergeTree(last_activity_raw)
PARTITION BY toYYYYMM(last_activity_at)
ORDER BY user_id;

-- Distributed-обёртка
CREATE TABLE IF NOT EXISTS betzo_core.dim_users ON CLUSTER 'projectk-analytics'
AS betzo_core.dim_users_shard
ENGINE = Distributed('projectk-analytics', 'betzo_core', 'dim_users_shard', cityHash64(user_id));

-- MV из STAGING → CORE (stream)
CREATE MATERIALIZED VIEW IF NOT EXISTS betzo_core.mv_dim_users_from_staging ON CLUSTER 'projectk-analytics'
TO betzo_core.dim_users_shard AS
SELECT
    id AS user_id,
    email,
    first_name,
    surname,
    username,

    date_of_registration_dt AS created_at,
    latest_activity_dt      AS last_activity_at,
    version_latest_activity AS last_activity_raw,

    date_of_birth,
    gender,
    language,
    currency,
    verification_status,
    status,
    status_id,

    is_banned,
    is_fake,
    is_suspicious,
    is_demo,
    is_internal_test,

    phone_number,
    additional_phone,
    ip_address,
    host,
    public_id,
    referral_code,
    registration_bonus,
    brand,
    google_id,
    telegram_id,
    line_id,
    steam_id,
    avatar,

    balance_live_real,
    balance_bonus_casino,
    balance_bonus_sport,
    balance_live_helicopter,
    balance_bilcoin,
    balance_bilcoin_potential,
    wager_deposits,
    wager_buffer,
    wager_helicopter,

    betting_preference,
    casino_preference,

    note,
    _peerdb_synced_at,
    _peerdb_version
FROM betzo_staging.users_latest_shard;

-- (опционально) Первичная заливка снапшотом
INSERT INTO betzo_core.dim_users
SELECT
    id AS user_id,
    email,
    first_name,
    surname,
    username,

    date_of_registration_dt AS created_at,
    latest_activity_dt      AS last_activity_at,
    version_latest_activity AS last_activity_raw,

    date_of_birth,
    gender,
    language,
    currency,
    verification_status,
    status,
    status_id,

    is_banned,
    is_fake,
    is_suspicious,
    is_demo,
    is_internal_test,

    phone_number,
    additional_phone,
    ip_address,
    host,
    public_id,
    referral_code,
    registration_bonus,
    brand,
    google_id,
    telegram_id,
    line_id,
    steam_id,
    avatar,

    balance_live_real,
    balance_bonus_casino,
    balance_bonus_sport,
    balance_live_helicopter,
    balance_bilcoin,
    balance_bilcoin_potential,
    wager_deposits,
    wager_buffer,
    wager_helicopter,

    betting_preference,
    casino_preference,

    note,
    _peerdb_synced_at,
    _peerdb_version
FROM betzo_staging.users_latest FINAL;
