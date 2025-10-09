-- View с филигранным фильтром (если ещё не создан)
CREATE OR REPLACE VIEW betzo_staging.v_users_eligible ON CLUSTER 'projectk-analytics' AS
SELECT *
FROM betzo_staging.users_latest
WHERE NOT is_demo
  AND NOT is_internal_test
  AND status != 'UNVERIFIED';

-- A) Общие метрики (вся выборка)
INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_count_vs_uniq_all','info','rows',toFloat64(count()),'-',NULL,1
FROM betzo_staging.users_latest FINAL;

INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_count_vs_uniq_all','info','uniq_ids',toFloat64(uniqExact(id)),'-',NULL,1
FROM betzo_staging.users_latest FINAL;

-- B) Eligible: строки и уникальные
INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_count_vs_uniq_eligible','info','rows_eligible',toFloat64(count()),'-',NULL,1
FROM betzo_staging.v_users_eligible;

INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_count_vs_uniq_eligible','info','uniq_ids_eligible',toFloat64(uniqExact(id)),'-',NULL,1
FROM betzo_staging.v_users_eligible;

-- C) Дубликаты (eligible)
INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_no_duplicates_eligible','error','dups',
       toFloat64(rows - uniq_ids),'eq',toFloat64(0), CAST(rows - uniq_ids = 0 AS UInt8)
FROM (
  SELECT count() AS rows, uniqExact(id) AS uniq_ids
  FROM betzo_staging.v_users_eligible
);

-- D) Свежесть: RAW vs now (SLA 7200s), STG vs now (info), GAP STG vs RAW (≤ 300s)
INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_raw_freshness_lag_sec','warn','raw_lag_sec',
       toFloat64(ifNull(dateDiff('second', (SELECT max(_peerdb_synced_at) FROM betzo_prod.public_users), now()), toInt64(1000000000))),
       'le', toFloat64(7200),
       CAST(ifNull(dateDiff('second', (SELECT max(_peerdb_synced_at) FROM betzo_prod.public_users), now()), toInt64(1000000000)) <= 7200 AS UInt8);

INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_freshness_lag_sec','info','stg_lag_sec',
       toFloat64(ifNull(dateDiff('second', max(_peerdb_synced_at), now()), toInt64(1000000000))),
       '-', NULL, 1
FROM betzo_staging.users_latest;

INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_pipeline_gap_sec','error','stg_vs_raw_gap_sec',
       toFloat64(abs(dateDiff('second',
         (SELECT max(_peerdb_synced_at) FROM betzo_prod.public_users),
         (SELECT max(_peerdb_synced_at) FROM betzo_staging.users_latest)))),
       'le', toFloat64(300),
       CAST(abs(dateDiff('second',
         (SELECT max(_peerdb_synced_at) FROM betzo_prod.public_users),
         (SELECT max(_peerdb_synced_at) FROM betzo_staging.users_latest))) <= 300 AS UInt8);

-- E) Email (eligible)
INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_email_present_eligible','error','empty_emails',
       toFloat64(countIf(email = '' OR email IS NULL)),
       'eq', toFloat64(0),
       CAST(countIf(email = '' OR email IS NULL) = 0 AS UInt8)
FROM betzo_staging.v_users_eligible;

INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_email_regex_eligible','warn','invalid_emails',
       toFloat64(countIf(email != '' AND email IS NOT NULL AND NOT match(email, '^[^@]+@[^@]+\\.[^@]+$'))),
       'eq', toFloat64(0),
       CAST(countIf(email != '' AND email IS NOT NULL AND NOT match(email, '^[^@]+@[^@]+\\.[^@]+$')) = 0 AS UInt8)
FROM betzo_staging.v_users_eligible;

-- F) IP формат (eligible)
INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_ip_format_eligible','warn','invalid_ip_rows',
       toFloat64(countIf(toIPv4OrNull(ip_address) IS NULL AND toIPv6OrNull(ip_address) IS NULL)),
       'eq', toFloat64(0),
       CAST(countIf(toIPv4OrNull(ip_address) IS NULL AND toIPv6OrNull(ip_address) IS NULL) = 0 AS UInt8)
FROM betzo_staging.v_users_eligible;

-- G) Будущие даты (eligible)
INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_no_future_dates_eligible','error','future_rows',
       toFloat64(countIf(latest_activity_dt > now() + INTERVAL 1 DAY
                      OR date_of_registration_dt > now() + INTERVAL 1 DAY)),
       'eq', toFloat64(0),
       CAST(countIf(latest_activity_dt > now() + INTERVAL 1 DAY
                 OR date_of_registration_dt > now() + INTERVAL 1 DAY) = 0 AS UInt8)
FROM betzo_staging.v_users_eligible;

-- H) Негативные балансы (eligible)
INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_negative_balances_eligible','warn','neg_balance_rows',
       toFloat64(countIf(least(balance_live_real,
                               balance_bonus_casino,
                               balance_bonus_sport,
                               balance_live_helicopter,
                               balance_bilcoin,
                               balance_bilcoin_potential) < 0)),
       'eq', toFloat64(0),
       CAST(countIf(least(balance_live_real,
                          balance_bonus_casino,
                          balance_bonus_sport,
                          balance_live_helicopter,
                          balance_bilcoin,
                          balance_bilcoin_potential) < 0) = 0 AS UInt8)
FROM betzo_staging.v_users_eligible;

-- I) Справочники (eligible) — при необходимости скорректируй белые списки
INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_status_values_eligible','warn','unknown_status_rows',
       toFloat64(countIf(status NOT IN ('active','inactive','blocked','deleted','pending'))),
       'eq', toFloat64(0),
       CAST(countIf(status NOT IN ('active','inactive','blocked','deleted','pending')) = 0 AS UInt8)
FROM betzo_staging.v_users_eligible;

INSERT INTO betzo_monitoring.dq_results
SELECT now(),'staging','users','stg_currency_values_eligible','warn','unknown_currency_rows',
       toFloat64(countIf(currency NOT IN ('USD','EUR','GBP','RUB','KZT','TRY','UAH','BRL','MXN'))),
       'eq', toFloat64(0),
       CAST(countIf(currency NOT IN ('USD','EUR','GBP','RUB','KZT','TRY','UAH','BRL','MXN')) = 0 AS UInt8)
FROM betzo_staging.v_users_eligible;
