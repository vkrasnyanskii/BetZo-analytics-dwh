-- База мониторинга
CREATE DATABASE IF NOT EXISTS betzo_monitoring ON CLUSTER 'projectk-analytics';

-- Локальная таблица результатов DQ
CREATE TABLE IF NOT EXISTS betzo_monitoring.dq_results_shard ON CLUSTER 'projectk-analytics'
(
  run_ts          DateTime,
  layer           LowCardinality(String),
  entity          LowCardinality(String),
  check_name      LowCardinality(String),

  severity        LowCardinality(String),    -- 'info'|'warn'|'error'
  metric_name     LowCardinality(String),
  metric_value    Float64,

  threshold_op    LowCardinality(String),    -- 'eq'|'le'|'-'
  threshold_value Nullable(Float64),

  is_ok           UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(run_ts)
ORDER BY (run_ts, layer, entity, check_name, metric_name);

-- Distributed-слой
CREATE TABLE IF NOT EXISTS betzo_monitoring.dq_results ON CLUSTER 'projectk-analytics'
AS betzo_monitoring.dq_results_shard
ENGINE = Distributed('projectk-analytics', 'betzo_monitoring', 'dq_results_shard', cityHash64(layer, entity, check_name, run_ts));
