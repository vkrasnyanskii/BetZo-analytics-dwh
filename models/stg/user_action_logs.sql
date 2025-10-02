ï»¿{{ config(materialized='view', schema='stg') }}

select
  cast(id as String)                      as action_id,
  cast(user_id as String)                 as user_id,
  {{ norm_upper('action') }}              as action,
  {{ epoch_to_dt64('timestamp') }}        as ts,
  ip_address, country_id, os_id, browser_id, device_id, admin_id,
  details, reason, fingerprint_id, ip_id,
  {{ norm_upper('user_flow_type') }}      as user_flow_type,
  _peerdb_synced_at
from {{ raw_src('user_action_logs') }}

