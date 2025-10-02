ï»¿{{ config(materialized='view', schema='stg') }}

select
  cast(user_id as String)                 as user_id,
  {{ norm_upper('usage_type') }}          as usage_type,
  {{ norm_upper('department') }}          as department,
  {{ norm_upper('access') }}              as access,
  {{ norm_upper('fin_volume') }}          as fin_volume,
  {{ norm_upper('trust') }}               as trust,
  {{ norm_upper('account_status') }}      as account_status,
  {{ norm_upper('kyc') }}                 as kyc,
  {{ norm_upper('aml') }}                 as aml,
  {{ norm_upper('risk') }}                as risk,
  {{ norm_upper('retention_segment') }}   as retention_segment,
  {{ norm_upper('retention_streak') }}    as retention_streak,
  {{ norm_upper('payment') }}             as payment,
  {{ norm_upper('funnel') }}              as funnel,
  {{ norm_upper('behavior') }}            as behavior,
  {{ norm_upper('value_current') }}       as value_current,
  {{ norm_upper('value_dynamic') }}       as value_dynamic,
  cast(is_multiaccount as UInt8)          as is_multiaccount,
  _peerdb_synced_at
from {{ raw_src('user_statuses') }}

