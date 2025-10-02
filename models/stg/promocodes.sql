ï»¿{{ config(materialized='view', schema='stg') }}

select
  cast(id as String)                     as promocode_id,
  name,
  max_usage,
  {{ amount_to_decimal('amount') }}      as amount,
  {{ epoch_to_dt64('created_at') }}      as created_at,
  {{ epoch_to_dt64('expired_at') }}      as expired_at,
  cast(is_deleted as UInt8)              as is_deleted,
  deleting_reason,
  betby_template_id,
  wager_multiplier,
  casino_freespin_template_id,
  {{ epoch_to_dt64('from_time') }}       as from_time,
  {{ epoch_to_dt64('to_time') }}         as to_time,
  cast(currency as String)               as currency,
  {{ amount_to_decimal('amount_won') }}  as amount_won,
  {{ norm_upper('bonus_type') }}         as bonus_type,
  {{ norm_upper('type') }}               as type,
  freespin_s_template_id, freespin_op_template_id, freespin_iog_template_id,
  {{ norm_upper('wager_source') }}       as wager_source,
  profile_conditions, bet_type, min_bet_count,
  min_coefficient_express, min_coefficient_ordinary,
  title, localization_key,
  {{ norm_upper('brand') }}              as brand,
  bonus_template_id,
  crm_id, crm_code,
  partner_id,
  subid1, subid2, subid3, subid4, subid5,
  {{ norm_upper('classification') }}     as classification,
  _peerdb_synced_at
from {{ raw_src('promocodes') }}

