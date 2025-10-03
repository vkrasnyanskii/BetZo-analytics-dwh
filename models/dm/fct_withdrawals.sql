Ã¯Â»Â¿{{ config(
    materialized='table',
    schema='dm',
    engine='MergeTree()',
    order_by='(toDate(withdrawal_time), user_id, withdrawal_id)'
) }}

select
  withdrawal_id,
  user_id,
  withdrawal_time,
  amount,
  currency,
  wallet_type,
  status,
  brand,
  is_first
from stg.withdrawals
where note is null

