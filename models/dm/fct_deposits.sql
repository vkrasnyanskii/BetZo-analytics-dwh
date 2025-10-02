ï»¿{{ config(
    materialized='table',
    schema='dm',
    engine='MergeTree()',
    order_by='(toDate(deposit_time), user_id, deposit_id)'
) }}

select
  deposit_id,
  user_id,
  deposit_time,
  amount,
  currency,
  payment_method,
  status,
  brand,
  (enrolled_number = 1 and status = 'ENROLLED') as is_ftd
from stg.deposits
where status in ('ENROLLED','PENDING','CANCELLED','EXPIRED')

