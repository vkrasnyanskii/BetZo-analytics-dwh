ï»¿{{ config(materialized='view', schema='stg') }}

select
  cast(id as String)                               as deposit_id,
  cast(user_id as String)                          as user_id,
  {{ epoch_to_dt64('timestamp') }}                 as deposit_time,
  {{ amount_to_decimal('amount') }}                as amount,
  cast(currency as String)                         as currency,
  cast(wallet as String)                           as payment_method,
  {{ normalize_deposit_status('status') }}         as status,
  brand,
  cast(enrolled_number as Int32)                   as enrolled_number,
  order_id, transaction_id as ext_transaction_id,
  _transaction_id as internal_tx_id,
  card_name_id, fee, user_flow_type,
  _peerdb_synced_at
from {{ raw_src('deposits') }}

