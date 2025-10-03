{{ config(materialized='view') }}

with base as (
  select
    toString(transaction_id)          as transaction_id,
    toString(user_id)                 as user_id,
    ts                                as bet_time,      -- DateTime64(3)
    amount                            as amount,        -- Decimal(18,2)
    toString(game_id)                 as game_id,
    toString(parent_transaction_id)   as parent_transaction_id,
    toString(session_id)              as session_id,
    {{ normalize_bet_status('status') }} as status,
    toString(type)                    as type,
    toInt64(id_raw)                   as id_raw,
    toInt32(bonus_id)                 as bonus_id,
    transaction_ids                   as transaction_ids,
    toInt64(balance)                  as balance,
    toUInt8(refund_inserted)          as refund_inserted,
    toUInt8(finished)                 as finished,
    toString(provider_round_id)       as provider_round_id,
    toString(round_id)                as round_id,
    _peerdb_synced_at,
    toUInt8(is_deleted)               as is_deleted,
    _peerdb_version
  from {{ source('betzo_raw','inout_games_transactions') }}
),
mapped as (
  select
    *,
    {{ classify_bet_status('status') }} as role
  from base
)
select
  transaction_id,
  user_id,
  bet_time,
  amount,
  game_id,
  parent_transaction_id,
  session_id,
  status,
  type,
  id_raw,
  bonus_id,
  transaction_ids,
  balance,
  refund_inserted,
  finished,
  provider_round_id,
  round_id,
  _peerdb_synced_at,
  is_deleted,
  _peerdb_version,

  role,
  if(role = 'STAKE',    amount, cast(0 as Decimal(18,2))) as stake_amount,
  if(role = 'WIN',      amount, cast(0 as Decimal(18,2))) as win_amount,
  if(role = 'LOSS',     amount, cast(0 as Decimal(18,2))) as loss_amount,
  if(role = 'ROLLBACK', amount, cast(0 as Decimal(18,2))) as rollback_amount,

  'inout' as src
from mapped