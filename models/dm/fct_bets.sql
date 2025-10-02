ï»¿{{ config(
    materialized='table',
    schema='dm',
    engine='MergeTree()',
    order_by='(toDate(bet_time), user_id, transaction_id)'
) }}

with unioned as (
  select * from stg.bets_inout
  union all
  select * from stg.bets_slotegrator
),
dedup as (
  select
    transaction_id,
    anyLast(user_id)    as user_id,
    anyLast(game_id)    as game_id,
    anyLast(session_id) as session_id,
    max(event_time)     as event_time,
    anyLast(status)     as status,
    argMax(amount, event_time) as amount,
    anyLast(aggregator) as aggregator
  from unioned
  group by transaction_id
)
select
  user_id,
  transaction_id,
  game_id,
  session_id,
  event_time as bet_time,
  if(status='WIN', toDecimal128(0,2), amount) as bet_amount,
  if(status='WIN', amount, toDecimal128(0,2)) as payout_amount,
  status,
  aggregator
from dedup

