{{ config(materialized="table") }}

with unioned as (
  select * from stg.bets_inout
  union all
  select * from stg.bets_slotegrator
),
dedup as (
  select
    *,
    row_number() over (
      partition by src, transaction_id, role
      order by _peerdb_version desc, _peerdb_synced_at desc, bet_time desc
    ) as rn
  from unioned
  where is_deleted = 0
)
select
  concat(src, ':', transaction_id, ':', role) as bet_id,
  transaction_id,
  src,
  user_id,
  bet_time,
  game_id,
  session_id,
  bonus_id,
  status,
  type,
  role,
  stake_amount,
  win_amount,
  loss_amount,
  rollback_amount,
  (win_amount - stake_amount) as net_amount
from dedup
where rn = 1