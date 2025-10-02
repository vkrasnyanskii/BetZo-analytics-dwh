ï»¿{{ config(materialized='view', schema='stg') }}

select
  cast(user_id as String)                           as user_id,
  {{ epoch_to_dt64("intDiv(toUnixTimestamp64Micro(timestamp),1000000)") }} as event_time,
  {{ amount_to_decimal('amount') }}                 as amount,
  cast(game_id as String)                           as game_id,
  cast(transaction_id as String)                    as transaction_id,
  cast(parent_transaction_id as String)             as parent_transaction_id,
  cast(session_id as String)                        as session_id,
  {{ normalize_bet_status('status') }}              as status,
  upper(coalesce(type,'GAME'))                      as type,
  arrayJoin([transaction_id]) as _txid_norm,        -- �´�»Ñ ï¿½�¾�²�¼�µï¿½Ñ‚�¸�¼�¾ï¿½Ñ‚�¸, �µï¿½�»�¸ �¿�¾�½�°�´�¾�±�¸Ñ‚ï¿½Ñ
  'inout_games'                                     as aggregator,
  finished, refund_inserted, provider_round_id, round_id,
  _peerdb_synced_at
from {{ raw_src('inout_games_transactions') }}


