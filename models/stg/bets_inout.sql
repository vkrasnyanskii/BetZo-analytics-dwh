Ã¯Â»Â¿{{ config(materialized='view', schema='stg') }}

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
  arrayJoin([transaction_id]) as _txid_norm,        -- ï¿½Â´ï¿½Â»Ã‘Â Ã¯Â¿Â½ï¿½Â¾ï¿½Â²ï¿½Â¼ï¿½ÂµÃ¯Â¿Â½Ã‘â€šï¿½Â¸ï¿½Â¼ï¿½Â¾Ã¯Â¿Â½Ã‘â€šï¿½Â¸, ï¿½ÂµÃ¯Â¿Â½ï¿½Â»ï¿½Â¸ ï¿½Â¿ï¿½Â¾ï¿½Â½ï¿½Â°ï¿½Â´ï¿½Â¾ï¿½Â±ï¿½Â¸Ã‘â€šÃ¯Â¿Â½Ã‘Â
  'inout_games'                                     as aggregator,
  finished, refund_inserted, provider_round_id, round_id,
  _peerdb_synced_at
from {{ raw_src('inout_games_transactions') }}


