Ã¯Â»Â¿{{ config(materialized='view', schema='stg') }}

select
  cast(uuid as String)           as game_uuid,
  {{ norm_upper('type') }}       as game_type,
  {{ norm_upper('technology') }} as technology,
  has_lobby, is_mobile, has_freespins, has_tables, freespin_valid_until_full_day,
  cast(developer_id as Int32)    as developer_id,
  cast(id as Int32)              as game_id,
  has_demo, seo_cards, origin_image_url, static_id,
  has_change_image,
  _peerdb_synced_at
from {{ raw_src('casino_games') }}

