ï»¿{{ config(materialized="table", schema="bi") }}

select
    now64(3)         as ts_utc,
    'dbt-clickhouse' as tool,
    version()        as ch_version

