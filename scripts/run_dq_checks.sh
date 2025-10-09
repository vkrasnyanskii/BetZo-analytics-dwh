#!/usr/bin/env bash
set -euo pipefail

: ${CH_HOST:?Set CH_HOST (host:port)}
: ${CH_USER:=default}
: ${CH_PASSWORD:=}

clickhouse-client \
  --host="${CH_HOST%:*}" --port="${CH_HOST#*:}" \
  -u "$CH_USER" --password "$CH_PASSWORD" \
  --multiquery -n < sql/users/91_dq_staging_users.sql

echo "DQ checks inserted into betzo_monitoring.dq_results"