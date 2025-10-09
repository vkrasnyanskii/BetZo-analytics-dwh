#!/usr/bin/env bash
set -euo pipefail

: ${CH_HOST:?Set CH_HOST (host:port)}
: ${CH_USER:=default}
: ${CH_PASSWORD:=}

run_sql() {
  local file="$1"
  echo ">>> APPLY: $file"
  clickhouse-client \
    --host="${CH_HOST%:*}" --port="${CH_HOST#*:}" \
    -u "$CH_USER" --password "$CH_PASSWORD" \
    --multiquery -n < "$file"
}

# ??????? ????? ????????
[ -f sql/common/02_monitoring_schema.sql ] && run_sql sql/common/02_monitoring_schema.sql || true
[ -f sql/users/10_staging_users.sql      ] && run_sql sql/users/10_staging_users.sql      || true
[ -f sql/users/20_core_dim_users.sql     ] && run_sql sql/users/20_core_dim_users.sql     || true

echo "Done."