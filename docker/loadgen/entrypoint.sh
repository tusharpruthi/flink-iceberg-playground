#!/bin/bash
# Steady trickle of WAL activity for CDC testing: a handful of pgbench transactions
# every INTERVAL_SECONDS, so the Flink job always has a small, predictable stream to sync.
set -euo pipefail

export PGPASSWORD="${POSTGRES_PASSWORD}"

until pg_isready -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; do
  echo "[loadgen] waiting for postgres..."
  sleep 2
done

if [ "$(psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -tAc "SELECT to_regclass('public.pgbench_accounts')")" = "" ]; then
  echo "[loadgen] initializing pgbench schema (scale factor 1, one-time bootstrap)..."
  pgbench -i -s 1 -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" "${POSTGRES_DB}"
fi

echo "[loadgen] steady trickle: ${TRANSACTIONS_PER_TICK} transaction(s) every ${INTERVAL_SECONDS}s"
while true; do
  sleep "${INTERVAL_SECONDS}"
  pgbench -c 1 -t "${TRANSACTIONS_PER_TICK}" -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" "${POSTGRES_DB}" \
    || echo "[loadgen] pgbench batch failed, will retry next tick"
done
