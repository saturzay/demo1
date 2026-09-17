#!/usr/bin/env bash
set -euo pipefail

if [ -n "${PREFECT_POSTGRES_DB:-}" ]; then
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    SELECT 'CREATE DATABASE "${PREFECT_POSTGRES_DB}"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${PREFECT_POSTGRES_DB}')\gexec
SQL
fi
