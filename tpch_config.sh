#!/bin/bash

# TPCH Default Configuration
# These are default values that can be overridden by command-line arguments

# PostgreSQL installation paths (defaults - override with arguments)
POSTGRES_BASE="${POSTGRES_BASE:-/usr/local/pgsql}"
POSTGRES_BIN="${POSTGRES_BIN:-${POSTGRES_BASE}/bin}"
POSTGRES_LIB="${POSTGRES_LIB:-${POSTGRES_BASE}/lib}"
POSTGRES_DATA="${POSTGRES_DATA:-${POSTGRES_BASE}/data}"
POSTGRES_DATA_CPY="${POSTGRES_DATA_CPY:-${POSTGRES_BASE}/data_cpy}"

# Database connection settings
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-tpch}"
DB_USER="${DB_USER:-tpch}"
DB_PASSWORD="${DB_PASSWORD:-changeme}"

# TPCH settings
SCALE_FACTOR="${SCALE_FACTOR:-100}"  # 100 = 100GB dataset
WARMUP_ROUNDS="${WARMUP_ROUNDS:-3}"   # Number of warmup query rounds

# Memory target (in GB) - warmup until postgres uses approximately this much memory
TARGET_MEMORY_GB="${TARGET_MEMORY_GB:-128}"

# Query execution settings
# Leave empty to run all queries, or specify comma-separated list like "1,3,6,12"
QUERIES_TO_RUN="${QUERIES_TO_RUN:-}"

# Number of parallel streams (optional, for throughput test)
NUM_STREAMS="${NUM_STREAMS:-1}"
