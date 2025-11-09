#!/bin/bash

# TPCH Benchmark Run Script
# This script should be run for EACH benchmark execution.
# It restores the database from the saved checkpoint and runs the benchmark.
#
# Usage:
#   run_tpch.sh [OPTIONS]
#
# Options:
#   --postgres-bin DIR      PostgreSQL bin directory
#   --postgres-lib DIR      PostgreSQL lib directory
#   --postgres-data DIR     PostgreSQL data directory (target)
#   --tpch-data DIR         TPCH checkpoint data directory (source)
#   --db-name NAME          Database name (default: tpch)
#   --db-user USER          Database user (default: tpch)
#   --db-password PASS      Database password (default: changeme)
#   --queries LIST          Comma-separated query numbers (default: all)
#   --run-id ID             Run ID for results file (default: 1)
#   --warmup-iterations N   Number of warmup iterations (default: 1)
#   --skip-warmup           Skip warmup phase (not recommended)
#   --help                  Show this help message

set -e  # Exit on error

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load default configuration (sets defaults via environment variable fallbacks)
source "${SCRIPT_DIR}/tpch_config.sh"

# Default warmup iterations
WARMUP_ITERATIONS="${WARMUP_ITERATIONS:-1}"
SKIP_WARMUP=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --postgres-bin)
            POSTGRES_BIN="$2"
            shift 2
            ;;
        --postgres-lib)
            POSTGRES_LIB="$2"
            shift 2
            ;;
        --postgres-data)
            POSTGRES_DATA="$2"
            shift 2
            ;;
        --tpch-data)
            TPCH_DATA="$2"
            shift 2
            ;;
        --db-name)
            DB_NAME="$2"
            shift 2
            ;;
        --db-user)
            DB_USER="$2"
            shift 2
            ;;
        --db-password)
            DB_PASSWORD="$2"
            shift 2
            ;;
        --queries)
            QUERIES_TO_RUN="$2"
            shift 2
            ;;
        --run-id)
            RUN_ID="$2"
            shift 2
            ;;
        --warmup-iterations)
            WARMUP_ITERATIONS="$2"
            shift 2
            ;;
        --skip-warmup)
            SKIP_WARMUP=true
            shift
            ;;
        --help)
            head -n 22 "$0" | grep "^#" | sed 's/^# //'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Set defaults for unset variables
RUN_ID=${RUN_ID:-1}
RESULTS_DIR="${SCRIPT_DIR}/results"

echo "=========================================="
echo "TPCH Benchmark Run Script"
echo "=========================================="
echo "Run ID: ${RUN_ID}"
echo "Results Directory: ${RESULTS_DIR}"
echo "=========================================="

# Determine source data directory
# If TPCH_DATA is set, use it; otherwise use POSTGRES_DATA_CPY
SOURCE_DATA_DIR="${TPCH_DATA:-${POSTGRES_DATA_CPY}}"

# Check if checkpoint exists
if [ ! -d "${SOURCE_DATA_DIR}" ]; then
    echo "ERROR: Data directory ${SOURCE_DATA_DIR} not found!"
    echo "Please run setup_tpch.sh first to create the checkpoint."
    exit 1
fi

# Create results directory if it doesn't exist
mkdir -p "${RESULTS_DIR}"

# Step 1: Stop any running PostgreSQL instances
echo ""
echo "Step 1: Stopping any running PostgreSQL instances..."
pkill postgres || true
sleep 2

# Step 2: Restore database from checkpoint
echo ""
echo "Step 2: Restoring database from checkpoint..."
echo "Source: ${SOURCE_DATA_DIR}"
echo "Target: ${POSTGRES_DATA}"
rm -rf "${POSTGRES_DATA}"
cp -r "${SOURCE_DATA_DIR}" "${POSTGRES_DATA}"
echo "Database restored successfully"

# Step 3: Start PostgreSQL
echo ""
echo "Step 3: Starting PostgreSQL..."
export LD_LIBRARY_PATH="${POSTGRES_LIB}"
"${POSTGRES_BIN}/pg_ctl" -D "${POSTGRES_DATA}" -l "${POSTGRES_DATA}/logfile" start
sleep 3

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if "${POSTGRES_BIN}/pg_isready" -h ${DB_HOST} -p ${DB_PORT} > /dev/null 2>&1; then
        echo "PostgreSQL is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: PostgreSQL failed to start within 30 seconds"
        exit 1
    fi
    sleep 1
done

# Step 4: Run TPCH benchmark with DCSim hooks
echo ""
echo "Step 4: Running TPCH benchmark..."
RESULT_FILE="${RESULTS_DIR}/tpch_run_${RUN_ID}.json"

export PGPASSWORD="${DB_PASSWORD}"

# Build query list parameter if specified
QUERY_PARAM=""
if [ -n "${QUERIES_TO_RUN}" ]; then
    QUERY_PARAM="--queries ${QUERIES_TO_RUN}"
fi

# Build warmup parameters
WARMUP_PARAM="--warmup-iterations ${WARMUP_ITERATIONS}"
if [ "$SKIP_WARMUP" = true ]; then
    WARMUP_PARAM="--skip-warmup"
fi

python3 "${SCRIPT_DIR}/execute_tpch_queries.py" \
    --host ${DB_HOST} \
    --port ${DB_PORT} \
    --dbname ${DB_NAME} \
    --user ${DB_USER} \
    --password ${DB_PASSWORD} \
    --query-dir "${SCRIPT_DIR}/queries" \
    --output "${RESULT_FILE}" \
    ${QUERY_PARAM} \
    ${WARMUP_PARAM}

# Step 5: Stop PostgreSQL
echo ""
echo "Step 5: Stopping PostgreSQL..."
"${POSTGRES_BIN}/pg_ctl" -D "${POSTGRES_DATA}" stop
sleep 2

echo ""
echo "=========================================="
echo "TPCH Benchmark Run Complete!"
echo "=========================================="
echo "Results saved to: ${RESULT_FILE}"
echo "=========================================="
