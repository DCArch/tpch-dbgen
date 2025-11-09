#!/bin/bash

# TPCH Setup Script
# This script should be run ONCE to:
# 1. Build TPCH dbgen
# 2. Generate TPCH data
# 3. Initialize PostgreSQL database
# 4. Load TPCH data into database
# 5. Run warmup queries until memory reaches ~128GB
# 6. Save database state for future benchmark runs
#
# Usage:
#   setup_tpch.sh [OPTIONS]
#
# Options:
#   --postgres-base DIR     PostgreSQL installation directory
#   --postgres-bin DIR      PostgreSQL bin directory
#   --postgres-lib DIR      PostgreSQL lib directory
#   --postgres-data DIR     PostgreSQL data directory
#   --postgres-data-cpy DIR PostgreSQL checkpoint directory
#   --db-name NAME          Database name (default: tpch)
#   --db-user USER          Database user (default: tpch)
#   --db-password PASS      Database password (default: changeme)
#   --scale-factor NUM      TPCH scale factor (default: 100)
#   --warmup-rounds NUM     Number of warmup rounds (default: 3)
#   --target-memory NUM     Target memory in GB (default: 128)
#   --help                  Show this help message

set -e  # Exit on error

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load default configuration (sets defaults via environment variable fallbacks)
source "${SCRIPT_DIR}/tpch_config.sh"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --postgres-base)
            POSTGRES_BASE="$2"
            POSTGRES_BIN="${POSTGRES_BASE}/bin"
            POSTGRES_LIB="${POSTGRES_BASE}/lib"
            shift 2
            ;;
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
        --postgres-data-cpy)
            POSTGRES_DATA_CPY="$2"
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
        --scale-factor)
            SCALE_FACTOR="$2"
            shift 2
            ;;
        --warmup-rounds)
            WARMUP_ROUNDS="$2"
            shift 2
            ;;
        --target-memory)
            TARGET_MEMORY_GB="$2"
            shift 2
            ;;
        --help)
            head -n 30 "$0" | grep "^#" | sed 's/^# //'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "TPCH Setup Script"
echo "=========================================="
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Target Memory: ${TARGET_MEMORY_GB} GB"
echo "PostgreSQL Data: ${POSTGRES_DATA}"
echo "=========================================="

# Step 1: Build dbgen if not already built
echo ""
echo "Step 1: Building TPCH dbgen..."
if [ ! -f "${SCRIPT_DIR}/dbgen" ]; then
    cd "${SCRIPT_DIR}"
    make
    echo "dbgen built successfully"
else
    echo "dbgen already exists, skipping build"
fi

# Step 2: Generate TPCH data if not already generated
echo ""
echo "Step 2: Generating TPCH data (Scale Factor: ${SCALE_FACTOR})..."
cd "${SCRIPT_DIR}"
if [ ! -f "lineitem.tbl" ]; then
    ./dbgen -s ${SCALE_FACTOR} -v
    echo "Data generation complete"
else
    echo "Data files already exist, skipping generation"
    read -p "Do you want to regenerate data? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        ./dbgen -s ${SCALE_FACTOR} -v -f
    fi
fi

# Step 3: Initialize PostgreSQL
echo ""
echo "Step 3: Initializing PostgreSQL..."

# Kill any existing postgres processes
pkill postgres || true
sleep 2

# Remove old data directories
rm -rf "${POSTGRES_DATA}"
rm -rf "${POSTGRES_DATA_CPY}"

# Initialize new database cluster
"${POSTGRES_BIN}/initdb" -D "${POSTGRES_DATA}"

# Configure PostgreSQL for large memory workloads
echo "Configuring PostgreSQL settings..."
sed -i -e 's/#*shared_buffers = .*/shared_buffers = 64GB/g' "${POSTGRES_DATA}/postgresql.conf"
sed -i -e 's/#*effective_cache_size = .*/effective_cache_size = 192GB/g' "${POSTGRES_DATA}/postgresql.conf"
sed -i -e 's/#*maintenance_work_mem = .*/maintenance_work_mem = 4GB/g' "${POSTGRES_DATA}/postgresql.conf"
sed -i -e 's/#*work_mem = .*/work_mem = 256MB/g' "${POSTGRES_DATA}/postgresql.conf"
sed -i -e 's/#*max_connections = .*/max_connections = 200/g' "${POSTGRES_DATA}/postgresql.conf"
sed -i -e 's/#*max_worker_processes = .*/max_worker_processes = 128/g' "${POSTGRES_DATA}/postgresql.conf"
sed -i -e 's/#*max_parallel_workers = .*/max_parallel_workers = 128/g' "${POSTGRES_DATA}/postgresql.conf"
sed -i -e 's/#*max_parallel_workers_per_gather = .*/max_parallel_workers_per_gather = 32/g' "${POSTGRES_DATA}/postgresql.conf"

# Start PostgreSQL
echo "Starting PostgreSQL..."
"${POSTGRES_BIN}/pg_ctl" -D "${POSTGRES_DATA}" -l "${POSTGRES_DATA}/logfile" start
sleep 3

# Step 4: Create database and user
echo ""
echo "Step 4: Creating database and user..."
export LD_LIBRARY_PATH="${POSTGRES_LIB}"
"${POSTGRES_BIN}/psql" -h ${DB_HOST} -p ${DB_PORT} -d postgres <<EOF
CREATE USER ${DB_USER} WITH ENCRYPTED PASSWORD '${DB_PASSWORD}';
CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};
GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};
EOF

# Create DCSim extension as superuser
echo "Creating DCSim extension..."
"${POSTGRES_BIN}/psql" -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} <<EOF
CREATE EXTENSION IF NOT EXISTS dcsim;
EOF

# Step 5: Create TPCH schema and load data
echo ""
echo "Step 5: Creating TPCH schema and loading data..."
cd "${SCRIPT_DIR}"

# Create tables
echo "Creating TPCH tables..."
"${POSTGRES_BIN}/psql" -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -f dss.ddl

# Load data
echo "Loading TPCH data (this may take a while for large scale factors)..."
for table in region nation customer supplier part partsupp orders lineitem; do
    echo "  Loading ${table}..."
    if [ -f "${table}.tbl" ]; then
        # Strip trailing delimiter from TPCH data files (they end with |)
        sed 's/|$//' "${SCRIPT_DIR}/${table}.tbl" > "${SCRIPT_DIR}/${table}.tbl.tmp"
        "${POSTGRES_BIN}/psql" -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "\COPY ${table} FROM '${SCRIPT_DIR}/${table}.tbl.tmp' DELIMITER '|';"
        rm "${SCRIPT_DIR}/${table}.tbl.tmp"
    fi
done

# Create indexes and constraints (from dss.ri)
echo "Creating indexes and foreign keys..."
"${POSTGRES_BIN}/psql" -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -f dss.ri

# Run ANALYZE to update statistics
echo "Analyzing tables..."
"${POSTGRES_BIN}/psql" -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "ANALYZE;"

# Step 6: Run warmup queries
echo ""
echo "Step 6: Running warmup queries to reach ${TARGET_MEMORY_GB}GB memory usage..."
export PGPASSWORD="${DB_PASSWORD}"
python3 "${SCRIPT_DIR}/warmup_tpch.py" \
    --host ${DB_HOST} \
    --port ${DB_PORT} \
    --dbname ${DB_NAME} \
    --user ${DB_USER} \
    --password ${DB_PASSWORD} \
    --query-dir "${SCRIPT_DIR}/queries" \
    --warmup-rounds ${WARMUP_ROUNDS} \
    --target-memory ${TARGET_MEMORY_GB}

# Step 7: Stop PostgreSQL and save data directory
echo ""
echo "Step 7: Saving database state..."
"${POSTGRES_BIN}/pg_ctl" -D "${POSTGRES_DATA}" stop
sleep 2

echo "Copying data directory to ${POSTGRES_DATA_CPY}..."
cp -r "${POSTGRES_DATA}" "${POSTGRES_DATA_CPY}"

echo ""
echo "=========================================="
echo "TPCH Setup Complete!"
echo "=========================================="
echo "Database state saved to: ${POSTGRES_DATA_CPY}"
echo "You can now run benchmarks using: ./run_tpch.sh"
echo "=========================================="
