#!/usr/bin/env python3
"""
TPCH Warmup Script
Runs a subset of TPCH queries repeatedly until PostgreSQL memory usage
reaches the target threshold (default: 128GB)
"""

import psycopg2
import subprocess
import time
import sys
import os
import argparse

def get_system_postgres_memory():
    """
    Get actual RSS memory usage of postgres processes from the system.
    Returns memory in GB.
    """
    try:
        # Get memory usage of all postgres processes
        cmd = "ps aux | grep postgres | grep -v grep | awk '{sum+=$6} END {print sum/1024/1024}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        memory_gb = float(result.stdout.strip()) if result.stdout.strip() else 0
        return memory_gb
    except Exception as e:
        print(f"Warning: Could not get system memory usage: {e}")
        return 0

def read_query_file(query_file):
    """
    Read and prepare a query file for execution.
    Removes QGEN template tags that aren't valid SQL.
    """
    with open(query_file, 'r') as f:
        lines = f.readlines()

    # Filter out QGEN-specific template lines (starting with :)
    sql_lines = []
    for line in lines:
        stripped = line.strip()
        # Skip lines that start with : (QGEN tags)
        if stripped and not stripped.startswith(':'):
            # Also skip comment lines starting with { or --
            if not stripped.startswith('{') and not stripped.startswith('--'):
                sql_lines.append(line)

    query = ''.join(sql_lines)

    # For simplicity, substitute common parameter placeholders with defaults
    # Query 1 uses :1 which should be a number of days
    query = query.replace(":1", "90")
    query = query.replace(":2", "15")
    query = query.replace(":3", "3")

    return query

def warmup_queries(conn, query_dir, warmup_rounds, target_memory_gb):
    """
    Run a subset of TPCH queries multiple times to warm up the database
    and load data into memory until target is reached.
    """
    print(f"Starting warmup phase...")
    print(f"Target memory: {target_memory_gb} GB")
    print(f"Warmup rounds: {warmup_rounds}")

    # Use queries 1, 3, 6, 12 for warmup (representative mix)
    # These queries touch different parts of the database
    warmup_query_nums = [1, 3, 6, 12, 14]

    cur = conn.cursor()

    for round_num in range(warmup_rounds):
        print(f"\nWarmup round {round_num + 1}/{warmup_rounds}")

        for qnum in warmup_query_nums:
            query_file = os.path.join(query_dir, f"{qnum}.sql")
            if not os.path.exists(query_file):
                print(f"  Warning: Query file {query_file} not found, skipping")
                continue

            try:
                query = read_query_file(query_file)

                print(f"  Running warmup query {qnum}...", end='', flush=True)
                start_time = time.time()
                cur.execute(query)
                result_count = len(cur.fetchall())
                elapsed = time.time() - start_time
                print(f" completed in {elapsed:.2f}s ({result_count} rows)")

                # Check memory after each query
                sys_mem = get_system_postgres_memory()
                print(f"    Current Postgres memory: {sys_mem:.2f} GB")

                # Check if we've reached the target
                if sys_mem >= target_memory_gb * 0.95:  # 95% of target
                    print(f"\n  Memory threshold reached ({sys_mem:.2f} GB >= {target_memory_gb * 0.95:.2f} GB)")
                    cur.close()
                    conn.commit()
                    return True

            except Exception as e:
                print(f" FAILED: {e}")
                conn.rollback()

        conn.commit()

        # Check memory at end of round
        sys_mem = get_system_postgres_memory()
        if sys_mem >= target_memory_gb * 0.95:
            print(f"\nMemory threshold reached ({sys_mem:.2f} GB)")
            cur.close()
            return True

    # Warmup rounds completed
    final_mem = get_system_postgres_memory()
    print(f"\nWarmup completed. Final memory: {final_mem:.2f} GB")

    if final_mem < target_memory_gb * 0.9:
        print(f"WARNING: Did not reach target memory ({target_memory_gb} GB)")
        print(f"You may need to:")
        print(f"  - Increase warmup rounds")
        print(f"  - Increase scale factor")
        print(f"  - Adjust PostgreSQL memory settings (shared_buffers, etc.)")

    cur.close()
    return False

def main():
    parser = argparse.ArgumentParser(
        description='Warmup TPCH database to target memory usage')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--dbname', default='tpch', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', default='', help='Database password')
    parser.add_argument('--query-dir', default='./queries',
                       help='Directory containing TPCH query SQL files')
    parser.add_argument('--warmup-rounds', type=int, default=3,
                       help='Number of warmup rounds')
    parser.add_argument('--target-memory', type=float, default=128.0,
                       help='Target memory usage in GB')

    args = parser.parse_args()

    # Check initial memory
    initial_mem = get_system_postgres_memory()
    print(f"Initial Postgres memory usage: {initial_mem:.2f} GB")

    # Connect to database
    print(f"\nConnecting to database {args.dbname} on {args.host}:{args.port}...")
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password
        )
        conn.autocommit = False
        print("Connected successfully\n")
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return 1

    try:
        warmup_queries(conn, args.query_dir, args.warmup_rounds, args.target_memory)
    except KeyboardInterrupt:
        print("\n\nWarmup interrupted by user")
        return 1
    except Exception as e:
        print(f"\n\nError during warmup: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()
        print("\nDatabase connection closed")

    return 0

if __name__ == '__main__':
    sys.exit(main())
