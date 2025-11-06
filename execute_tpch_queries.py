#!/usr/bin/env python3
"""
TPCH Query Executor with DCSim Hooks
Executes TPCH benchmark queries with simulation hooks:
1. Creates dcsim extension
2. Calls dcsim_start_simulation()
3. Runs TPCH queries
4. Calls dcsim_end_simulation()
"""

import psycopg2
import time
import sys
import os
import argparse
import json

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
    # In production, you'd use proper parameter generation as per TPC-H spec
    query = query.replace(":1", "90")
    query = query.replace(":2", "15")
    query = query.replace(":3", "3")

    return query

def activate_dcsim(conn):
    """Activate DCSim simulation hooks"""
    print("\n" + "="*60)
    print("ACTIVATING DCSIM SIMULATION HOOKS")
    print("="*60)

    cur = conn.cursor()
    try:
        # Create extension if it doesn't exist
        print("Creating dcsim extension...")
        cur.execute("CREATE EXTENSION IF NOT EXISTS dcsim;")
        conn.commit()

        # Start simulation
        print("Starting simulation...")
        cur.execute("SELECT dcsim_start_simulation();")
        conn.commit()

        print("DCSim simulation started successfully")
        print("="*60 + "\n")
    except Exception as e:
        print(f"Error activating DCSim: {e}")
        raise
    finally:
        cur.close()

def deactivate_dcsim(conn):
    """Deactivate DCSim simulation hooks"""
    print("\n" + "="*60)
    print("DEACTIVATING DCSIM SIMULATION HOOKS")
    print("="*60)

    cur = conn.cursor()
    try:
        cur.execute("SELECT dcsim_end_simulation();")
        conn.commit()
        print("DCSim simulation ended successfully")
        print("="*60 + "\n")
    except Exception as e:
        print(f"Error deactivating DCSim: {e}")
        raise
    finally:
        cur.close()

def run_tpch_queries(conn, query_dir, queries_to_run=None):
    """
    Run the TPCH benchmark queries.
    If queries_to_run is None, runs all 22 queries.
    """
    if queries_to_run is None:
        queries_to_run = range(1, 23)  # TPCH has 22 queries

    print("\n" + "="*60)
    print("RUNNING TPCH BENCHMARK QUERIES")
    print("="*60)

    results = {}
    cur = conn.cursor()

    for qnum in queries_to_run:
        query_file = os.path.join(query_dir, f"{qnum}.sql")
        if not os.path.exists(query_file):
            print(f"Warning: Query file {query_file} not found, skipping")
            continue

        try:
            query = read_query_file(query_file)

            print(f"\nRunning TPCH Query {qnum}...", end='', flush=True)
            start_time = time.time()
            cur.execute(query)
            result_count = len(cur.fetchall())
            elapsed = time.time() - start_time

            results[qnum] = {
                'time': elapsed,
                'rows': result_count,
                'success': True
            }
            print(f" completed in {elapsed:.2f}s ({result_count} rows)")

        except Exception as e:
            print(f" FAILED: {e}")
            results[qnum] = {
                'time': 0,
                'rows': 0,
                'success': False,
                'error': str(e)
            }
            conn.rollback()

        conn.commit()

    cur.close()
    return results

def print_results_summary(results):
    """Print a summary of benchmark results"""
    print("\n" + "="*60)
    print("BENCHMARK RESULTS SUMMARY")
    print("="*60)

    total_time = 0
    success_count = 0

    for qnum in sorted(results.keys()):
        result = results[qnum]
        status = "OK" if result['success'] else "FAILED"
        if result['success']:
            print(f"Query {qnum:2d}: {result['time']:8.2f}s ({result['rows']:6d} rows) - {status}")
            total_time += result['time']
            success_count += 1
        else:
            print(f"Query {qnum:2d}: {'N/A':>8s} ({'N/A':>6s} rows) - {status}")

    print("-"*60)
    print(f"Total queries: {len(results)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {len(results) - success_count}")
    print(f"Total execution time: {total_time:.2f}s")
    print("="*60)

    return results

def save_results(results, output_file):
    """Save results to JSON file"""
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description='Run TPCH benchmark with DCSim simulation hooks')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--dbname', default='tpch', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', default='', help='Database password')
    parser.add_argument('--query-dir', default='./queries',
                       help='Directory containing TPCH query SQL files')
    parser.add_argument('--queries', type=str, default=None,
                       help='Comma-separated list of query numbers to run (default: all)')
    parser.add_argument('--output', type=str, default=None,
                       help='Output file for results (JSON format)')

    args = parser.parse_args()

    # Parse query list if provided
    queries_to_run = None
    if args.queries:
        queries_to_run = [int(q.strip()) for q in args.queries.split(',')]

    # Connect to database
    print(f"Connecting to database {args.dbname} on {args.host}:{args.port}...")
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password
        )
        conn.autocommit = False
        print("Connected successfully")
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return 1

    try:
        # Activate DCSim hooks
        activate_dcsim(conn)

        # Run benchmark queries
        results = run_tpch_queries(conn, args.query_dir, queries_to_run)

        # Deactivate DCSim hooks
        deactivate_dcsim(conn)

        # Print and save results
        print_results_summary(results)

        if args.output:
            save_results(results, args.output)

    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
        try:
            deactivate_dcsim(conn)
        except:
            pass
        return 1
    except Exception as e:
        print(f"\n\nError during benchmark execution: {e}")
        import traceback
        traceback.print_exc()
        try:
            deactivate_dcsim(conn)
        except:
            pass
        return 1
    finally:
        conn.close()
        print("\nDatabase connection closed")

    return 0

if __name__ == '__main__':
    sys.exit(main())
