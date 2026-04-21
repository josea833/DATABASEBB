from __future__ import annotations

import argparse
import statistics

from benchmark_db_copy import bootstrap_benchmarks
from experiment2_core import run_real_time_experiment
from experiment2_utilities import RealTimeResult

def print_report(result: RealTimeResult) -> None:
    
    print("Experiment 2: Real Time data capture and analytics")

    print(f"\n Dataset: ")
    print(f" Base Rows: {result.base_row_count:,}")
    print(f" New Rows inserted: {result.new_rows_inserted:,}")
    print(f" Total after insert: {result.base_row_count + result.new_rows_inserted:,}")

    print(f"\n Insert Performance (appending new data):")
    print(f" {'Database' :<15}{'Total Time (ms)':>18}{'Per Row(ms)':>15}")
    print(f" {'-'*15}{'-'*18}{'-'*15}")
    print(f" {'MySQL':<15}{result.mysql_insert_total_ms:>18.2f}{result.mysql_insert_avg_ms:>15.3f}")
    print(f" {'Clickhouse':<15}{result.clickhouse_insert_total_ms:>18.2f}{result.clickhouse_insert_avg_ms:>15.3f}")

    insert_speedup = result.mysql_insert_total_ms / result.clickhouse_insert_total_ms
    if insert_speedup > 1:
        print(f"\n Clickhouse insert is {insert_speedup:.1f}x faster then MySQL")
    else:
        print(f"\n MySQL insert is {1/insert_speedup:.1f}x faster than Clickhouse")

    print(f"\n Query Performance: ")
    mysql_median = statistics.median(result.mysql_query_ms)
    ch_median = statistics.median(result.clickhouse_query_ms)

    print(f" {'Database':<15}{'Median (ms)':>15}{'Min (ms)':>12}{'Max (ms)':>12}")
    print(f" {'-'*15}{'-'*15}{'-'*12}{'-'*12}")
    print(f" {'MySQL':<15}{mysql_median:>15.2f}{min(result.mysql_query_ms):>12.2f}{max(result.mysql_query_ms):>12.2f}")
    print(f" {'Clickhouse':<15}{ch_median:>15.2f}{min(result.clickhouse_query_ms):>12.2f}{max(result.clickhouse_query_ms):>12.2f}")

    query_speedup = mysql_median / ch_median
    if query_speedup > 1:
         print(f"\n Clickhouse insert is {query_speedup:.1f}x faster then MySQL")
    else:
        print(f"\n MySQL insert is {1/query_speedup:.1f}x faster than Clickhouse")

    print(f"\n Individual Query Runtimes (ms):")
    print(f" {'Run':<8}{'MySQL':>12}{'ClickHouse':>14}")
    print(f" {'-'*8}{'-'*12}{'-'*14}")
    for i, (m, c) in enumerate(zip(result.mysql_query_ms, result.clickhouse_query_ms), start=1):
        print(f" {i:<8}{m:>12.2f}{c:>14.2f}")

    print(f"\n Real-time Data Visibility: ")
    print(f" MySQL: {'New Data Visible' if result.mysql_new_data_visible else 'New Data not visible'}")
    print(f" Clickhouse: {'New Data visible' if result.clickhouse_new_data_visible else 'New Data not visible'}")

    print(f"\n Real-time Analytics Score:")
    if query_speedup > 2 and insert_speedup > 1:
        print("  ★★★ EXCELLENT - ClickHouse is ideal for real-time analytics")
    elif query_speedup > 1.5:
        print("  ★★ GOOD - ClickHouse performs well for real-time queries")
    elif query_speedup > 1:
        print("  ★ FAIR - ClickHouse is faster but consider other factors")
    else:
        print("  • MySQL may be better for this specific workload")
    

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Experiment 2: Real-time Data capture and query performance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--table",
        default = "Products_wide",
        help="Table name (default: prodcuts_wide)"
    )
    parser.add_argument(
        "--insert-rows",
        type=int,
        default=10000,
        help="Number of new rows to insert (default: 10000)"
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of query runs after insert (default: 5)"
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help="Base dataset row count (default: for env or 100,000)"
    )
    parser.add_argument(
        "--bootstrap-only",
        action="store_true",
        help="Only bootstrap databases and exit"
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.bootstrap_only:
        rows = bootstrap_benchmarks(args.table, row_target=args.rows, rebuild=True)
        print(f" Bootstrapped table '{args.table}' with {rows:,} rows")
        return
    
    result = run_real_time_experiment(
        table_name=args.table,
        new_rows=args.insert_rows,
        query_runs=args.runs,
        base_rows=args.rows,
    )

    print_report(result)

if __name__ == "__main__":
    main()