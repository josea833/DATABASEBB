from __future__ import annotations

# Standard library imports used for command-line parsing and metric aggregation.
import argparse
import statistics

# Project imports for preparing data and running the benchmark experiment.
from benchmark_db_copy import bootstrap_benchmarks
from experiment2_core import run_real_time_experiment
from experiment2_utilities import RealTimeResult

def print_report(result: RealTimeResult) -> None:
    """Print a full benchmark report for the real-time experiment results."""

    # Print the experiment title and explain the output context.
    print("Experiment 2: Real Time data capture and analytics")

    # Dataset summary: show how many rows were present before insert and how many
    # synthetic rows were appended during the benchmark.
    print(f"\nDataset:")
    print(f"  Base Rows: {result.base_row_count:,}")
    print(f"  New Rows inserted: {result.new_rows_inserted:,}")
    print(f"  Total after insert: {result.base_row_count + result.new_rows_inserted:,}")

    # Insert performance: compare total insert time and average time per row.
    print(f"\nInsert Performance (appending new data):")
    print(f"  {'Database':<15}{'Total Time (ms)':>18}{'Per Row (ms)':>15}")
    print(f"  {'-'*15}{'-'*18}{'-'*15}")
    print(f"  {'MySQL':<15}{result.mysql_insert_total_ms:>18.2f}{result.mysql_insert_avg_ms:>15.3f}")
    print(f"  {'ClickHouse':<15}{result.clickhouse_insert_total_ms:>18.2f}{result.clickhouse_insert_avg_ms:>15.3f}")

    # Compare insert performance and give a simple speedup ratio.
    insert_speedup = result.mysql_insert_total_ms / result.clickhouse_insert_total_ms
    if insert_speedup > 1:
        print(f"\n  ClickHouse insert is {insert_speedup:.1f}x faster than MySQL")
    else:
        print(f"\n  MySQL insert is {1/insert_speedup:.1f}x faster than ClickHouse")

    # Query performance: report median, minimum, and maximum latency across runs.
    print(f"\nQuery Performance:")
    mysql_median = statistics.median(result.mysql_query_ms)
    ch_median = statistics.median(result.clickhouse_query_ms)

    print(f"  {'Database':<15}{'Median (ms)':>15}{'Min (ms)':>12}{'Max (ms)':>12}")
    print(f"  {'-'*15}{'-'*15}{'-'*12}{'-'*12}")
    print(f"  {'MySQL':<15}{mysql_median:>15.2f}{min(result.mysql_query_ms):>12.2f}{max(result.mysql_query_ms):>12.2f}")
    print(f"  {'ClickHouse':<15}{ch_median:>15.2f}{min(result.clickhouse_query_ms):>12.2f}{max(result.clickhouse_query_ms):>12.2f}")

    # Compare query latency and determine which database is faster.
    query_speedup = mysql_median / ch_median
    if query_speedup > 1:
        print(f"\n  ClickHouse query is {query_speedup:.1f}x faster than MySQL")
    else:
        print(f"\n  MySQL query is {1/query_speedup:.1f}x faster than ClickHouse")

    # Visibility check: determine whether the newly inserted rows are visible in
    # subsequent queries for each database.
    print(f"\nReal-time Data Visibility:")
    print(f"  MySQL: {'New Data Visible' if result.mysql_new_data_visible else 'New Data not visible'}")
    print(f"  ClickHouse: {'New Data Visible' if result.clickhouse_new_data_visible else 'New Data not visible'}")

    # Add a high-level recommendation based on performance comparison.
    print(f"\nReal-time Analytics Score:")
    if query_speedup > 2 and insert_speedup > 1:
        print("  ★★★ EXCELLENT - ClickHouse is ideal for real-time analytics")
    elif query_speedup > 1.5:
        print("  ★★ GOOD - ClickHouse performs well for real-time queries")
    elif query_speedup > 1:
        print("  ★ FAIR - ClickHouse is faster but consider other factors")
    else:
        print("  • MySQL may be better for this specific workload")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments and return the parsed namespace."""
    parser = argparse.ArgumentParser(
        description="Experiment 2: Real-time Data capture and query performance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # The benchmark table name used by both MySQL and ClickHouse.
    parser.add_argument(
        "--table",
        default="Products_wide",
        help="Table name to use for the benchmark (default: Products_wide).",
    )

    # Number of synthetic rows to append during this experiment.
    # This value is used by both database insert helpers.
    parser.add_argument(
        "--insert-rows",
        type=int,
        default=10000,
        help="Number of synthetic rows to insert into each database.",
    )

    # How many times to execute the recent-data query after inserts.
    # Multiple runs help capture latency variance instead of a single sample.
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of repeated query executions after insert.",
    )

    # Optional target size for the starting dataset. If provided, the bootstrap
    # step will ensure at least this many rows exist before inserting new rows.
    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help="Minimum number of rows to bootstrap before inserting new data.",
    )

    # If set, only bootstrap the database tables and exit without running the
    # insert/query experiment.
    parser.add_argument(
        "--bootstrap-only",
        action="store_true",
        help="Only bootstrap the benchmark tables and exit without running the experiment.",
    )

    return parser.parse_args()


def main() -> None:
    """Application entry point that runs the experiment or bootstraps data."""
    args = parse_args()

    # If the user only wants to build or refresh the base dataset, do that and exit.
    if args.bootstrap_only:
        rows = bootstrap_benchmarks(args.table, row_target=args.rows, rebuild=True)
        print(f"Bootstrapped table '{args.table}' with {rows:,} rows")
        return

    # Otherwise, run the real-time experiment and produce a report.
    result = run_real_time_experiment(
        table_name=args.table,
        new_rows=args.insert_rows,
        query_runs=args.runs,
        base_rows=args.rows,
    )

    print_report(result)


if __name__ == "__main__":
    main()