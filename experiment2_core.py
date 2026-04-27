from __future__ import annotations

import time
from benchmark_db_copy import bootstrap_benchmarks
from experiment2_utilities import (
    RealTimeResult,
    insert_new_data_mysql,
    insert_new_data_clickhouse,
    query_recent_data_mysql,
    query_recent_data_clickhouse,
    get_total_rows_mysql,
    get_total_rows_clickhouse,
)


def run_real_time_experiment(
        table_name: str,
        new_rows: int = 10000,
        query_runs: int = 5,
        base_rows: int | None = None

) -> RealTimeResult:
    """Run a real-time insert/query performance experiment on two databases.

    This experiment is intended to compare MySQL and ClickHouse for:
      1. Insert performance for a batch of synthetic rows.
      2. Read/query latency for recently inserted data.
      3. Visibility of newly inserted rows after insert operations.

    Parameters:
      table_name: The benchmark table name in both databases.
      new_rows: Number of rows to insert into each database.
      query_runs: How many times to run the recent-data query for each database.
      base_rows: Optional minimum number of rows to prepare before inserting new data.

    Returns:
      RealTimeResult containing insert timing, query timing, and visibility results.
    """

    # Prepare the base dataset and ensure the target table has at least the requested row count.
    # The `bootstrap_benchmarks` function may create the table or append rows if needed.
    print(f"\nSetting up base data...")
    actual_base_rows = bootstrap_benchmarks(table_name, row_target=base_rows, rebuild=False)

    # Determine a safe start ID for the new synthetic rows.
    # We use the larger row count from either database to avoid duplicate primary keys.
    mysql_current_max = get_total_rows_mysql(table_name)
    ch_current_max = get_total_rows_clickhouse(table_name)
    start_id = max(mysql_current_max, ch_current_max) + 1

    # Insert new rows into both databases and capture total elapsed time for each.
    # The returned values are measured in milliseconds.
    mysql_insert_time = insert_new_data_mysql(table_name, start_id, new_rows)
    ch_insert_time = insert_new_data_clickhouse(table_name, start_id, new_rows)

    # Initialize collections for query timing and row count measurements.
    mysql_query_times: list[float] = []
    ch_query_times: list[float] = []
    mysql_row_counts: list[int] = []
    ch_row_counts: list[int] = []

    # Run repeated queries to capture variance over multiple executions.
    for i in range(query_runs):
        # Query MySQL for data inserted within the last `lookback_seconds`.
        mysql_count, mysql_time = query_recent_data_mysql(table_name)
        mysql_query_times.append(mysql_time)
        mysql_row_counts.append(mysql_count)

        # Query ClickHouse for the same recent data window.
        ch_count, ch_time = query_recent_data_clickhouse(table_name)
        ch_query_times.append(ch_time)
        ch_row_counts.append(ch_count)

        # Sleep briefly between iterations to reduce caching or connection effects
        # that could make consecutive timing measurements misleading.
        if i < query_runs - 1:
            time.sleep(0.5)

    # Re-read the total rows for each database after inserts.
    # This verifies whether insertion completed and whether the row count is higher
    # than the original prepared base dataset.
    mysql_final_rows = get_total_rows_mysql(table_name)
    ch_final_rows = get_total_rows_clickhouse(table_name)

    # Compare final row counts against the base row count to detect visibility.
    # If the final rows exceed the base rows, the insert is likely visible to reads.
    mysql_new_visible = mysql_final_rows > actual_base_rows
    ch_new_visible = ch_final_rows > actual_base_rows

    # Return all metrics and visibility flags in a single structured object.
    return RealTimeResult(
        base_row_count=actual_base_rows,
        new_rows_inserted=new_rows,
        mysql_insert_total_ms=mysql_insert_time,
        mysql_insert_avg_ms=mysql_insert_time / new_rows,
        clickhouse_insert_total_ms=ch_insert_time,
        clickhouse_insert_avg_ms=ch_insert_time / new_rows,
        mysql_query_ms=mysql_query_times,
        clickhouse_query_ms=ch_query_times,
        mysql_new_data_visible=mysql_new_visible,
        clickhouse_new_data_visible=ch_new_visible,
        mysql_rows_after_query=mysql_final_rows,
        clickhouse_rows_after_query=ch_final_rows,
    )