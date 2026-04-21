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

    print(f"\n Setting up base data...")
    actual_base_rows = bootstrap_benchmarks(table_name, row_target=base_rows, rebuild=False)

    mysql_current_max = get_total_rows_mysql(table_name)
    ch_current_max = get_total_rows_clickhouse(table_name)
    start_id = max(mysql_current_max, ch_current_max) + 1

    mysql_insert_time = insert_new_data_mysql(table_name, start_id, new_rows)
    ch_insert_time = insert_new_data_clickhouse(table_name, start_id, new_rows)

    mysql_query_times = []
    ch_query_times = []
    mysql_row_counts = []
    ch_row_counts = []

    for i in range(query_runs):
        mysql_count, mysql_time = query_recent_data_mysql(table_name)
        mysql_query_times.append(mysql_time)
        mysql_row_counts.append(mysql_count)

        ch_count, ch_time = query_recent_data_clickhouse(table_name)
        ch_query_times.append(ch_time)
        ch_row_counts.append(ch_count)

        if i < query_runs - 1:
            time.sleep(0.5)

    mysql_final_rows = get_total_rows_mysql(table_name)
    ch_final_rows = get_total_rows_clickhouse(table_name)

    mysql_new_visible = mysql_final_rows > actual_base_rows
    ch_new_visible = ch_final_rows > actual_base_rows

    return RealTimeResult(
        base_row_count = actual_base_rows,
        new_rows_inserted = new_rows,
        mysql_insert_total_ms = mysql_insert_time,
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