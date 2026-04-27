from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime

from benchmark_db_copy import (
    generate_benchmark_rows,
    connect_mysql,
    connect_clickhouse,
    load_mysql_config,
    load_clickhouse_config,
)

# Utility helpers used by the experiment driver.
# Each helper function isolates one database operation so the benchmark logic
# can remain simple and the same operation can be reused across experiments.
@dataclass
class RealTimeResult:
    """Structured result returned by the real-time benchmark experiment."""

    # The row count available in both tables before inserting new benchmark rows.
    base_row_count: int

    # Number of synthetic rows inserted into MySQL and ClickHouse.
    new_rows_inserted: int

    # Insert performance metrics for both databases.
    mysql_insert_total_ms: float
    mysql_insert_avg_ms: float
    clickhouse_insert_total_ms: float
    clickhouse_insert_avg_ms: float

    # A list of query durations collected across repeated runs.
    mysql_query_ms: list[float]
    clickhouse_query_ms: list[float]

    # Visibility flags that indicate whether the newly inserted rows were visible
    # after the experiment ended.
    mysql_new_data_visible: bool
    clickhouse_new_data_visible: bool

    # Final row counts observed after insert and query operations.
    mysql_rows_after_query: int
    clickhouse_rows_after_query: int


def insert_new_data_mysql(table_name: str, start_id: int, row_count: int) -> float:
    """Insert generated benchmark rows into MySQL and measure total insert time.

    This function inserts a deterministic set of synthetic rows into the
    specified MySQL table. It uses batched inserts and commits after every
    batch to keep the transaction size manageable and to emulate realistic
    append workloads.
    """
    # Load MySQL configuration values from the shared benchmark config.
    cfg = load_mysql_config()
    conn = connect_mysql(cfg)

    # Prepare the parameterized INSERT statement with all table columns.
    insert_sql = f"""
    INSERT INTO {table_name}(
        id, sku, product_name, category, brand, color_type,
        size_label, material, price, weight_grams, stock_quantity,
        description, created_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Start timing immediately before the actual inserts.
    start_time = time.perf_counter()
    cur = conn.cursor()

    # Split the work into batches instead of inserting everything in one shot.
    # This reduces memory pressure, keeps transactions smaller, and makes the
    # benchmark closer to realistic production behavior.
    batch_size = 1000
    for i in range(0, row_count, batch_size):
        batch_rows = min(batch_size, row_count - i)

        # Generate synthetic rows with consecutive IDs starting at `start_id`.
        rows = generate_benchmark_rows(start_id + i, batch_rows)

        # Use executemany to insert the entire batch in a single round trip.
        cur.executemany(insert_sql, rows)

        # Commit after each batch to persist the rows and avoid a large open
        # transaction period.
        conn.commit()

    # Close cursor and connection once all rows are inserted.
    cur.close()
    conn.close()

    # Return the elapsed insert duration in milliseconds.
    return (time.perf_counter() - start_time) * 1000.0


def insert_new_data_clickhouse(table_name: str, start_id: int, row_count: int) -> float:
    """Insert generated benchmark rows into ClickHouse and measure total insert time.

    ClickHouse can accept bulk inserts efficiently, so this function uses a
    larger batch size and converts timestamp strings into datetime objects
    before sending them to the ClickHouse client.
    """
    cfg = load_clickhouse_config()
    client = connect_clickhouse(cfg)

    start_time = time.perf_counter()

    # Ensure columns are provided in the same order as the generated tuples.
    column_names = [
        'id', 'sku', 'product_name', 'category', 'brand',
        'color_type', 'size_label', 'material', 'price',
        'weight_grams', 'stock_quantity', 'description', 'created_at'
    ]

    batch_size = 5000
    for i in range(0, row_count, batch_size):
        batch_rows = min(batch_size, row_count - i)
        rows = generate_benchmark_rows(start_id + i, batch_rows)

        # Convert created_at values from strings to datetime before insertion.
        # This ensures ClickHouse receives native datetime values rather than raw text.
        converted_rows = []
        for row in rows:
            row_list = list(row)
            row_list[-1] = datetime.strptime(row_list[-1], "%Y-%m-%d %H:%M:%S")
            converted_rows.append(tuple(row_list))

        client.insert(
            table=f"{table_name}",
            data=converted_rows,
            column_names=column_names,
        )

    client.close()
    return (time.perf_counter() - start_time) * 1000.0


def query_recent_data_mysql(table_name: str, lookback_seconds: int = 60) -> tuple[int, float]:
    """Run a query on MySQL for recently inserted rows and return count/time.

    The query counts rows whose `created_at` timestamp is within the last
    `lookback_seconds` seconds. This mimics a real-time analytics workload.
    """
    sql = f"""
    SELECT COUNT(*), MAX(created_at),  SUM(stock_quantity)
    FROM {table_name}
    WHERE created_at >= DATE_SUB(NOW(), INTERVAL {lookback_seconds} SECOND)
    """

    start = time.perf_counter()
    conn = connect_mysql(load_mysql_config())
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    cur.close()
    conn.close()

    # Return the row count and the elapsed execution time.
    return int(row[0]) if row else 0, elapsed_ms


def query_recent_data_clickhouse(table_name: str, lookback_seconds: int = 60) -> tuple[int, float]:
    """Run a query on ClickHouse for recently inserted rows and return count/time."""
    sql = f"""
    SELECT COUNT(*), MAX(created_at), SUM(stock_quantity)
    FROM {table_name}
    WHERE created_at >= now() - INTERVAL {lookback_seconds} SECOND
    """

    start = time.perf_counter()
    client = connect_clickhouse(load_clickhouse_config())
    result = client.query(sql)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    client.close()

    # ClickHouse returns rows in result_rows; use a default if no rows are returned.
    row = result.result_rows[0] if result.result_rows else [0, None, 0]
    return int(row[0]), elapsed_ms


def get_total_rows_mysql(table_name: str) -> int:
    """Return the total number of rows present in the MySQL benchmark table."""
    # Connect to MySQL using the shared benchmark configuration.
    conn = connect_mysql(load_mysql_config())
    cur = conn.cursor()

    # Run a simple count query to determine the current table size.
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = int(cur.fetchone()[0])

    # Clean up database objects once the query is complete.
    cur.close()
    conn.close()
    return count


def get_total_rows_clickhouse(table_name: str) -> int:
    """Return the total number of rows present in the ClickHouse benchmark table."""
    # Connect to ClickHouse using the shared benchmark configuration.
    client = connect_clickhouse(load_clickhouse_config())

    # Execute a simple count query to get the current number of rows.
    result = client.query(f"SELECT COUNT(*) FROM {table_name}")

    # Extract the count from the returned result rows and close the client.
    count = int(result.result_rows[0][0])
    client.close()
    return count