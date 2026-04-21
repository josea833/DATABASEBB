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

@dataclass
class RealTimeResult:
    base_row_count: int
    new_rows_inserted: int

    mysql_insert_total_ms: float
    mysql_insert_avg_ms: float
    clickhouse_insert_total_ms: float
    clickhouse_insert_avg_ms: float

    mysql_query_ms: list[float]
    clickhouse_query_ms: list[float]

    mysql_new_data_visible: bool
    clickhouse_new_data_visible: bool
    mysql_rows_after_query: int
    clickhouse_rows_after_query: int


def insert_new_data_mysql(table_name: str, start_id: int, row_count: int) -> float:
    cfg = load_mysql_config()
    conn = connect_mysql(cfg)

    insert_sql = f"""
    INSERT INTO {table_name}(
        id, sku, product_name, category, brand, color_type,
        size_label, material, price, weight_grams, stock_quantity,
        description, created_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    start_time = time.perf_counter()
    cur = conn.cursor()

    batch_size = 1000
    for i in range(0, row_count, batch_size):
        batch_rows = min(batch_size, row_count - i)
        rows = generate_benchmark_rows(start_id + i, batch_rows)
        cur.executemany(insert_sql, rows)
        conn.commit()

    cur.close()
    conn.close()

    return (time.perf_counter() - start_time) * 1000.0


def insert_new_data_clickhouse(table_name: str, start_id: int, row_count: int) -> float:
    cfg = load_clickhouse_config()
    client = connect_clickhouse(cfg)

    start_time = time.perf_counter()

    column_names = ['id', 'sku', 'product_name', 'category', 'brand', 
                   'color_type', 'size_label', 'material', 'price', 
                   'weight_grams', 'stock_quantity', 'description', 'created_at']

    batch_size = 5000
    for i in range(0, row_count, batch_size):
        batch_rows = min(batch_size, row_count - i)
        rows = generate_benchmark_rows(start_id + i, batch_rows)
        
        # Convert created_at from string to datetime for ClickHouse
        converted_rows = []
        for row in rows:
            row_list = list(row)
            # Convert the last element (created_at) from string to datetime
            row_list[-1] = datetime.strptime(row_list[-1], "%Y-%m-%d %H:%M:%S")
            converted_rows.append(tuple(row_list))
        
        # Insert with converted rows
        client.insert(
            table=f"{table_name}",
            data=converted_rows,
            column_names=column_names
        )

    client.close()
    return (time.perf_counter() - start_time) * 1000.0


def query_recent_data_mysql(table_name: str, lookback_seconds: int = 60) -> tuple[int, float]:
    sql = f"""
    SELECT COUNT(*), MAX(created_at),  SUM(stock_quantity)
    FROM {table_name}
    where created_at >= DATE_SUB(NOW(), INTERVAL {lookback_seconds} SECOND)
    """

    start = time.perf_counter()
    conn = connect_mysql(load_mysql_config())
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    cur.close()
    conn.close()

    return int(row[0]) if row else 0, elapsed_ms

def query_recent_data_clickhouse(table_name:str, lookback_seconds: int = 60) -> tuple[int, float]:
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
    row = result.result_rows[0] if result.result_rows else [0, None, 0]
    return int(row[0]), elapsed_ms


def get_total_rows_mysql(table_name:str) -> int:
    conn = connect_mysql(load_mysql_config())
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = int(cur.fetchone()[0])
    cur.close()
    conn.close()
    return count

def get_total_rows_clickhouse(table_name:str) -> int:
    client = connect_clickhouse(load_clickhouse_config())
    result = client.query(f"SELECT COUNT(*) FROM {table_name}")
    count = int(result.result_rows[0][0])
    client.close()
    return count