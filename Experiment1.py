"""Experiment 1 benchmark harness: MySQL vs ClickHouse (compression + column scan).

This script measures:
1) Storage footprint on disk for the same logical table
2) Query latency for a performance-focused analytic workload query that combines:
	- large-range filtering (created_at and id range)
	- aggregation (COUNT, SUM, AVG)
	- grouping and ordering (GROUP BY + ORDER BY)

Run examples:
	python Experiment1.py --bootstrap --table products_wide --rows 100000
	python Experiment1.py --table products_wide --runs 5 --rows 100000
	python Experiment1.py --check-connections

It is intentionally structured so additional experiments can be added later
as separate runner functions.

Transformation is needed for clickhouse
the data must be sorted by id and created_at to achieve optimal compression and query performance,
which is done in the data loading step in benchmark_db.py. 
The MySQL table uses InnoDB with a clustered primary key on id, which also benefits from sorted data for compression and scan efficiency.
"""

from __future__ import annotations

import argparse
import statistics
import time
from dataclasses import dataclass
from typing import Iterable, List

from benchmark_db import bootstrap_benchmarks, connect_clickhouse, connect_mysql, load_benchmark_row_target, load_clickhouse_config, load_mysql_config


@dataclass
class QueryResult:
	value: int
	elapsed_ms: float


@dataclass
class ExperimentSummary:
	row_count: int
	mysql_storage_bytes: int
	clickhouse_storage_bytes: int
	mysql_total_grouped_rows: int
	clickhouse_total_grouped_rows: int
	mysql_workload_runs_ms: List[float]
	clickhouse_workload_runs_ms: List[float]


def mysql_storage_bytes(conn, database: str, table: str) -> int:
	sql = """
	SELECT COALESCE(data_length + index_length, 0)
	FROM information_schema.tables
	WHERE table_schema = %s AND table_name = %s
	"""
	cur = conn.cursor()
	cur.execute(sql, (database, table))
	row = cur.fetchone()
	cur.close()
	return int(row[0] if row else 0)


def clickhouse_storage_bytes(client, database: str, table: str) -> int:
	sql = """
	SELECT COALESCE(sum(bytes_on_disk), 0)
	FROM system.parts
	WHERE database = %(db)s AND table = %(tbl)s AND active
	"""
	result = client.query(sql, parameters={"db": database, "tbl": table})
	return int(result.result_rows[0][0] if result.result_rows else 0)


def mysql_workload_query(conn, table: str, min_id: int, max_id: int, start_ts: str, end_ts: str) -> QueryResult:
	sql = f"""
	SELECT COUNT(*)
	FROM (
		SELECT
			category,
			brand,
			color_type,
			COUNT(*) AS row_count,
			SUM(stock_quantity) AS total_stock,
			AVG(price) AS avg_price,
			SUM(price * stock_quantity) AS weighted_revenue
		FROM {table}
		WHERE id BETWEEN %s AND %s
		  AND created_at BETWEEN %s AND %s
		  AND stock_quantity >= 50
		GROUP BY category, brand, color_type
		ORDER BY weighted_revenue DESC
		LIMIT 100
	) t
	"""
	start = time.perf_counter()
	cur = conn.cursor()
	cur.execute(sql, (min_id, max_id, start_ts, end_ts))
	row = cur.fetchone()
	cur.close()
	elapsed_ms = (time.perf_counter() - start) * 1000.0
	return QueryResult(value=int(row[0]), elapsed_ms=elapsed_ms)


def clickhouse_workload_query(client, table: str, min_id: int, max_id: int, start_ts: str, end_ts: str) -> QueryResult:
	sql = f"""
	SELECT COUNT(*)
	FROM (
		SELECT
			category,
			brand,
			color_type,
			count() AS row_count,
			sum(stock_quantity) AS total_stock,
			avg(price) AS avg_price,
			sum(price * stock_quantity) AS weighted_revenue
		FROM {table}
		WHERE id BETWEEN %(min_id)s AND %(max_id)s
		  AND created_at BETWEEN %(start_ts)s AND %(end_ts)s
		  AND stock_quantity >= 50
		GROUP BY category, brand, color_type
		ORDER BY weighted_revenue DESC
		LIMIT 100
	)
	"""
	start = time.perf_counter()
	result = client.query(
		sql,
		parameters={
			"min_id": min_id,
			"max_id": max_id,
			"start_ts": start_ts,
			"end_ts": end_ts,
		},
	)
	elapsed_ms = (time.perf_counter() - start) * 1000.0
	value = int(result.result_rows[0][0])
	return QueryResult(value=value, elapsed_ms=elapsed_ms)


def mysql_row_count(conn, table: str) -> int:
	cur = conn.cursor()
	cur.execute(f"SELECT COUNT(*) FROM {table}")
	row = cur.fetchone()
	cur.close()
	return int(row[0]) if row else 0


def median(values: Iterable[float]) -> float:
	vals = list(values)
	return statistics.median(vals) if vals else 0.0


def check_connections() -> int:
	mysql_cfg = load_mysql_config()
	ch_cfg = load_clickhouse_config()
	failures = 0

	print("=== Connection Check ===")

	try:
		mysql_conn = connect_mysql(mysql_cfg)
		try:
			cur = mysql_conn.cursor()
			cur.execute("SELECT DATABASE()")
			row = cur.fetchone()
			cur.close()
			database = mysql_cfg.database
			if isinstance(row, tuple) and row and row[0] is not None:
				database = str(row[0])
			print(f"MySQL: OK ({mysql_cfg.host}:{mysql_cfg.port}, database={database})")
		finally:
			mysql_conn.close()
	except Exception as exc:
		failures += 1
		print(f"MySQL: FAIL ({exc})")

	try:
		ch_client = connect_clickhouse(ch_cfg)
		try:
			result = ch_client.query("SELECT currentDatabase()")
			database = result.result_rows[0][0] if result.result_rows else ch_cfg.database
			print(f"ClickHouse: OK ({ch_cfg.host}:{ch_cfg.port}, database={database})")
		finally:
			ch_client.close()
	except Exception as exc:
		failures += 1
		print(f"ClickHouse: FAIL ({exc})")

	return 1 if failures else 0


def run_experiment_1(table_name: str, runs: int = 5, row_target: int | None = None) -> ExperimentSummary:
	bootstrap_benchmarks(table_name, row_target=row_target)
	mysql_cfg = load_mysql_config()
	ch_cfg = load_clickhouse_config()

	mysql_conn = connect_mysql(mysql_cfg)
	ch_client = connect_clickhouse(ch_cfg)

	try:
		row_count = mysql_row_count(mysql_conn, table_name)
		mysql_storage = mysql_storage_bytes(mysql_conn, mysql_cfg.database, table_name)
		ch_storage = clickhouse_storage_bytes(ch_client, ch_cfg.database, table_name)
		filter_min_id = 1
		filter_max_id = max(1, int(row_count * 0.95))
		start_ts = "2026-01-01 00:00:00"
		end_ts = "2026-09-30 23:59:59"

		# Warm-up (not included in reported runs)
		_ = mysql_workload_query(mysql_conn, table_name, filter_min_id, filter_max_id, start_ts, end_ts)
		_ = clickhouse_workload_query(ch_client, table_name, filter_min_id, filter_max_id, start_ts, end_ts)

		mysql_workload_runs: List[float] = []
		ch_workload_runs: List[float] = []
		mysql_grouped_rows = -1
		ch_grouped_rows = -1

		for _ in range(runs):
			mysql_result = mysql_workload_query(mysql_conn, table_name, filter_min_id, filter_max_id, start_ts, end_ts)
			ch_result = clickhouse_workload_query(ch_client, table_name, filter_min_id, filter_max_id, start_ts, end_ts)
			mysql_grouped_rows = mysql_result.value
			ch_grouped_rows = ch_result.value
			mysql_workload_runs.append(mysql_result.elapsed_ms)
			ch_workload_runs.append(ch_result.elapsed_ms)

		return ExperimentSummary(
			row_count=row_count,
			mysql_storage_bytes=mysql_storage,
			clickhouse_storage_bytes=ch_storage,
			mysql_total_grouped_rows=mysql_grouped_rows,
			clickhouse_total_grouped_rows=ch_grouped_rows,
			mysql_workload_runs_ms=mysql_workload_runs,
			clickhouse_workload_runs_ms=ch_workload_runs,
		)
	finally:
		mysql_conn.close()
		ch_client.close()


def print_report(summary: ExperimentSummary) -> None:
	print(f"Rows benchmarked:         {summary.row_count}")
	mysql_storage = summary.mysql_storage_bytes
	ch_storage = summary.clickhouse_storage_bytes
	ratio = (mysql_storage / ch_storage) if ch_storage > 0 else float("inf")
	mysql_med = median(summary.mysql_workload_runs_ms)
	ch_med = median(summary.clickhouse_workload_runs_ms)
	speedup = (mysql_med / ch_med) if ch_med > 0 else float("inf")

	print("=== Experiment 1: Query 2 Workload Benchmark ===")
	print(f"MySQL storage bytes:      {mysql_storage}")
	print(f"ClickHouse storage bytes: {ch_storage}")
	print(f"Storage ratio (MySQL/CH): {ratio:.2f}x")
	print()
	print("-- Query 2: large-range filter + aggregation + grouping/order --")
	print("Query shape: GROUP BY category, brand, color_type with SUM/AVG and ORDER BY weighted_revenue")
	print(f"MySQL grouped rows returned:      {summary.mysql_total_grouped_rows}")
	print(f"ClickHouse grouped rows returned: {summary.clickhouse_total_grouped_rows}")
	print(f"MySQL runs (ms):      {[round(v, 2) for v in summary.mysql_workload_runs_ms]}")
	print(f"ClickHouse runs (ms): {[round(v, 2) for v in summary.clickhouse_workload_runs_ms]}")
	print(f"Median MySQL (ms):      {mysql_med:.2f}")
	print(f"Median ClickHouse (ms): {ch_med:.2f}")
	print(f"Latency speedup (MySQL/CH): {speedup:.2f}x")


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Run Experiment 1: MySQL vs ClickHouse for storage and COUNT DISTINCT"
	)
	parser.add_argument(
		"--table",
		default="products_wide",
		help="Table name that exists in both MySQL and ClickHouse",
	)
	parser.add_argument(
		"--runs",
		type=int,
		default=5,
		help="Number of measured runs (after one warm-up run)",
	)
	parser.add_argument(
		"--check-connections",
		action="store_true",
		help="Validate MySQL and ClickHouse connectivity and exit",
	)
	parser.add_argument(
		"--bootstrap",
		action="store_true",
		help="Create benchmark databases, tables, and seed data for MySQL and ClickHouse",
	)
	parser.add_argument(
		"--rows",
		type=int,
		default=load_benchmark_row_target(),
		help="Target row count for Experiment 1 dataset generation",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	if args.check_connections:
		raise SystemExit(check_connections())
	if args.bootstrap:
		initialized_rows = bootstrap_benchmarks(args.table, row_target=args.rows)
		print(f"Initialized MySQL and ClickHouse for table '{args.table}' with target rows={initialized_rows}")
		return
	summary = run_experiment_1(table_name=args.table, runs=args.runs, row_target=args.rows)
	print_report(summary)


if __name__ == "__main__":
	main()
