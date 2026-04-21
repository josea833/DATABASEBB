from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


type BenchmarkRow = tuple[
	int,
	str,
	str,
	str,
	str,
	str,
	str,
	str,
	float,
	int,
	int,
	str,
	str,
]


_DOTENV_LOADED = False


@dataclass
class MysqlConfig:
	host: str
	port: int
	user: str
	password: str
	database: str


@dataclass
class ClickHouseConfig:
	host: str
	port: int
	user: str
	password: str
	database: str


BENCHMARK_COLUMNS = [
	"id",
	"sku",
	"product_name",
	"category",
	"brand",
	"color_type",
	"size_label",
	"material",
	"price",
	"weight_grams",
	"stock_quantity",
	"description",
	"created_at",
]


DEFAULT_ROW_TARGET = 100_000
INSERT_BATCH_SIZE = 5_000

CATEGORIES = ["Outerwear", "Apparel", "Accessories", "Bags", "Footwear", "Fitness"]
BRANDS = ["Northwind", "Alta", "Meridian", "Pioneer", "Everpeak", "SummitCo"]
COLORS = ["Blue", "Black", "Silver", "Green", "White", "Red", "Gray", "Orange"]
SIZES = ["XS", "S", "M", "L", "XL", "One Size"]
MATERIALS = ["Nylon", "Cotton", "Steel", "Canvas", "Polyester", "Leather", "Mesh"]
BASE_TIMESTAMP = datetime(2026, 1, 1, 0, 0, 0)


def _env(name: str, default: str) -> str:
	_load_dotenv_once()
	return os.getenv(name, default)


def _load_dotenv_once(dotenv_path: str = ".env") -> None:
	global _DOTENV_LOADED
	if _DOTENV_LOADED:
		return

	path = Path(dotenv_path)
	if not path.exists():
		_DOTENV_LOADED = True
		return

	for raw_line in path.read_text(encoding="utf-8").splitlines():
		line = raw_line.strip()
		if not line or line.startswith("#") or "=" not in line:
			continue
		key, value = line.split("=", 1)
		key = key.strip()
		value = value.strip().strip('"').strip("'")
		os.environ.setdefault(key, value)

	_DOTENV_LOADED = True


def load_mysql_config() -> MysqlConfig:
	return MysqlConfig(
		host=_env("MYSQL_HOST", "127.0.0.1"),
		port=int(_env("MYSQL_PORT", "3306")),
		user=_env("MYSQL_USER", "root"),
		password=_env("MYSQL_PASSWORD", ""),
		database=_env("MYSQL_DATABASE", "benchmark_db"),
	)


def load_clickhouse_config() -> ClickHouseConfig:
	return ClickHouseConfig(
		host=_env("CLICKHOUSE_HOST", "127.0.0.1"),
		port=int(_env("CLICKHOUSE_PORT", "8123")),
		user=_env("CLICKHOUSE_USER", "default"),
		password=_env("CLICKHOUSE_PASSWORD", ""),
		database=_env("CLICKHOUSE_DATABASE", "benchmark_db"),
	)


def load_benchmark_row_target() -> int:
	value = int(_env("BENCHMARK_ROW_TARGET", str(DEFAULT_ROW_TARGET)))
	return max(10_000, value)


def generate_benchmark_rows(start_id: int, count: int) -> list[BenchmarkRow]:
	rows: list[BenchmarkRow] = []
	for item_id in range(start_id, start_id + count):
		category = CATEGORIES[item_id % len(CATEGORIES)]
		brand = BRANDS[item_id % len(BRANDS)]
		color = COLORS[item_id % len(COLORS)]
		size = SIZES[item_id % len(SIZES)]
		material = MATERIALS[item_id % len(MATERIALS)]
		price = round(15.0 + ((item_id * 17) % 8_000) / 100.0, 2)
		weight_grams = 120 + ((item_id * 37) % 1_800)
		stock_quantity = 5 + ((item_id * 19) % 500)
		created_at = (BASE_TIMESTAMP + timedelta(minutes=item_id % 500_000)).strftime("%Y-%m-%d %H:%M:%S")
		rows.append(
			(
				item_id,
				f"SKU-{item_id:07d}",
				f"{brand} {category} Item {item_id}",
				category,
				brand,
				color,
				size,
				material,
				price,
				weight_grams,
				stock_quantity,
				f"Synthetic benchmark row {item_id} for compression and scan testing.",
				created_at,
			)
		)
	return rows


def connect_mysql(cfg: MysqlConfig):
	try:
		import mysql.connector
	except ImportError as exc:
		raise RuntimeError(
			"mysql-connector-python is required. Install with: pip install mysql-connector-python"
		) from exc

	try:
		return mysql.connector.connect(
			host=cfg.host,
			port=cfg.port,
			user=cfg.user,
			password=cfg.password,
			database=cfg.database,
			autocommit=True,
		)
	except Exception as exc:
		raise RuntimeError(
			f"MySQL connection failed for {cfg.user}@{cfg.host}:{cfg.port}/{cfg.database}: {exc}"
		) from exc


def connect_mysql_server(cfg: MysqlConfig):
	try:
		import mysql.connector
	except ImportError as exc:
		raise RuntimeError(
			"mysql-connector-python is required. Install with: pip install mysql-connector-python"
		) from exc

	try:
		return mysql.connector.connect(
			host=cfg.host,
			port=cfg.port,
			user=cfg.user,
			password=cfg.password,
			autocommit=True,
		)
	except Exception as exc:
		raise RuntimeError(
			f"MySQL server connection failed for {cfg.user}@{cfg.host}:{cfg.port}: {exc}"
		) from exc


def connect_clickhouse(cfg: ClickHouseConfig, database: str | None = None):
	try:
		import clickhouse_connect
	except ImportError as exc:
		raise RuntimeError(
			"clickhouse-connect is required. Install with: pip install clickhouse-connect"
		) from exc

	db = database if database is not None else cfg.database
	try:
		return clickhouse_connect.get_client(
			host=cfg.host,
			port=cfg.port,
			username=cfg.user,
			password=cfg.password,
			database=db,
		)
	except Exception as exc:
		raise RuntimeError(
			f"ClickHouse connection failed for {cfg.user}@{cfg.host}:{cfg.port}/{db}: {exc}"
		) from exc


def _load_schema_sql(section: str, table: str, db: str) -> list[str]:
	"""Return DDL statements for *section* from benchmark_schema_copy.sql.

	Skips CREATE DATABASE and USE statements (those are handled inline).
	Substitutes {table} and {db} placeholders with the supplied values.
	"""
	sql_path = Path(__file__).parent / "benchmark_schema_copy.sql"
	text = sql_path.read_text(encoding="utf-8")

	section_markers = {
		"mysql": "-- MySQL (InnoDB)",
		"clickhouse": "-- ClickHouse (MergeTree)",
	}
	end_markers = {
		"mysql": "-- ClickHouse (MergeTree)",
		"clickhouse": "-- Notes",
	}

	start_marker = section_markers[section]
	end_marker = end_markers[section]
	start = text.index(start_marker) + len(start_marker)
	end_pos = text.find(end_marker, start)
	end = end_pos if end_pos != -1 else len(text)
	section_text = text[start:end]

	# Strip comment lines BEFORE splitting by semicolon so that comments
	# containing semicolons don't create phantom statement fragments.
	stripped_lines = [ln for ln in section_text.splitlines() if not ln.strip().startswith("--")]
	stripped_text = "\n".join(stripped_lines)

	results = []
	for raw in stripped_text.split(";"):
		stmt = raw.strip()
		if not stmt:
			continue
		upper = stmt.upper()
		if upper.startswith("CREATE DATABASE") or upper.startswith("USE "):
			continue
		stmt = stmt.replace("{table}", table).replace("{db}", db)
		results.append(stmt)
	return results


def _mysql_index_exists(cur, database: str, table_name: str, index_name: str) -> bool:
	cur.execute(
		"""
		SELECT 1
		FROM information_schema.statistics
		WHERE table_schema = %s AND table_name = %s AND index_name = %s
		LIMIT 1
		""",
		(database, table_name, index_name),
	)
	return cur.fetchone() is not None


def ensure_mysql_ready(cfg: MysqlConfig, table_name: str, row_target: int, rebuild: bool = False) -> None:
	server_conn = connect_mysql_server(cfg)
	try:
		cur = server_conn.cursor()
		cur.execute(f"CREATE DATABASE IF NOT EXISTS `{cfg.database}`")
		cur.close()
	finally:
		server_conn.close()

	conn = connect_mysql(cfg)
	try:
		cur = conn.cursor()
		if rebuild:
			cur.execute(f"DROP TABLE IF EXISTS `{table_name}`")
		mysql_stmts = _load_schema_sql("mysql", table_name, cfg.database)
		cur.execute(mysql_stmts[0])
		cur.execute(f"SELECT COUNT(*), COALESCE(MAX(id), 0) FROM `{table_name}`")
		row = cur.fetchone()
		row_count = 0
		max_id = 0
		if isinstance(row, tuple) and len(row) >= 2:
			count_value = row[0]
			max_value = row[1]
			if isinstance(count_value, (int, float, str, bytes)):
				row_count = int(count_value)
			if isinstance(max_value, (int, float, str, bytes)):
				max_id = int(max_value)
		if row_count < row_target:
			insert_sql = f"""
			INSERT INTO `{table_name}` (
				id, sku, product_name, category, brand, color_type,
				size_label, material, price, weight_grams, stock_quantity,
				description, created_at
			) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
			"""
			remaining = row_target - row_count
			next_id = max_id + 1 if max_id > 0 else 1
			while remaining > 0:
				batch_size = min(INSERT_BATCH_SIZE, remaining)
				cur.executemany(insert_sql, generate_benchmark_rows(next_id, batch_size))
				next_id += batch_size
				remaining -= batch_size
		if not _mysql_index_exists(cur, cfg.database, table_name, "idx_created_at_stock"):
			cur.execute(mysql_stmts[1])
		cur.close()
	finally:
		conn.close()


def _clickhouse_literal(value: object) -> str:
	if isinstance(value, str):
		escaped = value.replace("\\", "\\\\").replace("'", "\\'")
		return f"'{escaped}'"
	return str(value)


def ensure_clickhouse_ready(cfg: ClickHouseConfig, table_name: str, row_target: int, rebuild: bool = False) -> None:
	server_client = connect_clickhouse(cfg, database="default")
	try:
		server_client.command(f"CREATE DATABASE IF NOT EXISTS {cfg.database}")
	finally:
		server_client.close()

	client = connect_clickhouse(cfg)
	try:
		if rebuild:
			client.command(f"DROP TABLE IF EXISTS {cfg.database}.{table_name} SYNC")
		ch_stmts = _load_schema_sql("clickhouse", table_name, cfg.database)
		client.command(ch_stmts[0])
		result = client.query(
			f"SELECT count(), coalesce(max(id), 0) FROM {cfg.database}.{table_name}"
		)
		row_count = int(result.result_rows[0][0]) if result.result_rows else 0
		max_id = int(result.result_rows[0][1]) if result.result_rows else 0
		if row_count < row_target:
			remaining = row_target - row_count
			next_id = max_id + 1 if max_id > 0 else 1
			while remaining > 0:
				batch_size = min(INSERT_BATCH_SIZE, remaining)
				rows = generate_benchmark_rows(next_id, batch_size)
				value_sql = ", ".join(
					"(" + ", ".join(_clickhouse_literal(value) for value in row) + ")"
					for row in rows
				)
				client.command(
					f"INSERT INTO {cfg.database}.{table_name} ({', '.join(BENCHMARK_COLUMNS)}) VALUES {value_sql}"
				)
				next_id += batch_size
				remaining -= batch_size
	finally:
		client.close()


def bootstrap_benchmarks(table_name: str, row_target: int | None = None, rebuild: bool = False) -> int:
	target = row_target if row_target is not None else load_benchmark_row_target()
	ensure_mysql_ready(load_mysql_config(), table_name, target, rebuild=rebuild)
	ensure_clickhouse_ready(load_clickhouse_config(), table_name, target, rebuild=rebuild)
	return target