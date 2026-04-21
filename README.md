# DATABASEBB Experiments

This repository benchmarks MySQL vs ClickHouse for analytics-oriented workloads.

## What is included

- Experiment 1: storage footprint and analytic query latency
- Experiment 2: real-time insert + recent-data query behavior

Main scripts:
- Experiment 1 runner: Experiment1.py
- Experiment 2 runner: Experiment2.py

## Prerequisites

1. Python 3.12+ (required because the code uses modern Python type alias syntax).
2. A running MySQL server.
3. A running ClickHouse server.

Python dependencies:

~~~bash
python -m pip install --upgrade pip
python -m pip install mysql-connector-python clickhouse-connect
~~~

## Environment configuration

Create a local .env file in the project root. You can copy from .env.example and fill in values:

~~~powershell
Copy-Item .env.example .env
~~~

Example .env values:

~~~env
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=DBProj

CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=DBProj

# Optional dataset size used by bootstrap defaults
BENCHMARK_ROW_TARGET=100000
~~~

Notes:
- MySQL and ClickHouse database names should match for clean side-by-side comparison.
- If you do not set BENCHMARK_ROW_TARGET, the default is 100000 rows.

## Quick connectivity check

Run this before running benchmarks:

~~~bash
python Experiment1.py --check-connections
~~~

## Experiment 1

Purpose:
- Compares storage size and analytic query latency between MySQL and ClickHouse.

### Bootstrap only

Creates databases/tables and seeds data in both engines:

~~~bash
python Experiment1.py --bootstrap --table products_wide --rows 100000
~~~

### Run benchmark

~~~bash
python Experiment1.py --table products_wide --runs 5 --rows 100000
~~~

Useful flags:
- --table: logical table name in both databases
- --runs: measured runs after warm-up
- --rows: target dataset size

## Experiment 2

Purpose:
- Simulates real-time inserts and compares recent-data query performance.

### Bootstrap only

~~~bash
python Experiment2.py --bootstrap-only --table Products_wide --rows 100000
~~~

### Run benchmark

~~~bash
python Experiment2.py --table Products_wide --insert-rows 10000 --runs 5 --rows 100000
~~~

Useful flags:
- --insert-rows: number of new rows appended for the test
- --runs: number of post-insert query measurements
- --rows: base dataset size

## Important caveat for Experiment 2

Experiment 2 imports benchmark_db_copy.py, and that module currently expects a file named benchmark_schema_copy.sql.

If benchmark_schema_copy.sql is missing, create it by copying benchmark_schema.sql:

~~~powershell
Copy-Item benchmark_schema.sql benchmark_schema_copy.sql
~~~

Then run Experiment 2 again.

## Common issues

1. Connection failures
- Verify host/port/user/password values in .env.
- Confirm MySQL and ClickHouse services are running.

2. Python dependency errors
- Re-run dependency installation command.

3. Table or schema errors
- Re-run bootstrap command with the intended table name and row count.

## Typical workflow

1. Configure .env.
2. Verify connectivity.
3. Bootstrap dataset.
4. Run Experiment 1.
5. Run Experiment 2.
