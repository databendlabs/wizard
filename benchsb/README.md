# benchsb

<b>Bench</b>marking for the <b>s</b>now and <b>b</b>end TPC-H and TPC-DS queries.

# Installation

## Prerequisites

This tool requires both BendSQL and SnowSQL CLI tools to be installed.

### Automated Installation (Ubuntu/Debian)

Use our CLI tools installer:

```bash
# From the wizard repository root
cd cli
python3 install_bendsql_snowsql.py
```

### Manual Installation

1. Install [BendSQL](https://github.com/datafuselabs/bendsql)
2. Install [SnowSQL](https://docs.snowflake.com/en/user-guide/snowsql.html)

## Configuration

### BendSQL

Set up your Databend connection:

```bash
export BENDSQL_DSN="databend://<user>:<pwd>@<host>:443/?tenant=<tenant>&warehouse=<warehouse>"
```

### SnowSQL

Configure `~/.snowsql/config`:

```
[connections]
accountname = <account-id>
username = <username>
password = <password>
```

# Test method

## TPC-H
- TPC-H SF100, [data is from redshift](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/CloudDataWarehouseBenchmark/Cloud-DWB-Derived-from-TPCH), and loaded into [Databend](./sql/bend/setup.sql)/[Snowflake](./sql/snow/setup.sql).
- Each run, suspend the warehouse, and resume it before the query runs, this is to avoid any cache effect. Use the `--suspend` flag to enable this behavior.
- Each run time is the server side time.

## TPC-DS
- TPC-DS SF100, [data is from redshift](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/CloudDataWarehouseBenchmark/Cloud-DWB-Derived-from-TPCDS), and loaded into [Databend](./sql/bend/tpcds_setup.sql)/[Snowflake](./sql/snow/tpcds_setup.sql).
- Uses the same warehouse suspension/resumption strategy as TPC-H to avoid cache effects. Use the `--suspend` flag to enable this behavior.
- Server-side execution time is measured for accurate performance comparison.

# How to run

Before running, you should prepare the data and config. Below are the command examples for different scenarios.

## Command Examples

### TPC-H Benchmark

#### Setup

| Database  | Command |
|-----------|--------|
| Databend  | `python3 ./benchsb.py --case tpch --database tpch_100 --setup --runbend` |
| Snowflake | `python3 ./benchsb.py --case tpch --database tpch_100 --setup --runsnow` |

#### Run (Normal)

| Database  | Command |
|-----------|--------|
| Databend  | `python3 ./benchsb.py --case tpch --database tpch_100 --runbend` |
| Snowflake | `python3 ./benchsb.py --case tpch --database tpch_100 --runsnow` |

#### Cold Run (With Warehouse Suspension)

| Database  | Command |
|-----------|--------|
| Databend  | `python3 ./benchsb.py --case tpch --database tpch_100 --runbend --suspend` |
| Snowflake | `python3 ./benchsb.py --case tpch --database tpch_100 --runsnow --suspend` |

### TPC-DS Benchmark

#### Setup

| Database  | Command |
|-----------|--------|
| Databend  | `python3 ./benchsb.py --case tpcds --database tpcds_100 --setup --runbend` |
| Snowflake | `python3 ./benchsb.py --case tpcds --database tpcds_100 --setup --runsnow` |

#### Run (Normal)

| Database  | Command |
|-----------|--------|
| Databend  | `python3 ./benchsb.py --case tpcds --database tpcds_100 --runbend` |
| Snowflake | `python3 ./benchsb.py --case tpcds --database tpcds_100 --runsnow` |

#### Cold Run (With Warehouse Suspension)

| Database  | Command |
|-----------|--------|
| Databend  | `python3 ./benchsb.py --case tpcds --database tpcds_100 --runbend --suspend` |
| Snowflake | `python3 ./benchsb.py --case tpcds --database tpcds_100 --runsnow --suspend` |

## 🔥 Flamegraph Generation (Databend Only)

Generate interactive performance flamegraphs for your queries.

### Usage

```bash
# Basic flamegraph generation
python3 ./benchsb.py --case tpch --database tpch_100 --runbend --flamegraph


### Notes

- Flamegraph generation is **only available with Databend** (`--runbend`)
- Requires Databend version that supports `EXPLAIN PERF`
- Flamegraphs are generated for query execution phase only (not setup)
- All content is self-contained in a single HTML file
- No external server or dependencies required for viewing

Example output:

```
Queries Execution Summary (snowsql):
----------------------------------------
Total queries: 99
Successful queries: 99
Failed queries: 0
Total server execution time: 409.66s
Total warehouse restart time: 0.00s
Total wall clock time: 495.74s
Average query time (server): 4.14s

Queries completed. Total execution time: 409.66s, Wall time: 495.74s

============================================================
FINAL BENCHMARK SUMMARY - SNOWSQL
============================================================
Database: tpcds_100
Warehouse: COMPUTE_WH
Timestamp: 2025-06-05 11:37:25.258799
============================================================

QUERIES PHASE:
  - Queries: 99/99 successful
  - Server execution time: 409.66s
  - Warehouse restart time: 0.00s
  - Total wall time: 495.74s

OVERALL:
  - Total server execution time: 409.66s
  - Total warehouse restart time: 0.00s
  - Total benchmark time: 496.67s
```