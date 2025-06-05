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
- Each run, suspend the warehouse, and resume it before the query runs, this is to avoid any cache effect.
- Each run time is the server side time.

## TPC-DS
- TPC-DS SF100, data is loaded from S3 into [Databend](./sql/bend/tpcds_setup.sql)/[Snowflake](./sql/snow/tpcds_setup.sql).
- Uses the same warehouse suspension/resumption strategy as TPC-H to avoid cache effects.
- Server-side execution time is measured for accurate performance comparison.

# How to run

Before running, you should prepare the data and config:

## Databend

Setup:
```
python3 ./benchsb.py --database tpch_sf100 --setup --runbend
```
Run:
```sql
python3 ./benchsb.py --database tpch_sf100 --runbend
```

### TPC-H Benchmark
```bash
python3 ./benchsb.py --database tpch_sf100 --runbend
```

### TPC-DS Benchmark
Add the `--tpcds` flag to run TPC-DS benchmark instead of TPC-H:
```bash
python3 ./benchsb.py --tpcds --database tpcds_100 --runbend
```

When using the `--tpcds` flag with `--setup`, the script will automatically use the TPC-DS setup files instead of TPC-H:
```bash
python3 ./benchsb.py --tpcds --database tpcds_100 --setup --runbend
```

## Snowflake
Setup:
```
python3 ./benchsb.py --database tpch_sf100 --setup --runsnow
```
### TPC-H Benchmark
```bash
python3 ./benchsb.py --database tpch_sf100 --runsnow
```

### TPC-DS Benchmark
Add the `--tpcds` flag to run TPC-DS benchmark instead of TPC-H:
```bash
python3 ./benchsb.py --tpcds --database tpcds_100 --runsnow
```

When using the `--tpcds` flag with `--setup`, the script will automatically use the TPC-DS setup files instead of TPC-H:
```bash
python3 ./benchsb.py --tpcds --database tpcds_100 --setup --runsnow
```
