# benchsb

<b>Bench</b>marking for the <b>s</b>now and <b>b</b>end TPC-H queries.

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
- TPC-H SF100, [data is from redshift](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/CloudDataWarehouseBenchmark/Cloud-DWB-Derived-from-TPCH), and loaded into [Databend](./bend/prepare.sql)/[Snowflake](./snow/prepare.sql).
- Each run, suspend the warehouse, and resume it before the query runs, this is to avoid any cache effect.
- Each run time is the server side time.

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

## Snowflake
Setup:
```
python3 ./benchsb.py --database tpch_sf100 --setup --runsnow
```
Run:
```sql
python3 ./benchsb.py --database tpch_sf100 --runsnow
```
