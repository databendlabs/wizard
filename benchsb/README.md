# benchsb

TPC-H/TPC-DS benchmarking for Databend and Snowflake.

## Setup

**Python environment:**
```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate
```

**Install CLI tools:**
```bash
cd cli && python3 install_bendsql_snowsql.py
```

**Configure connections:**
```bash
# Databend
export BENDSQL_DSN="databend://<user>:<pwd>@<host>:443/?tenant=<tenant>&warehouse=<warehouse>"

# Snowflake (~/.snowsql/config)
[connections]
accountname = <account-id>
username = <username>
password = <password>
```

## Usage

**Setup data:**
```bash
# TPC-H
python benchsb.py --case tpch --database tpch_100 --setup --runbend
python benchsb.py --case tpch --database tpch_100 --setup --runsnow

# TPC-DS  
python benchsb.py --case tpcds --database tpcds_100 --setup --runbend
python benchsb.py --case tpcds --database tpcds_100 --setup --runsnow
```

**Run benchmarks:**
```bash
# Normal run
python benchsb.py --case tpch --database tpch_100 --runbend
python benchsb.py --case tpch --database tpch_100 --runsnow

# Cold run (restart warehouse each query)
python benchsb.py --case tpch --database tpch_100 --runbend --suspend
```

## Flamegraph

**Generate performance flamegraphs (Databend only):**
```bash
python benchsb.py --case tpch --database tpch_100 --runbend --flamegraph
```

## Features

- **TPC-H/TPC-DS SF100** benchmarks
- **Cold runs** with `--suspend` (restart warehouse per query)
- **Flamegraphs** with `--flamegraph` (Databend only)
- **Organized logs** in `log/` directory
- **Absolute paths** for easy file discovery