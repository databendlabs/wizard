# Wizard

A GPT-4 powered tool for detecting bugs in Databend and benchmarking database performance.

## Installation

### Prerequisites

This project contains several tools that require BendSQL and/or SnowSQL CLI tools.

```bash
# Clone the repository
git clone https://github.com/datafuselabs/wizard.git
cd wizard
```

### CLI Tools Installation

For Ubuntu/Debian users, we provide an automated installer for BendSQL and SnowSQL:

```bash
cd cli
python3 install_bendsql_snowsql.py
```

See [CLI README](./cli/README.md) for more details.

### Configuration

Most tools require configuration for database connections:

#### BendSQL

```bash
export BENDSQL_DSN="databend://<user>:<pwd>@<host>:443/?tenant=<tenant>&warehouse=<warehouse>"
```

#### SnowSQL

Configure `~/.snowsql/config`:

```
[connections]
accountname = <account-id>
username = <username>
password = <password>
```

## Project Components

- **benchsb**: Benchmark tool for TPC-H queries on Databend and Snowflake
- **cli**: Installer for BendSQL and SnowSQL CLI tools
- **job**: Job runner for executing SQL queries with different analyze methods
- **spill**: Tool for testing spill-to-disk functionality
- **threads**: SQL concurrency benchmark tool
- **double_check**: Verification tool for SQL query results
