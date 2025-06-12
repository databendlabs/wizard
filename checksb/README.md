
Check the operation behaves as expected, by comparing the result of the operation with snowflake.

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

1. Install [BendSQL](https://docs.databend.com/guides/sql-clients/bendsql/#installing-bendsql)
2. Install [SnowSQL](https://docs.snowflake.com/en/user-guide/snowsql-install-config)

# Configuration

### BendSQL

Set up your Databend connection:

```bash
export BENDSQL_DSN='databend://<user>:<pwd>@<tenant>--<warehouse>.gw.aws-us-east-2.default.databend.com:443'
```

### SnowSQL

Configure `~/.snowsql/config` as:

```bash
[connections]          
accountname = <abcded-xx62390>
username = <name>
password = <pwd>
```

# Usage

`python3 checksb.py --database <db_name> --case <case_name|all> [options]`

## Key Options

| Flag          | Description                                                      |
|---------------|------------------------------------------------------------------|
| `--setup`     | Run setup and action scripts before checking. (Default: skipped) |
| `--runbend`   | With `--setup`, only run scripts for Databend.                   |
| `--runsnow`   | With `--setup`, only run scripts for Snowflake.                  |
| `--warehouse` | Specify Snowflake warehouse (Default: `COMPUTE_WH`).             |
| `--skip`      | Comma-separated list of benchmarks to skip (e.g., `tpcds,selects`). |

## Examples

**Run checks only (default behavior):**
```bash
# Check a single case
python3 checksb.py --database test_db --case tpcds

# Check all cases
python3 checksb.py --database test_db --case all
```

**Run setup and then checks:**
```bash
python3 checksb.py --database test_db --case tpcds --setup
```
