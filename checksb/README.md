
Check the operation behaves as expected, by comparing the result of the operation with snowflake.

# How it works
1. setup.sql: set up the tables and same data in Databend and Snowflake.
2. action.sql: run the same actions in Databend and Snowflake.
3. check.sql: run the check query to compare the result of the action in Databend and Snowflake.

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

## Configuration

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

# Run

To run all available test cases:

```bash
python3 checksb.py --database test_db --case all
```
