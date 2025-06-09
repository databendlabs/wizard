
Check the operation behaves as expected, by comparing the result of the operation with snowflake.

# How it works
1. setup.sql: set up the tables and same data in Databend and Snowflake.
2. action.sql: run the same actions in Databend and Snowflake.
3. check.sql: run the check query to compare the result of the action in Databend and Snowflake.

# Requirements

## bendsql

Install [bendsql](https://docs.databend.com/guides/sql-clients/bendsql/#installing-bendsql)
 
Config:
```bash
export BENDSQL_DSN='databend://<user>:<pwd>@<tenant>--<warehouse>.gw.aws-us-east-2.default.databend.com:443'
```


## snowsql

Install [snowsql](https://docs.snowflake.com/en/user-guide/snowsql-install-config) 

Config `~/.snowsql/config` as:

```bash
[connections]          
accountname = <abcded-xx62390>
username = <name>
password = <pwd>
```

# Run

## Basic Usage

```bash
python3 checksb.py --database test_db --case selects

options:
  --run-check-only      Run only check.sql if set
  --case CASE           Case to execute (e.g., selects, mergeinto, streams, updates, deletes, all).
                        Multiple cases can be specified with comma separation (e.g., selects,streams).
                        Use 'all' to run all available test cases.
  --runbend             Run only bendsql setup and action
  --runsnow             Run only snowsql setup and action
```

## Running Multiple Cases

You can run multiple test cases by separating them with commas:

```bash
python3 checksb.py --database test_db --case selects,streams
```

## Running All Available Cases

To run all available test cases:

```bash
python3 checksb.py --database test_db --case all
```
