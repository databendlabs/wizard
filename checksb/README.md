
Check the operation behaves as expected, by comparing the result of the operation with snowflake.

1. set up the tables and same data in Databend and Snowflake.
2. run the same actions in Databend and Snowflake.
3. run the same checks in Databend and Snowflake.

# Requirements

## bendsql

Install [bendsql](https://github.com/datafuselabs/bendsql)
 
Config:
```bash
export BENDSQL_DSN="databend://<user>:<pwd>@<host>:443/<database>?tenant=<tenant>&warehouse=<warehouse>"
```

For example:
```bash
export BENDSQL_DSN="databend://usera:pwdb@gw.aws-us-east-2.default.databend.com/mergeinto?tenant=tenant_t1&warehouse=bh-v224"
```

## snowsql

Install [snowsql](https://docs.snowflake.com/en/user-guide/snowsql.html) 

Config `~/.snowsql/config` as:

```bash
[connections]          
accountname = <abcded-xx62390>
username = <name>
password = <pwd>
```

# Run

```bash
python3 checksb.py --database mergeinto --warehouse COMPUTE_WH --case <mergeinto|selects>

options:
  --run-check-only      Run only check.sql if set
  --case CASE           Case to execute (e.g., mergeinto, selects)
  --runbend             Run only bendsql setup and action
  --runsnow             Run only snowsql setup and action
```