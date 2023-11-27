
Check the operation behaves as expected, by comparing the result of the operation with snowflake.

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
python3 checksb.py --database mergeinto --warehouse COMPUTE_WH --case mergeinto

options:
  --run-check-only      Run only check.sql if set
```