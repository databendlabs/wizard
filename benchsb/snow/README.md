# Snowflake

[Start for free](https://signup.snowflake.com/)

# snowsql

https://docs.snowflake.com/en/user-guide/snowsql

# Config

Config as snowsql_config to  `~/.snowsql/config`

# Preapre

[prepare.sql](prepare.sql)

# Run

```
python3 ./snow.py [--warehouse <warehousename>] [--nosuspend]
```
* `--warehouse`: warehouse name, default is `COMPUTE_WH`.
* `--nosuspend`: do not suspend warehouse before a query, default is to suspend warehouse before a query.

