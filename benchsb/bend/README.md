# DatabendCloud

[Apply for a free trial](https://databend.com/apply)

# bendsql

https://github.com/datafuselabs/bendsql

**version >= v0.8.0**

# Config

```
export BENDSQL_DSN="databend://<user>:<pwd>@<host>:443/<database>"
```

# Preapre

[prepare.sql](prepare.sql)

# Run

```
python3 ./bend.py [--warehouse <warehousename>] [--nosuspend]
```
* `--warehouse`: warehouse name, default is fetch from BENDSQL_DSN.
* `--nosuspend`: do not suspend warehouse before a query, default is to suspend warehouse before a query.
