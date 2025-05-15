Benchmark for Databend and Snowflake concurrency

# Requirements

## bendsql

Install [bendsql](https://github.com/datafuselabs/bendsql)
 
Config:
```bash
export BENDSQL_DSN="databend://<user>:<pwd>@<host>:443/?tenant=<tenant>&warehouse=<warehouse>"
```

For example:
```bash
export BENDSQL_DSN="databend://user:pwd@gw.aws-us-east-2.default.databend.com/?tenant=tenant_t1&warehouse=bh-v224"
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
python3 bench.py --help

options:
  -h, --help            show this help message and exit
  --runbend             Run only bendsql queries
  --runsnow             Run only snowsql queries
  --total TOTAL         Total number of operations
  --threads THREADS     Number of processes to use
```

# Example

## Databend

```bash
python bench.py --runbend --total 10000000 --threads 100
```


## Snowflake

```bash
python bench.py --runsnow --total 10000000 --threads 100
```
