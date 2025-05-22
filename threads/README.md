# SQL Concurrency Benchmark

Benchmark tool for testing SQL query concurrency with customizable test cases.

## Requirements

### bendsql

Install [bendsql](https://github.com/datafuselabs/bendsql)
 
Config:
```bash
export BENDSQL_DSN="databend://<user>:<pwd>@<host>:443/?tenant=<tenant>&warehouse=<warehouse>"
```

For example:
```bash
export BENDSQL_DSN="databend://user:pwd@gw.aws-us-east-2.default.databend.com/?tenant=tenant_t1&warehouse=bh-v224"
```

### snowsql

Install [snowsql](https://docs.snowflake.com/en/user-guide/snowsql.html) 

Config `~/.snowsql/config` as:

```bash
[connections]          
accountname = <abcded-xx62390>
username = <n>
password = <pwd>
```

## Run

```bash
python3 bench.py --help

options:
  -h, --help            show this help message and exit
  --runbend             Run benchmark using bendsql
  --runsnow             Run benchmark using snowsql
  --total TOTAL         Total number of operations to execute
  --threads THREADS     Number of concurrent threads
  --case CASE           Test case to run (e.g., 'select')
```

## Test Cases

The benchmark supports different test cases through the `--case` parameter. Each case consists of:

1. `setup.sql` - SQL statements for initializing the database and tables
2. `action.sql` - SQL query that will be executed concurrently by multiple threads

Test cases are stored in the `cases/` directory, with each case in its own subdirectory:

```
cases/
└── select/
    ├── setup.sql    # Database initialization
    └── action.sql   # Query to benchmark
```

### Creating a New Test Case

To create a new test case:

1. Create a new directory under `cases/` (e.g., `cases/join/`)
2. Add a `setup.sql` file with initialization statements
3. Add an `action.sql` file with the SQL query to benchmark

## Examples

### Databend Select Query Benchmark

```bash
python bench.py --runbend --total 100 --threads 20 --case select
```

### Snowflake Select Query Benchmark

```bash
python bench.py --runsnow --total 100 --threads 20 --case select
```

## Output

The benchmark provides detailed concurrency metrics including:

- Concurrency levels (average and peak)
- Throughput (operations per second)
- Operation execution times
- Stability and consistency metrics
