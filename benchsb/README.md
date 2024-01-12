# benchsb

<b>Bench</b>marking for the <b>s</b>now and <b>b</b>end TPC-H queries.

# Test method
- TPC-H SF100, [data is from redshift](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/CloudDataWarehouseBenchmark/Cloud-DWB-Derived-from-TPCH), and loaded into [Databend](./bend/prepare.sql)/[Snowflake](./snow/prepare.sql).
- Each run, suspend the warehouse, and resume it before the query runs, this is to avoid any cache effect.
- Each run time is the server side time.

# How to run

Before run, you should read the README.md in each folder([bend](./bend/README.md), [snow](./snow/README.md)), and prepare the data and config.

## Databend

Setup:
```
python3 ./benchsb.py --database tpch_sf100 --setup --runbend
```
Run:
```sql
python3 ./benchsb.py --database tpch_sf100 --runbend
```

## Snowflake
Setup:
```
python3 ./benchsb.py --database tpch_sf100 --setup --runsnow
```
Run:
```sql
python3 ./benchsb.py --database tpch_sf100 --runsnow
```