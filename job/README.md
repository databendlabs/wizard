# Join Order Benchmark for Databend

## Load

**Download dataset**
```sh
mkdir data && cd data
curl -O https://bonsai.cedardb.com/job/imdb.tgz
tar -zxvf imdb.tgz
```
Note: The compressed tarball to download is about 1.2 GB, which decompresses to about 3.7 GB.

**Create table**

The DDL queries can be found in `schema.sql` file which contains 21 tables.

**Import data**
The arg means if need accurate histogram. If need, during analyzing table, accurate histogram will be generated.
```sh
sh load.sh [0 | 1]
```

## Run
```python
python3 run.py
```

## Comparison
The `results` dir contains basic benchmark results(without/with accurate histogram), you can open the html file to check the difference.




