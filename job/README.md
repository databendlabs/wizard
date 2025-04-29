# Join Order Benchmark (JOB) for BendSQL

## Overview

The `job_runner.py` script runs the Join Order Benchmark using the IMDB dataset (21 tables, ~3.7GB) to evaluate query performance.

## Usage

```bash
python job_runner.py [--setup] [--run] [--analyze] [--accurate-histograms]
```

### Options

- `--setup`: Create schema and load data
- `--run`: Execute benchmark queries
- `--analyze`: Generate table statistics
- `--accurate-histograms`: Use detailed histograms (with `--analyze`)

### Examples

```bash
# Complete workflow
python job_runner.py --setup --analyze --accurate-histograms --run

# Only setup database
python job_runner.py --setup

# Only run queries
python job_runner.py --run
```

## Results

Benchmark results are stored in the `results` directory.
