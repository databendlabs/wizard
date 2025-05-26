# Job Runner for BendSQL

This tool consolidates the functionality of env.sh, load.sh, and run.py into a single Python file. It provides capabilities to:

1. Set up the BendSQL environment
2. Execute setup.sql to create schema and load data from public AWS bucket
3. Run SQL queries from the queries directory
4. Compare query performance with three different analyze methods

# Installation

## Prerequisites

This tool requires BendSQL CLI to be installed.

### Automated Installation (Ubuntu/Debian)

Use our CLI tools installer:

```bash
# From the wizard repository root
cd cli
python3 install_bendsql_snowsql.py
```

### Manual Installation

Install [BendSQL](https://docs.databend.com/guides/sql-clients/bendsql/#installing-bendsql) following the official documentation.

## Configuration

Set up your Databend connection:

```bash
export BENDSQL_DSN='databend://<user>:<pwd>@<tenant>--<warehouse>.gw.aws-us-east-2.default.databend.com:443'
```


## Usage


```bash
python job_runner.py --compare
```

## Comparing Analyze Methods

The `--compare` option provides a comprehensive way to compare query performance with three different analyze methods:

1. **Raw** - No analyze (tables as they are after loading)
2. **Standard Analyze** - Regular analyze without histograms
3. **Histogram Analyze** - Analyze with accurate histograms enabled

When you run:

```bash
python job_runner.py --compare
```

The tool will:

1. Set up the database using setup.sql
2. Run queries with no analyze (raw tables)
3. Analyze tables with standard analyze
4. Run queries with standard analyze
5. Analyze tables with accurate histograms
6. Run queries with histogram analyze
7. Generate comparison reports in multiple formats

### Comparison Reports

The comparison results are saved in the `results` directory with the following formats:

1. **HTML Report** (`analyze_comparison_TIMESTAMP.html`): A formatted HTML table showing:
   - Query ID
   - Execution time with raw tables (no analyze)
   - Execution time with standard analyze
   - Execution time with histogram analyze
   - Time differences (as percentages)
   - Which method was fastest
   - Summary statistics
   - Detailed histogram data for each table

2. **Text Report** (`analyze_comparison_TIMESTAMP.txt`): A plain text version of the same information.

3. **JSON Data** (`analyze_comparison_TIMESTAMP.json`): Raw data for further analysis.

### Interpreting Results

- **Blue values** in the HTML report indicate queries where raw (no analyze) was fastest
- **Green values** indicate queries where standard analyze was fastest
- **Purple values** indicate queries where histogram analyze was fastest
- The summary section shows the percentage of queries that performed better with each method

The HTML report also includes a tab to view and compare the actual histogram data between standard analyze and analyze with accurate histograms.

### Logs

Detailed logs of the comparison process are saved in the `logs` directory, showing the exact commands executed and their results.

This comparison helps you understand the impact of different analyze methods on query performance for your specific workload.
