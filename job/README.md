# BendSQL Job Runner - Simplified Version

A redesigned and simplified BendSQL job runner.


## 🚀 Usage

### Basic Usage

```bash
# Setup database
python job_runner.py --setup

# Analyze tables
python job_runner.py --analyze standard

# Run queries
python job_runner.py --run

# Complete workflow
python job_runner.py --all

# Generate report
python job_runner.py --report
```

### Advanced Options

```bash
# Specify database
python job_runner.py --database mydb --setup

# Use histogram analysis
python job_runner.py --analyze histogram

# Specify queries directory
python job_runner.py --run --queries-dir custom_queries

# Custom workflow
python job_runner.py --setup --analyze standard --run --report
```
