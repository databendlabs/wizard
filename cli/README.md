# CLI Tools Installer

Installs BendSQL and SnowSQL command-line tools to `/usr/bin` on Ubuntu systems.

## Requirements
- Ubuntu/Debian Linux
- Sudo access
- Python 3.6+

## Install

Clone this repository and run the installer:

```bash
git clone https://github.com/datafuselabs/wizard.git
cd wizard/cli
python3 install_bendsql_snowsql.py
```

## Features
- Automatic architecture detection (x86_64)
- System-wide installation without PATH modification
- Verification of successful installation

## After Installation

```bash
bendsql --help
snowsql --help
```