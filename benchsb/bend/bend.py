import subprocess
import re
import sys
import os
from pathlib import Path
import argparse
import time


def execute_sql(query):
    """Execute an SQL query using bendsql(need >=0.8.0)."""
    command = ["bendsql", "--query=" + query, "--time=server"]
    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"bendsql command failed: {e.stderr}")


def extract_time(output):
    """Extract execution time from the bendsql output."""
    match = re.search(r"([0-9.]+)$", output)
    if not match:
        raise ValueError("Could not extract time from output.")
    return match.group(1)


def get_warehouse_from_env():
    """Retrieve warehouse name from the environment variable."""
    dsn = os.environ.get("BENDSQL_DSN", "")

    # Try to match the first format
    match = re.search(r"--([\w-]+)\.gw", dsn)
    if match:
        return match.group(1)

    # Try to match the second format
    match = re.search(r"warehouse=([\w-]+)", dsn)
    if match:
        return match.group(1)

    raise ValueError("Could not extract warehouse name from BENDSQL_DSN.")


def restart_warehouse(warehouse):
    """Suspend and then resume a warehouse."""
    try:
        execute_sql(f"ALTER WAREHOUSE '{warehouse}' SUSPEND;")
        print(f"Warehouse {warehouse} suspended.")

        time.sleep(5)
        execute_sql(f"SELECT 1;")
        print(f"Warehouse {warehouse} resumed.")
    except Exception as e:
        raise RuntimeError(f"Failed to suspend/resume warehouse {warehouse}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Execute SQL queries and optionally suspend the warehouse."
    )
    parser.add_argument(
        "--nosuspend",
        action="store_true",
        help="Do not suspend the warehouse before executing the query",
    )
    parser.add_argument(
        "--warehouse",
        default=None,
        help="Specify the warehouse to use. Defaults to value from DATABEND_DSN if not set",
    )
    args = parser.parse_args()

    if args.warehouse:
        warehouse = args.warehouse
    else:
        warehouse = get_warehouse_from_env()

    sql_file_path = Path("./queries.sql")
    if not sql_file_path.exists():
        sys.exit("SQL file does not exist.")

    with open(sql_file_path, "r") as f:
        content = f.read()

    queries = [query.strip() for query in content.split(";") if query.strip()]
    results = []

    with open("query_results.txt", "w") as result_file:
        for index, query in enumerate(queries):
            # Suspend the warehouse before executing the query, unless --nosuspend is specified
            if not args.nosuspend:
                restart_warehouse(warehouse)

            try:
                print(f"Executing SQL: {query}")
                output = execute_sql(query)
                time_elapsed = extract_time(output)
                print(f"Time Elapsed: {time_elapsed}s\n")
                result_file.write(f"SQL: {query}\nTime Elapsed: {time_elapsed}s\n\n")
                results.append(time_elapsed)
            except Exception as e:
                print(e)
                result_file.write(f"SQL: {query}\nError: {e}\n\n")
                results.append(f"{index + 1}|Error")

    print("Overall Execution Results:")
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
