import subprocess
import re
from pathlib import Path
import argparse  # Import argparse module
import time


def execute_sql(query, warehouse):
    """Execute an SQL query using snowsql."""
    command = [
        "snowsql",
        "--warehouse",
        warehouse,
        "--schemaname",
        "PUBLIC",
        "--dbname",
        "tpch_sf100",
        "-q",
        query,
    ]

    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"snowsql command failed: {e.stderr}")


def extract_time(output):
    """Extract execution time from the snowsql output."""
    match = re.search(r"Time Elapsed:\s*([0-9.]+)s", output)
    return match.group(1) if match else None


def restart_warehouse(warehouse):
    """Restart a specific warehouse by suspending and then resuming it."""
    alter_suspend = f"ALTER WAREHOUSE {warehouse} SUSPEND;"

    print(f"Suspending warehouse {warehouse}...")
    execute_sql(alter_suspend, warehouse)

    time.sleep(5)
    execute_sql(f"SELECT 1;", warehouse)
    print(f"Resuming warehouse {warehouse}...")


def main():
    parser = argparse.ArgumentParser(
        description="Execute SQL queries and optionally restart the warehouse."
    )
    parser.add_argument(
        "--nosuspend",
        action="store_false",
        default=True,
        dest="suspend",
        help="Do not run the alter statement to suspend the warehouse",
    )
    parser.add_argument(
        "--warehouse",
        default="COMPUTE_WH",
        help="Specify the warehouse to use. Defaults to COMPUTE_WH",
    )
    args = parser.parse_args()

    sql_file_path = Path("./queries.sql")

    if not sql_file_path.exists():
        print("SQL file does not exist.")
        return

    with open(sql_file_path, "r") as f:
        content = f.read()

    queries = [query.strip() for query in content.split(";") if query.strip()]
    results = []

    # Disable caching of results
    execute_sql("ALTER ACCOUNT SET USE_CACHED_RESULT=FALSE;", args.warehouse)
    with open("query_results.txt", "w") as result_file:
        for index, query in enumerate(queries):
            print(f"Executing SQL-{index + 1}: {query}")

            if args.suspend:  # Check if --nosuspend option is not present
                restart_warehouse(args.warehouse)

            print("Executing query...\n")
            output = execute_sql(query, args.warehouse)

            time_elapsed = extract_time(output)
            if time_elapsed:
                print(f"Time Elapsed: {time_elapsed}s\n")
                result_file.write(
                    f"SQL-{index + 1}: {query}\nTime Elapsed: {time_elapsed}s\n\n"
                )
                results.append(time_elapsed)
            else:
                print("Could not extract time from output.\n")
                result_file.write(
                    f"SQL-{index + 1}: {query}\nTime Elapsed: Unknown\n\n"
                )
                results.append(f"SQL-{index + 1}: Unknown")

    print("Overall Execution Results:")
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
