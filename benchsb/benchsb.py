import argparse
import subprocess
import sys
import os
import re
import time


def get_bendsql_warehouse_from_env():
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


def execute_snowsql(query, database, warehouse):
    """Execute an SQL query using snowsql."""
    command = [
        "snowsql",
        "--warehouse",
        warehouse,
        "--schemaname",
        "PUBLIC",
        "--dbname",
        database,
        "-q",
        query,
    ]

    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"snowsql command failed: {e.stderr}")


def extract_snowsql_time(output):
    """Extract execution time from the snowsql output."""
    match = re.search(r"Time Elapsed:\s*([0-9.]+)s", output)
    return match.group(1) if match else None


def execute_bendsql(query, database):
    """Execute an SQL query using bendsql."""
    command = ["bendsql", "--query=" + query, "--database=" + database, "--time=server"]
    result = subprocess.run(command, text=True, capture_output=True)

    if "APIError: ResponseError" in result.stderr:
        raise RuntimeError(
            f"'APIError: ResponseError' found in bendsql output: {result.stderr}"
        )
    elif result.returncode != 0:
        raise RuntimeError(
            f"bendsql command failed with return code {result.returncode}: {result.stderr}"
        )

    return result.stdout


def extract_bendsql_time(output):
    """Extract execution time from the bendsql output."""
    match = re.search(r"([0-9.]+)$", output)
    return match.group(1) if match else None


def execute_sql(query, sql_tool, database, warehouse=None):
    """General function to execute a SQL query using the specified tool."""
    if sql_tool == "bendsql":
        return execute_bendsql(query, database)
    elif sql_tool == "snowsql":
        return execute_snowsql(query, database, warehouse)


def setup_database(database_name, sql_tool, warehouse):
    """Set up the database by dropping and creating it."""
    drop_query = f"DROP DATABASE IF EXISTS {database_name};"
    create_query = f"CREATE DATABASE {database_name};"
    execute_sql(drop_query, sql_tool, database_name, warehouse)
    execute_sql(create_query, sql_tool, database_name, warehouse)
    print(f"Database '{database_name}' has been set up.")


def restart_warehouse(sql_tool, warehouse, database):
    """Restart a specific warehouse by suspending and then resuming it."""
    if sql_tool == "bendsql":
        alter_suspend = f"ALTER WAREHOUSE '{warehouse}' SUSPEND;"
    else:
        alter_suspend = f"ALTER WAREHOUSE {warehouse} SUSPEND;"

    print(f"Suspending warehouse {warehouse}...")
    execute_sql(alter_suspend, sql_tool, database, warehouse)

    time.sleep(2)
    if sql_tool == "bendsql":
        alter_resume = f"ALTER WAREHOUSE '{warehouse}' RESUME;"
    else:
        alter_resume = f"ALTER WAREHOUSE {warehouse} RESUME;"

    execute_sql(alter_resume, sql_tool, database, warehouse)
    print(f"Resuming warehouse {warehouse}...")


def execute_sql_file(sql_file, sql_tool, database, warehouse, nosuspend):
    """Execute SQL queries from a file using the specified tool and write results to a file."""
    with open(sql_file, "r") as file:
        queries = [query.strip() for query in file.read().split(";") if query.strip()]

    results = []
    result_file_path = "query_results.txt"

    with open(result_file_path, "w") as result_file:
        for index, query in enumerate(queries):
            try:
                if nosuspend == False:
                    restart_warehouse(sql_tool, warehouse, database)

                output = execute_sql(query, sql_tool, database, warehouse)

                if sql_tool == "snowsql":
                    time_elapsed = extract_snowsql_time(output)
                else:
                    time_elapsed = extract_bendsql_time(output)

                print(f"Executing SQL: {query}\nTime Elapsed: {time_elapsed}s")
                result_file.write(f"SQL: {query}\nTime Elapsed: {time_elapsed}s\n\n")
                results.append(time_elapsed)
            except Exception as e:
                print(e)
                result_file.write(f"SQL: {query}\nError: {e}\n\n")
                results.append(f"{index + 1}|Error")

    print("Overall Execution Results:")
    for result in results:
        print(result)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Run SQL queries using bendsql or snowsql."
    )
    parser.add_argument("--database", help="Database name", required=True)
    parser.add_argument(
        "--warehouse",
        default="COMPUTE_WH",
        help="Warehouse name for snowsql",
        required=False,
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Setup the database by executing the setup SQL",
    )
    parser.add_argument(
        "--runbend", action="store_true", help="Run only bendsql setup and action"
    )
    parser.add_argument(
        "--runsnow", action="store_true", help="Run only snowsql setup and action"
    )
    parser.add_argument(
        "--nosuspend",
        default=False,
        action="store_true",
        help="Restart the warehouse before each query",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    base_sql_dir = "sql"  # Base directory for SQL files
    database = args.database

    if args.runbend:
        sql_tool = "bendsql"
        sql_dir = os.path.join(base_sql_dir, "bend")
        warehouse = get_bendsql_warehouse_from_env()
    elif args.runsnow:
        sql_tool = "snowsql"
        sql_dir = os.path.join(base_sql_dir, "snow")
        warehouse = args.warehouse
        # Disable caching of results
        execute_sql(
            "ALTER ACCOUNT SET USE_CACHED_RESULT=FALSE;", sql_tool, database, warehouse
        )
    else:
        print("Please specify --runbend or --runsnow.")
        sys.exit(1)

    if args.setup:
        setup_database(database, sql_tool, warehouse)
        setup_file = os.path.join(sql_dir, "setup.sql")
        execute_sql_file(setup_file, sql_tool, database, warehouse, True)

    queries_file = os.path.join(sql_dir, "queries.sql")
    execute_sql_file(queries_file, sql_tool, database, warehouse, args.nosuspend)


if __name__ == "__main__":
    main()
