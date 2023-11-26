import argparse
import os
import subprocess
from termcolor import colored
import difflib


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Compare SQL execution on different databases."
    )
    parser.add_argument("--database", help="Database name", required=True)
    parser.add_argument(
        "--warehouse", help="Warehouse name for snowsql", required=False
    )
    args = parser.parse_args()
    print(f"Database selected: {args.database}")
    if args.warehouse:
        print(f"Warehouse selected for snowsql: {args.warehouse}")
    return args.database, args.warehouse


def execute_sql(query, sql_tool, database, warehouse=None):
    """Execute an SQL query using snowsql or bendsql and return the output."""
    command = [sql_tool]
    if sql_tool == "snowsql":
        # Enclosing the query in double quotes
        snowsql_query = f'"{query}"'
        command.extend(
            [
                "--query",
                snowsql_query,
                "-d",
                database,
                "-o",
                "output_format=tsv",
                "-o",
                "header=false",
                "-o",
                "timing=false",
                "-o",
                "friendly=false",
            ]
        )
        if warehouse:
            command.extend(["--warehouse", warehouse])
    elif sql_tool == "bendsql":
        command.extend(["--query=" + query, "-D", database])

    # Logging the command to be executed
    print(f"Executing command: {' '.join(command)}")

    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
        print("Command executed successfully. Output:")
        print(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        error_message = f"{sql_tool} command failed: {e.stderr}"
        print(error_message)
        raise RuntimeError(error_message)


def execute_sql_scripts(sql_tool, script_path, database, warehouse=None):
    print(f"Executing SQL scripts from: {script_path} using {sql_tool}")
    with open(script_path, "r") as file:
        sql_script = file.read()
    queries = sql_script.split(";")
    for query in queries:
        if query.strip():
            print(f"Executing query on {sql_tool}: {query}")
            # Execute each query
            execute_sql(query, sql_tool, database, warehouse)


def fetch_query_results(query, sql_tool, database, warehouse=None):
    print(f"Fetching results for {sql_tool} with query: {query}")
    result = execute_sql(query, sql_tool, database, warehouse)
    print(f"Results fetched for {sql_tool}:")
    print(result)
    return result


def main():
    database_name, warehouse = parse_arguments()

    # Read DSN from environment variable
    databend_dsn = os.environ.get("DATABEND_DSN")
    print("Checking DATABEND_DSN environment variable...")
    if not databend_dsn:
        print("Please set the DATABEND_DSN environment variable.")
        return
    else:
        print("DATABEND_DSN environment variable found.")

    # Execute setup scripts
    print("Starting setup script execution...")
    execute_sql_scripts("bendsql", "sql/bend/setup.sql", database_name)
    execute_sql_scripts("snowsql", "sql/snow/setup.sql", database_name, warehouse)

    # Execute action scripts
    print("Starting action script execution...")
    execute_sql_scripts("bendsql", "sql/action.sql", database_name)
    execute_sql_scripts("snowsql", "sql/action.sql", database_name, warehouse)

    # Compare results from check.sql
    print("Starting comparison of results from check.sql...")
    with open("sql/check.sql", "r") as file:
        check_queries = file.read().split(";")

    for query in check_queries:
        if query.strip():
            print(f"Comparing results for query: {query}")
            bend_result = fetch_query_results(query, "bendsql", database_name)
            snow_result = fetch_query_results(
                query, "snowsql", database_name, warehouse
            )

            if bend_result != snow_result:
                print(colored("DIFFERENCE FOUND", "red"))
                print("Query:")
                print(query)
                print("\nDifferences:")
                diff = difflib.unified_diff(
                    bend_result.splitlines(),
                    snow_result.splitlines(),
                    fromfile="bendsql",
                    tofile="snowsql",
                    lineterm="",
                )
                for line in diff:
                    print(line)
                break
            else:
                print(colored("OK", "green"))


if __name__ == "__main__":
    main()
