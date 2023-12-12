import argparse
import re
import subprocess
from termcolor import colored


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Compare SQL execution on different databases."
    )
    parser.add_argument("--database", help="Database name", required=True)
    parser.add_argument(
        "--warehouse",
        default="COMPUTE_WH",
        help="Warehouse name for snowsql",
        required=False,
    )
    parser.add_argument(
        "--run-check-only", action="store_true", help="Run only check.sql if set"
    )
    parser.add_argument(
        "--case",
        help="Case to execute (e.g., mergeinto, selects, streams, updates)",
        required=True,
    )
    # New arguments for executing only bendsql or snowsql
    parser.add_argument(
        "--runbend", action="store_true", help="Run only bendsql setup and action"
    )
    parser.add_argument(
        "--runsnow", action="store_true", help="Run only snowsql setup and action"
    )
    args = parser.parse_args()
    return args


def execute_sql(query, sql_tool, database, warehouse=None):
    command = [sql_tool]
    if sql_tool == "snowsql":
        command.extend(
            [
                "--query",
                query,
                "--dbname",
                database,
                "--schemaname",
                "PUBLIC",
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
            execute_sql(query, sql_tool, database, warehouse)


def fetch_query_results(query, sql_tool, database, warehouse=None):
    result = execute_sql(query, sql_tool, database, warehouse)
    return result


def run_check_sql(database_name, warehouse, script_path):
    with open(script_path, "r") as file:
        check_queries = file.read().split(";")

    for query in check_queries:
        if query.strip():
            # Extract the query identifier (like MERGE-INTO-C13) from the comment
            match = re.search(r"--\s*([\w-]+):", query)
            query_identifier = match.group(1).strip() if match else "Unknown Query"

            # Print the preparing message in yellow
            print(colored(f"Preparing to run {query_identifier}...", "yellow"))

            bend_result = fetch_query_results(query, "bendsql", database_name)
            snow_result = fetch_query_results(
                query, "snowsql", database_name, warehouse
            )

            if bend_result != snow_result:
                print(colored("DIFFERENCE FOUND\n", "red"))
                print(colored(f"{query_identifier}:\n" + query, "red"))
                print("Differences:\n")
                print(colored("bendsql:\n" + bend_result, "red"))
                print(colored("snowsql:\n" + snow_result, "red"))
            else:
                print(colored(f"OK - {query_identifier}", "green"))
                print(colored(bend_result, "green"))


def setup_database(database_name, sql_tool):
    drop_query = f"DROP DATABASE IF EXISTS {database_name};"
    create_query = f"CREATE DATABASE {database_name};"
    execute_sql(drop_query, sql_tool, database_name)
    execute_sql(create_query, sql_tool, database_name)
    print(f"Database '{database_name}' has been set up.")


def setup_and_execute(sql_tool, base_sql_dir, database_name, warehouse=None):
    # Determine the correct setup directory based on the SQL tool
    setup_dir = "bend" if sql_tool == "bendsql" else "snow"

    setup_database(database_name, sql_tool)

    execute_sql_scripts(
        sql_tool, f"{base_sql_dir}/{setup_dir}/setup.sql", database_name, warehouse
    )
    execute_sql_scripts(
        sql_tool, f"{base_sql_dir}/action.sql", database_name, warehouse
    )


def main():
    args = parse_arguments()

    base_sql_dir = f"sql/{args.case}"
    database_name, warehouse = args.database, args.warehouse

    if args.run_check_only:
        # Run only the check script
        check_sql_path = f"{base_sql_dir}/check.sql"
        run_check_sql(database_name, warehouse, check_sql_path)
    else:
        # Setup database based on the specified arguments
        if args.runbend:
            print("Setting up and executing scripts for bendsql...")
            setup_and_execute("bendsql", base_sql_dir, database_name)
        elif args.runsnow:
            print("Setting up and executing scripts for snowsql...")
            setup_and_execute("snowsql", base_sql_dir, database_name, warehouse)
        else:
            print("Setting up and executing scripts for both bendsql and snowsql...")
            setup_and_execute("bendsql", base_sql_dir, database_name)
            setup_and_execute("snowsql", base_sql_dir, database_name, warehouse)

        # Compare results from check.sql after executing scripts
        check_sql_path = f"{base_sql_dir}/check.sql"
        run_check_sql(database_name, warehouse, check_sql_path)


if __name__ == "__main__":
    main()
