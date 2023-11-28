import argparse
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
        "--case", help="Case to execute (e.g., mergeinto, selects)", required=True
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
            print(f"Comparing results for query: {query}")
            bend_result = fetch_query_results(query, "bendsql", database_name)
            snow_result = fetch_query_results(
                query, "snowsql", database_name, warehouse
            )

            if bend_result != snow_result:
                print(colored("DIFFERENCE FOUND\n", "red"))
                print(colored("query:\n" + query, "red"))
                print("Differences:\n")
                print(colored("bendsql:\n" + bend_result, "red"))
                print(colored("snowsql:\n" + snow_result, "red"))
            else:
                print(colored("OK", "green"))
                print(colored(bend_result, "green"))


def main():
    args = parse_arguments()

    base_sql_dir = f"sql/{args.case}"

    if args.run_check_only:
        check_sql_path = f"{base_sql_dir}/check.sql"
        run_check_sql(args.database, args.warehouse, check_sql_path)
    else:
        database_name, warehouse = args.database, args.warehouse

        # Execute setup and action scripts based on the specified arguments
        if args.runbend:
            print("Executing setup and action scripts for bendsql...")
            execute_sql_scripts(
                "bendsql", f"{base_sql_dir}/bend/setup.sql", database_name
            )
            execute_sql_scripts("bendsql", f"{base_sql_dir}/action.sql", database_name)
        elif args.runsnow:
            print("Executing setup and action scripts for snowsql...")
            execute_sql_scripts(
                "snowsql", f"{base_sql_dir}/snow/setup.sql", database_name, warehouse
            )
            execute_sql_scripts(
                "snowsql", f"{base_sql_dir}/action.sql", database_name, warehouse
            )
        else:
            print("Executing setup and action scripts for both bendsql and snowsql...")
            execute_sql_scripts(
                "bendsql", f"{base_sql_dir}/bend/setup.sql", database_name
            )
            execute_sql_scripts("bendsql", f"{base_sql_dir}/action.sql", database_name)
            execute_sql_scripts(
                "snowsql", f"{base_sql_dir}/snow/setup.sql", database_name, warehouse
            )
            execute_sql_scripts(
                "snowsql", f"{base_sql_dir}/action.sql", database_name, warehouse
            )

        # Compare results from check.sql
        check_sql_path = f"{base_sql_dir}/check.sql"
        run_check_sql(database_name, warehouse, check_sql_path)


if __name__ == "__main__":
    main()
