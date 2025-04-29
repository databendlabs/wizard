import argparse
import re
import sys
import subprocess
import time
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
        help="Case to execute (e.g., selects, mergeinto, streams, updates, deletes)",
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
        command.extend([
            "--query", query,
            "--dbname", database,
            "--schemaname", "PUBLIC",
            "-o", "output_format=tsv",
            "-o", "header=false",
            "-o", "timing=false",
            "-o", "friendly=false",
        ])
        if warehouse:
            command.extend(["--warehouse", warehouse])
    elif sql_tool == "bendsql":
        command.extend(["--query=" + query, "-D", database])

    print(f"Executing command: {' '.join(command)}")

    try:
        result = subprocess.run(command, text=True, capture_output=True, check=False)
        output = result.stdout
        error = result.stderr

        # For database setup operations, we want to continue even if there are errors
        if "DROP DATABASE" in query and "Unknown database" in (output + error):
            print("Database doesn't exist, continuing with creation...")
            return output

        # Custom check for known error patterns
        if ("error" in output.lower() or "error" in error.lower() or 
            "unknown function" in output.lower()):
            error_message = f"Error detected in command output: {output or error}"
            print(colored(error_message, "red"))  # Print the error in red
            if "DROP DATABASE" in query:
                # Don't exit for database drop errors
                return output
            sys.exit(1)

        print("Command executed successfully. Output:")
        print(output)
        return output
    except subprocess.CalledProcessError as e:
        error_message = f"{sql_tool} command failed: {e.stderr}"
        print(colored(error_message, "red"))  # Print the error in red
        # Re-raise the exception to be handled by the caller
        raise


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
    failed_tests = []
    passed_tests = []
    total_start_time = time.time()
    total_queries = 0
    current_query = 0

    with open(script_path, "r") as file:
        check_queries = file.read().split(";")
        # Count non-empty queries
        total_queries = sum(1 for q in check_queries if q.strip())

    print(colored(f"\n{'='*80}", "blue"))
    print(colored(f"Starting comparison of {total_queries} queries between bendsql and snowsql", "blue"))
    print(colored(f"{'='*80}\n", "blue"))

    for query in check_queries:
        if query.strip():
            current_query += 1
            # Extract the query identifier (like MERGE-INTO-C13) from the comment
            match = re.search(r"--\s*([\w-]+):", query)
            query_identifier = match.group(1).strip() if match else f"Query-{current_query}"

            # Print the preparing message with progress indicator
            print(colored(f"\n[{current_query}/{total_queries}] Testing {query_identifier}...", "yellow"))
            print(colored(f"Query: {query.strip()[:100]}{'...' if len(query.strip()) > 100 else ''}", "yellow"))

            start_time = time.time()
            bend_result = fetch_query_results(query, "bendsql", database_name)
            snow_result = fetch_query_results(
                query, "snowsql", database_name, warehouse
            )
            end_time = time.time()
            elapsed_time = end_time - start_time

            if bend_result != snow_result:
                print(colored("❌ DIFFERENCE FOUND", "red"))
                print(colored("Differences:", "red"))
                print(colored("bendsql result:", "red"))
                print(colored(bend_result, "red"))
                print(colored("snowsql result:", "red"))
                print(colored(snow_result, "red"))
                failed_tests.append((query_identifier, bend_result, snow_result))
            else:
                print(colored(f"✅ MATCH - Results are identical ({elapsed_time:.2f}s)", "green"))
                passed_tests.append((query_identifier, elapsed_time))
            
            # Print current progress summary
            print(colored(f"\nCurrent Progress: [passed: {len(passed_tests)}, failed: {len(failed_tests)}, total: {current_query}/{total_queries}]", "blue"))

    total_end_time = time.time()
    total_elapsed_time = total_end_time - total_start_time

    # Print final summary with clear separation
    print(colored(f"\n{'='*80}", "blue"))
    print(colored("FINAL SUMMARY", "blue"))
    print(colored(f"{'='*80}", "blue"))
    
    print(colored(f"\nTotal Queries: {total_queries}", "white"))
    print(colored(f"Passed: {len(passed_tests)} ({len(passed_tests)/total_queries*100:.1f}%)", "green"))
    print(colored(f"Failed: {len(failed_tests)} ({len(failed_tests)/total_queries*100:.1f}%)", "red" if failed_tests else "green"))
    print(colored(f"Total Time: {total_elapsed_time:.2f}s", "blue"))

    if passed_tests:
        print(colored("\nPassed Tests:", "green"))
        for i, (test, elapsed_time) in enumerate(passed_tests, 1):
            print(colored(f"  {i}. {test} ({elapsed_time:.2f}s)", "green"))

    if failed_tests:
        print(colored("\nFailed Tests:", "red"))
        for i, (test, _, _) in enumerate(failed_tests, 1):
            print(colored(f"  {i}. {test}", "red"))
        
        print(colored("\nDetailed Differences:", "red"))
        for i, (test, bend_result, snow_result) in enumerate(failed_tests, 1):
            print(colored(f"\n{i}. Test: {test}", "red"))
            print(colored("   bendsql result:", "yellow"))
            print(f"   {bend_result.replace(chr(10), chr(10)+'   ')}")
            print(colored("   snowsql result:", "yellow"))
            print(f"   {snow_result.replace(chr(10), chr(10)+'   ')}")
    
    # Final result indicator
    if failed_tests:
        print(colored(f"\n❌ COMPARISON FAILED: {len(failed_tests)} differences found", "red"))
    else:
        print(colored(f"\n✅ COMPARISON SUCCESSFUL: All {total_queries} queries match!", "green"))


def setup_database(database_name, sql_tool):
    print(colored(f"\nSetting up database '{database_name}' using {sql_tool}...", "blue"))
    
    # For bendsql, we need to handle the case where the database doesn't exist yet
    if sql_tool == "bendsql":
        # Try to drop the database, but ignore errors if it doesn't exist
        try:
            drop_query = f"DROP DATABASE IF EXISTS {database_name};"
            execute_sql(drop_query, sql_tool, "default")  # Use default database for initial connection
        except Exception as e:
            print(f"Warning: Could not drop database (it may not exist): {e}")
        
        # Create the database
        create_query = f"CREATE DATABASE {database_name};"
        execute_sql(create_query, sql_tool, "default")  # Use default database for initial connection
    else:
        # For snowsql, the IF EXISTS clause works as expected
        drop_query = f"DROP DATABASE IF EXISTS {database_name};"
        create_query = f"CREATE DATABASE {database_name};"
        execute_sql(drop_query, sql_tool, database_name)
        execute_sql(create_query, sql_tool, database_name)
    
    print(colored(f"✅ Database '{database_name}' has been set up successfully.", "green"))


def setup_and_execute(sql_tool, base_sql_dir, database_name, warehouse=None):
    # Determine the correct setup directory based on the SQL tool
    setup_dir = "bend" if sql_tool == "bendsql" else "snow"

    print(colored(f"\n{'='*80}", "blue"))
    print(colored(f"Setting up and executing {sql_tool} scripts", "blue"))
    print(colored(f"{'='*80}", "blue"))
    
    setup_database(database_name, sql_tool)

    print(colored(f"\nExecuting setup scripts for {sql_tool}...", "blue"))
    execute_sql_scripts(
        sql_tool, f"{base_sql_dir}/{setup_dir}/setup.sql", database_name, warehouse
    )
    
    print(colored(f"\nExecuting action scripts for {sql_tool}...", "blue"))
    execute_sql_scripts(
        sql_tool, f"{base_sql_dir}/action.sql", database_name, warehouse
    )
    
    print(colored(f"✅ All {sql_tool} scripts executed successfully.", "green"))


def main():
    args = parse_arguments()

    base_sql_dir = f"sql/{args.case}"
    database_name, warehouse = args.database, args.warehouse

    # Print a nice header
    print(colored(f"\n{'='*80}", "blue"))
    print(colored(f"SQL Compatibility Test: {args.case.upper()}", "blue"))
    print(colored(f"Database: {database_name}", "blue"))
    print(colored(f"{'='*80}\n", "blue"))

    if args.run_check_only:
        # Run only the check script
        print(colored("Running check queries only (skipping setup and action scripts)", "yellow"))
        check_sql_path = f"{base_sql_dir}/check.sql"
        run_check_sql(database_name, warehouse, check_sql_path)
    else:
        # Setup database based on the specified arguments
        if args.runbend:
            print(colored("Running bendsql setup and action only", "yellow"))
            setup_and_execute("bendsql", base_sql_dir, database_name)
        elif args.runsnow:
            print(colored("Running snowsql setup and action only", "yellow"))
            setup_and_execute("snowsql", base_sql_dir, database_name, warehouse)
        else:
            print(colored("Running complete test (bendsql and snowsql)", "yellow"))
            setup_and_execute("bendsql", base_sql_dir, database_name)
            setup_and_execute("snowsql", base_sql_dir, database_name, warehouse)

        # Compare results from check.sql after executing scripts
        check_sql_path = f"{base_sql_dir}/check.sql"
        run_check_sql(database_name, warehouse, check_sql_path)


if __name__ == "__main__":
    main()
