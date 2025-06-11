import argparse
import re
import sys
import subprocess
import time
from termcolor import colored
import logging
from datetime import datetime
import difflib

# Global logger instance
logger = logging.getLogger(__name__)


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
        "--setup", action="store_true", help="Run setup scripts. By default, setup is skipped."
    )
    parser.add_argument(
        "--case",
        help="Case to execute (e.g., selects, mergeinto, streams, updates, deletes, all). Multiple cases can be specified with comma separation (e.g., selects,streams).",
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

    logger.info(f"Executing command: {' '.join(command)}")

    try:
        result = subprocess.run(command, text=True, capture_output=True, check=False)
        output = result.stdout
        error = result.stderr

        # For database setup operations, we want to continue even if there are errors
        if "DROP DATABASE" in query and "Unknown database" in (output + error):
            logger.info("Database doesn't exist, continuing with creation...")
            return output

        # Custom check for known error patterns
        if ("error" in output.lower() or "error" in error.lower() or 
            "unknown function" in output.lower()):
            error_message = f"Error detected in command output: {output or error}"
            logger.error(error_message) # Print the error in red
            if "DROP DATABASE" in query:
                # Don't exit for database drop errors
                return output
            sys.exit(1)

        logger.info("Command executed successfully.")
        return output
    except subprocess.CalledProcessError as e:
        error_message = f"{sql_tool} command failed: {e.stderr}"
        logger.error(error_message)  # Print the error in red
        # Re-raise the exception to be handled by the caller
        raise


def execute_sql_scripts(sql_tool, script_path, database, warehouse=None):
    logger.info(f"Executing SQL scripts from: {script_path} using {sql_tool}")
    with open(script_path, "r") as file:
        sql_script = file.read()
    queries = sql_script.split(";")
    for query in queries:
        if query.strip():
            execute_sql(query, sql_tool, database, warehouse)


def fetch_query_results(query, sql_tool, database, warehouse=None):
    result = execute_sql(query, sql_tool, database, warehouse)
    # Normalize "None" to "NULL" to treat them as semantically equivalent for comparison
    if result is not None:
        result = result.replace("None", "NULL")
    return result


# Helper function to normalize a single line of query output
def normalize_line(line_string):
    parts = re.split(r'\s+', line_string.strip()) # Split by any whitespace, strip leading/trailing
    normalized_parts = []
    for part in parts:
        if not part: # Handle potential empty strings from multiple spaces
            continue
        try:
            num = float(part)
            if num == int(num): # Check if it's an integer to avoid ".0"
                normalized_parts.append(str(int(num)))
            else:
                normalized_parts.append(str(num)) # Normalize float representation
        except ValueError:
            normalized_parts.append(part) # Not a number, keep original
    return "\t".join(normalized_parts) # Rejoin with tabs for consistent spacing


def run_check_sql(database_name, warehouse, script_path):
    failed_tests = []
    passed_tests = []
    total_start_time = time.time()
    total_queries = 0
    current_query = 0
    case_success = True  # Track if this case was successful overall

    with open(script_path, "r") as file:
        check_queries = file.read().split(";")
        # Count non-empty queries
        total_queries = sum(1 for q in check_queries if q.strip())

    logger.info(f"\n{'='*80}")
    logger.info(f"Starting comparison of {total_queries} queries between bendsql and snowsql")
    logger.info(f"{'='*80}\n")

    for query in check_queries:
        if query.strip():
            current_query += 1
            # Extract the query identifier (like MERGE-INTO-C13) from the comment
            match = re.search(r"--\s*([\w-]+):", query)
            query_identifier = match.group(1).strip() if match else f"Query-{current_query}"

            # Print the preparing message with progress indicator
            logger.info(f"\n[{current_query}/{total_queries}] Testing {query_identifier}...")
            logger.info(f"Query: {query.strip()[:60]}{'...' if len(query.strip()) > 60 else ''}")

            start_time = time.time()
            bend_result_str = fetch_query_results(query, "bendsql", database_name)
            snow_result_str = fetch_query_results(
                query, "snowsql", database_name, warehouse
            )
            end_time = time.time()
            elapsed_time = end_time - start_time

            # Stage 1: Exact string comparison
            if bend_result_str == snow_result_str:
                passed_tests.append((query_identifier, elapsed_time))
                progress_summary = f" [Progress: passed {len(passed_tests)}, failed {len(failed_tests)}, total {current_query}/{total_queries}]"
                logger.info(f"✅ MATCH (exact) - Results are identical ({elapsed_time:.2f}s){progress_summary}")
            else:
                # Stage 2: Normalized, Order-Sensitive Comparison
                bend_lines_raw = bend_result_str.splitlines()
                snow_lines_raw = snow_result_str.splitlines()

                bend_lines_normalized_ordered = [normalize_line(line) for line in bend_lines_raw]
                snow_lines_normalized_ordered = [normalize_line(line) for line in snow_lines_raw]

                if bend_lines_normalized_ordered == snow_lines_normalized_ordered:
                    passed_tests.append((query_identifier, elapsed_time))
                    progress_summary = f" [Progress: passed {len(passed_tests)}, failed {len(failed_tests)}, total {current_query}/{total_queries}]"
                    logger.info(f"✅ MATCH (numeric format, order-sensitive) - Results identical after numeric normalization ({elapsed_time:.2f}s){progress_summary}")
                    logger.info("  (Note: Original results differed only in numeric formatting but content and order were otherwise the same)")
                else:
                    # Stage 3: Normalized, Order-Agnostic Comparison
                    bend_lines_normalized_sorted = sorted(bend_lines_normalized_ordered)
                    snow_lines_normalized_sorted = sorted(snow_lines_normalized_ordered)

                    if bend_lines_normalized_sorted == snow_lines_normalized_sorted:
                        passed_tests.append((query_identifier, elapsed_time))
                        progress_summary = f" [Progress: passed {len(passed_tests)}, failed {len(failed_tests)}, total {current_query}/{total_queries}]"
                        logger.info(f"✅ MATCH (numeric format, order-agnostic) - Results identical after numeric normalization and ignoring row order ({elapsed_time:.2f}s){progress_summary}")
                        logger.info("  (Note: Original results differed in row order and/or numeric formatting, but content is the same after normalization)")
                    else:
                        # Stage 4: Reporting Differences
                        failed_tests.append((query_identifier, bend_result_str, snow_result_str))
                        progress_summary = f" [Progress: passed {len(passed_tests)}, failed {len(failed_tests)}, total {current_query}/{total_queries}]"
                        logger.error(f"❌ DIFFERENCE FOUND (content mismatch after all normalizations){progress_summary}")
                        logger.error("Differences (line-by-line, based on normalized & sorted results):")
                        
                        len_snow = len(snow_lines_normalized_sorted)
                        len_bend = len(bend_lines_normalized_sorted)
                        max_len = max(len_snow, len_bend)

                        for i in range(max_len):
                            row_num = i + 1
                            snow_line_display = snow_lines_normalized_sorted[i] if i < len_snow else "--- (missing in snowsql output)"
                            bend_line_display = bend_lines_normalized_sorted[i] if i < len_bend else "--- (missing in bendsql output)"

                            # Only print if lines actually differ or one is missing
                            if (i >= len_snow or i >= len_bend or 
                                snow_lines_normalized_sorted[i] != bend_lines_normalized_sorted[i]):
                                logger.error(f"  row-{row_num}:")
                                logger.error(f"    snowsql: {snow_line_display}")
                                logger.error(f"    bendsql: {bend_line_display}")
                        
                        if len_snow != len_bend:
                            logger.error(f"  (Note: Result sets have different number of rows after normalization: snowsql {len_snow}, bendsql {len_bend})")
                        logger.error("bendsql result (original):")
                        logger.error(bend_result_str)
                        logger.error("snowsql result (original):")
                        logger.error(snow_result_str)
            

    total_end_time = time.time()
    total_elapsed_time = total_end_time - total_start_time

    # Print final summary with clear separation
    logger.info(f"\n{'='*80}")
    logger.info("FINAL SUMMARY")
    logger.info(f"{'='*80}")
    
    logger.info(f"\nTotal Queries: {total_queries}")
    logger.info(f"Passed: {len(passed_tests)} ({len(passed_tests)/total_queries*100:.1f}%)")
    logger.info(f"Failed: {len(failed_tests)} ({len(failed_tests)/total_queries*100:.1f}%)")
    logger.info(f"Total Time: {total_elapsed_time:.2f}s")

    if passed_tests:
        logger.info("\nPassed Tests:")
        for i, (test, elapsed_time) in enumerate(passed_tests, 1):
            logger.info(f"  {i}. {test} ({elapsed_time:.2f}s)")

    if failed_tests:
        logger.error("\nFailed Tests:")
        for i, (test, _, _) in enumerate(failed_tests, 1):
            logger.error(f"  {i}. {test}")
        
        logger.error("\nDetailed Differences:")
        for i, (test, bend_result, snow_result) in enumerate(failed_tests, 1):
            logger.error(f"\n{i}. Test: {test}")
            logger.error("   bendsql result:")
            logger.error(f"   {bend_result.replace(chr(10), chr(10)+'   ')}")
            logger.error("   snowsql result:")
            logger.error(f"   {snow_result.replace(chr(10), chr(10)+'   ')}")
    
    # Final result indicator
    if failed_tests:
        logger.error(f"\n❌ COMPARISON FAILED: {len(failed_tests)} differences found")
        case_success = False
    else:
        logger.info(f"\n✅ COMPARISON SUCCESSFUL: All {total_queries} queries match!")
        
    # Return the results instead of exiting
    return {
        "success": case_success,
        "total_queries": total_queries,
        "passed_tests": passed_tests,
        "failed_tests": failed_tests,
        "elapsed_time": total_elapsed_time
    }


def setup_database(database_name, sql_tool):
    logger.info(f"\nSetting up database '{database_name}' using {sql_tool}...")
    
    # For bendsql, we need to handle the case where the database doesn't exist yet
    if sql_tool == "bendsql":
        # Try to drop the database, but ignore errors if it doesn't exist
        try:
            drop_query = f"DROP DATABASE IF EXISTS {database_name};"
            execute_sql(drop_query, sql_tool, "default")  # Use default database for initial connection
        except Exception as e:
            logger.warning(f"Warning: Could not drop database (it may not exist): {e}")
        
        # Create the database
        create_query = f"CREATE DATABASE {database_name};"
        execute_sql(create_query, sql_tool, "default")  # Use default database for initial connection
    else:
        # For snowsql, the IF EXISTS clause works as expected
        drop_query = f"DROP DATABASE IF EXISTS {database_name};"
        create_query = f"CREATE DATABASE {database_name};"
        execute_sql(drop_query, sql_tool, database_name)
        execute_sql(create_query, sql_tool, database_name)
    
    logger.info(f"✅ Database '{database_name}' has been set up successfully.")


def setup_and_execute(sql_tool, base_sql_dir, database_name, warehouse=None):
    # Determine the correct setup directory based on the SQL tool
    setup_dir = "bend" if sql_tool == "bendsql" else "snow"

    logger.info(f"\n{'='*80}")
    logger.info(f"Setting up and executing {sql_tool} scripts")
    logger.info(f"{'='*80}")
    
    setup_database(database_name, sql_tool)

    logger.info(f"\nExecuting setup scripts for {sql_tool}...")
    execute_sql_scripts(
        sql_tool, f"{base_sql_dir}/{setup_dir}/setup.sql", database_name, warehouse
    )
    
    logger.info(f"\nExecuting action scripts for {sql_tool}...")
    execute_sql_scripts(
        sql_tool, f"{base_sql_dir}/action.sql", database_name, warehouse
    )
    
    logger.info(f"✅ All {sql_tool} scripts executed successfully.")


def main():
    # Setup logging
    log_filename = f"checksb_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout) # Keep console output
        ]
    )

    args = parse_arguments()

    # Process the case argument to handle multiple cases or 'all'
    cases = []
    if args.case.lower() == 'all':
        # Get all available cases from the sql directory
        import os
        cases = [d for d in os.listdir('sql') if os.path.isdir(os.path.join('sql', d))]
    else:
        # Handle comma-separated cases
        cases = [case.strip() for case in args.case.split(',')]
    
    # Print a nice header for the entire run
    logger.info(f"\n{'='*80}")
    logger.info(f"SQL Compatibility Test: Running {len(cases)} case(s): {', '.join(cases).upper()}")
    logger.info(f"Database: {args.database}")
    logger.info(f"{'='*80}\n")

    # Track overall results
    overall_results = {
        "total_cases": len(cases),
        "passed_cases": 0,
        "failed_cases": [],
        "case_results": [],
        "start_time": time.time(),
        "total_queries": 0,
        "passed_queries": 0,
        "failed_queries": 0
    }

    # Run each case
    for case in cases:
        case_result = run_single_case(case, args)
        overall_results["case_results"].append(case_result)
        
        # Update overall statistics
        overall_results["total_queries"] += case_result["total_queries"]
        overall_results["passed_queries"] += case_result["passed_queries"]
        overall_results["failed_queries"] += case_result["failed_queries"]
        
        if case_result["success"]:
            overall_results["passed_cases"] += 1
        else:
            overall_results["failed_cases"].append(case_result["case"])
    
    # Print overall summary
    overall_end_time = time.time()
    overall_elapsed_time = overall_end_time - overall_results["start_time"]
    
    logger.info(f"\n{'='*80}")
    logger.info(f"OVERALL SUMMARY FOR {len(cases)} CASE(S)")
    logger.info(f"{'='*80}")
    
    logger.info(f"\nTotal Cases: {overall_results['total_cases']}")
    logger.info(f"Passed Cases: {overall_results['passed_cases']} ({overall_results['passed_cases']/overall_results['total_cases']*100:.1f}%)")
    
    if overall_results["failed_cases"]:
        logger.error(f"Failed Cases: {len(overall_results['failed_cases'])} ({len(overall_results['failed_cases'])/overall_results['total_cases']*100:.1f}%)")
        logger.error("\nFailed Cases List:")
        for i, failed_case in enumerate(overall_results["failed_cases"], 1):
            logger.error(f"  {i}. {failed_case}")
    
    logger.info(f"\nTotal Queries: {overall_results['total_queries']}")
    logger.info(f"Passed Queries: {overall_results['passed_queries']}")
    
    if overall_results["failed_queries"] > 0:
        logger.error(f"Failed Queries: {overall_results['failed_queries']}")
        
        # Show detailed failures for each case
        logger.error("\nDetailed Failures by Case:")
        for case_result in overall_results["case_results"]:
            if not case_result["success"] and case_result.get("failed_tests"):
                logger.error(f"\n  Case: {case_result['case'].upper()}")
                
                # Print detailed differences for each failed test
                logger.error("\n  Detailed Differences:")
                for i, (test, bend_result, snow_result) in enumerate(case_result["failed_tests"], 1):
                    logger.error(f"\n    {i}. Test: {test}")
                    logger.error("       bendsql result:")
                    logger.error(f"       {bend_result.replace(chr(10), chr(10)+'       ')}")
                    logger.error("       snowsql result:")
                    logger.error(f"       {snow_result.replace(chr(10), chr(10)+'       ')}")

    
    logger.info(f"\nTotal Time: {overall_elapsed_time:.2f}s")


def run_single_case(case, args):
    base_sql_dir = f"sql/{case}"
    database_name, warehouse = args.database, args.warehouse

    # Print a nice header for this case
    logger.info(f"\n{'='*80}")
    logger.info(f"SQL Compatibility Test: {case.upper()}")
    logger.info(f"Database: {database_name}")
    logger.info(f"{'='*80}\n")

    # Initialize result
    result = {
        "case": case,
        "success": False,
        "total_queries": 0,
        "passed_queries": 0,
        "failed_queries": 0,
        "failed_tests": [],
        "elapsed_time": 0
    }

    try:
        if args.setup:
            # Setup database based on the specified arguments
            if args.runbend:
                logger.info("Running bendsql setup and action only")
                setup_and_execute("bendsql", base_sql_dir, database_name)
            elif args.runsnow:
                logger.info("Running snowsql setup and action only")
                setup_and_execute("snowsql", base_sql_dir, database_name, warehouse)
            else:
                logger.info("Running complete test (bendsql and snowsql)")
                setup_and_execute("bendsql", base_sql_dir, database_name)
                setup_and_execute("snowsql", base_sql_dir, database_name, warehouse)
        else:
            logger.info("Skipping setup and action scripts. Use --setup to run them.")

        # Always run the check
        logger.info("\nRunning check queries...")
        check_sql_path = f"{base_sql_dir}/check.sql"
        check_result = run_check_sql(database_name, warehouse, check_sql_path)
        
        # Update result with check results
        result["success"] = check_result["success"]
        result["total_queries"] = check_result["total_queries"]
        result["passed_queries"] = len(check_result["passed_tests"])
        result["failed_queries"] = len(check_result["failed_tests"])
        result["failed_tests"] = check_result["failed_tests"]
        result["elapsed_time"] = check_result["elapsed_time"]
    
    except Exception as e:
        logger.error(f"Error running case {case}: {str(e)}")
        result["success"] = False
        result["error"] = str(e)
    
    return result


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.error("Test interrupted by user. Exiting...")
        sys.exit(1)
    except Exception as e:
        # Ensure logger is available or use print for this fallback
        if logger.handlers:
            logger.critical(f"Unhandled exception: {e}", exc_info=True)
        else:
            # Fallback if logger isn't initialized (e.g., error during arg parsing before logging setup)
            print(f"CRITICAL: Unhandled exception before logger initialization: {e}")
        sys.exit(1)
