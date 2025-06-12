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
    parser.add_argument("--database", help="Database name", default="checksb_db", required=False)
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
    parser.add_argument(
        "--skip",
        type=str,
        help="Benchmarks to skip (e.g., tpcds,selects). Multiple benchmarks can be specified with comma separation.",
        default="",
        required=False,
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
            # Combine output and error streams to capture the full error message
            error_content = (output + "\n" + error).strip()
            # Return a uniquely prefixed string to signal an error to the caller
            return f"__ERROR__:{error_content}"

        logger.info("Command executed successfully.")
        return output
    except subprocess.CalledProcessError as e:
        # This block is unlikely to be hit with check=False, but as a safeguard:
        logger.error(f"{sql_tool} command failed unexpectedly: {e}")
        return f"__ERROR__:{e}"


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
    # Check for the error flag and pass it through without modification
    if result is not None and result.startswith("__ERROR__:"):
        return result
    
    # For successful results, normalize "None" to "NULL"
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
            # Round to a defined number of decimal places, e.g., 3
            num_rounded = round(num, 3)
            # If after rounding, it's an integer, display as int
            if num_rounded == int(num_rounded):
                normalized_parts.append(str(int(num_rounded)))
            else:
                # Format to ensure consistent number of decimal places if needed, or just str()
                # Using str() will show up to 6 decimal places correctly due to rounding.
                normalized_parts.append(str(num_rounded))
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

            start_time = time.time()
            bend_result_str = fetch_query_results(query, "bendsql", database_name)
            snow_result_str = fetch_query_results(query, "snowsql", database_name, warehouse)

            # current_query is now incremented only once at the start of the loop iteration

            bend_is_error = bend_result_str is not None and bend_result_str.startswith("__ERROR__:")
            snow_is_error = snow_result_str is not None and snow_result_str.startswith("__ERROR__:")

            # Handle cases where SQL execution failed
            if bend_is_error or snow_is_error:
                bend_info = bend_result_str[len("__ERROR__:"):] if bend_is_error else "Execution OK"
                snow_info = snow_result_str[len("__ERROR__:"):] if snow_is_error else "Execution OK"
                
                failed_tests.append((query_identifier, "Execution Failed", bend_info, snow_info))
                progress_summary = f" [Progress: passed {len(passed_tests)}, failed {len(failed_tests)}, total {current_query}/{total_queries}]"
                logger.error(f"❌ FAILED (execution error) - Query: {query_identifier}{progress_summary}")
                
                # Log the truncated error(s)
                MAX_ERROR_LENGTH = 250
                if bend_is_error:
                    truncated_error = (bend_info[:MAX_ERROR_LENGTH] + '...') if len(bend_info) > MAX_ERROR_LENGTH else bend_info
                    logger.error(f"  bendsql error: {truncated_error.replace(chr(10), ' ')}")
                if snow_is_error:
                    truncated_error = (snow_info[:MAX_ERROR_LENGTH] + '...') if len(snow_info) > MAX_ERROR_LENGTH else snow_info
                    logger.error(f"  snowsql error: {truncated_error.replace(chr(10), ' ')}")
                
                continue

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
                    bend_lines_normalized_fully_sorted = sorted(bend_lines_normalized_ordered)
                    snow_lines_normalized_fully_sorted = sorted(snow_lines_normalized_ordered)

                    MAX_LINES_FOR_DETAILED_COMPARISON = 100
                    is_truncated_comparison = False

                    # Determine actual lists to compare (potentially truncated)
                    final_bend_lines_to_compare = bend_lines_normalized_fully_sorted
                    final_snow_lines_to_compare = snow_lines_normalized_fully_sorted

                    if len(bend_lines_normalized_fully_sorted) > MAX_LINES_FOR_DETAILED_COMPARISON or \
                       len(snow_lines_normalized_fully_sorted) > MAX_LINES_FOR_DETAILED_COMPARISON:
                        logger.warning(
                            f"Result sets are large. Order-agnostic comparison and diff will be limited to the top "
                            f"{MAX_LINES_FOR_DETAILED_COMPARISON} sorted & normalized lines."
                        )
                        final_bend_lines_to_compare = bend_lines_normalized_fully_sorted[:MAX_LINES_FOR_DETAILED_COMPARISON]
                        final_snow_lines_to_compare = snow_lines_normalized_fully_sorted[:MAX_LINES_FOR_DETAILED_COMPARISON]
                        is_truncated_comparison = True

                    if final_bend_lines_to_compare == final_snow_lines_to_compare:
                        passed_tests.append((query_identifier, elapsed_time))
                        progress_summary = f" [Progress: passed {len(passed_tests)}, failed {len(failed_tests)}, total {current_query}/{total_queries}]"
                        truncation_note = f" (comparison limited to top {MAX_LINES_FOR_DETAILED_COMPARISON} lines)" if is_truncated_comparison else ""
                        logger.info(f"✅ MATCH (numeric format, order-agnostic) - Results identical after numeric normalization and ignoring row order ({elapsed_time:.2f}s){progress_summary}{truncation_note}")
                        logger.info("  (Note: Original results differed in row order and/or numeric formatting, but content is the same after normalization)")
                    else:
                        # Stage 4: Reporting Differences
                        diff_details_for_summary = []
                        truncation_note_for_diff = f" (comparison limited to top {MAX_LINES_FOR_DETAILED_COMPARISON} lines)" if is_truncated_comparison else ""
                        diff_log_header = f"Differences (line-by-line, based on normalized & sorted results{truncation_note_for_diff}):"
                        logger.error(diff_log_header)
                        diff_details_for_summary.append(diff_log_header)
                        
                        # Use the (potentially truncated) lists for diff generation
                        len_snow = len(final_snow_lines_to_compare)
                        len_bend = len(final_bend_lines_to_compare)
                        max_len = max(len_snow, len_bend)

                        for i in range(max_len):
                            row_num = i + 1
                            snow_line_display = final_snow_lines_to_compare[i] if i < len_snow else "--- (missing in snowsql output)"
                            bend_line_display = final_bend_lines_to_compare[i] if i < len_bend else "--- (missing in bendsql output)"

                            # This comparison is now on the potentially truncated and sorted lists
                            if (i >= len_snow or i >= len_bend or 
                                final_snow_lines_to_compare[i] != final_bend_lines_to_compare[i]):
                                line_diff_row = f"  row-{row_num}:"
                                line_diff_snow = f"    snowsql: {snow_line_display}"
                                line_diff_bend = f"    bendsql: {bend_line_display}"
                                logger.error(line_diff_row)
                                logger.error(line_diff_snow)
                                logger.error(line_diff_bend)
                                diff_details_for_summary.extend([line_diff_row, line_diff_snow, line_diff_bend])
                        
                        # Note about row count differences should refer to the lists used for comparison
                        if len_snow != len_bend:
                            row_count_note = f"  (Note: Compared result sets have different number of rows: snowsql {len_snow}, bendsql {len_bend}{truncation_note_for_diff})"
                            logger.error(row_count_note)
                            diff_details_for_summary.append(row_count_note)
                        
                        # Store the captured diff details along with original results for potential deeper inspection if needed
                        failed_tests.append((query_identifier, "\n".join(diff_details_for_summary), bend_result_str, snow_result_str))
                        progress_summary = f" [Progress: passed {len(passed_tests)}, failed {len(failed_tests)}, total {current_query}/{total_queries}]"
                        logger.error(f"❌ DIFFERENCE FOUND (content mismatch after all normalizations){progress_summary}")
                        # The detailed diff has already been logged above.
                        # Original results are captured in failed_tests but no longer printed here.
            

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
        logger.error("\nFailed Tests and Differences:")
        # The tuple now is (query_identifier, captured_diff_string, original_bend_result, original_snow_result)
        # The tuple is (query_identifier, diff_summary_or_flag, bend_data, snow_data)
        for i, (test_identifier, result_info, bend_data, snow_data) in enumerate(failed_tests, 1):
            logger.error(f"  {i}. Test: {test_identifier}")
            if result_info == "Execution Failed":
                logger.error("    Result: Execution Failed")
                MAX_ERROR_LENGTH = 250
                if "Execution OK" not in bend_data:
                    truncated_error = (bend_data[:MAX_ERROR_LENGTH] + '...') if len(bend_data) > MAX_ERROR_LENGTH else bend_data
                    logger.error(f"      bendsql: {truncated_error.replace(chr(10), ' ')}")
                if "Execution OK" not in snow_data:
                    truncated_error = (snow_data[:MAX_ERROR_LENGTH] + '...') if len(snow_data) > MAX_ERROR_LENGTH else snow_data
                    logger.error(f"      snowsql: {truncated_error.replace(chr(10), ' ')}")
            else:
                # This is a comparison failure, print the captured diff
                for line in result_info.splitlines():
                    logger.error(f"    {line}")
            logger.error("-"*40) # Separator for readability
    
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
    logger.info(f"Setting up and executing {sql_tool} scripts for case: {base_sql_dir.split('/')[-1]}")
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
        cases = [c.strip() for c in args.case.split(',')]

    skipped_benchmarks = [b.strip().lower() for b in args.skip.split(',')] if args.skip else []
    
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
        case_name = case
        if case_name.lower() in skipped_benchmarks:
            logger.info(f"SKIPPING benchmark checks: {case_name} as per --skip argument.")
            # Add a placeholder result or handle as appropriate for overall summary
            overall_results["case_results"].append({
                "case": case_name,
                "status": "SKIPPED",
                "summary": "Skipped due to --skip argument."
            })
            continue

        result = run_single_case(case_name, args)
        overall_results["case_results"].append(result)
        
        # Update overall statistics
        overall_results["total_queries"] += result["total_queries"]
        overall_results["passed_queries"] += result["passed_queries"]
        overall_results["failed_queries"] += result["failed_queries"]
        
        if result["success"]:
            overall_results["passed_cases"] += 1
        else:
            overall_results["failed_cases"].append(result["case"])
    
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
            skipped_benchmarks_for_setup = [b.strip().lower() for b in args.skip.split(',')] if args.skip else []
            if case.lower() in skipped_benchmarks_for_setup:
                logger.info(f"SKIPPING setup for benchmark: {case} as per --skip argument.")
            else:
                logger.info(f"\n--- Setting up for case: {case} ---")
                # If --runbend or --runsnow is specified, only run the respective setup and action
                if args.runbend or args.runsnow:
                    if args.runbend:
                        setup_and_execute("bendsql", f"sql/{case}", args.database)
                    if args.runsnow:
                        setup_and_execute("snowsql", f"sql/{case}", args.database, args.warehouse)
                else:
                    # Default behavior: setup and run for both if neither is specified
                    setup_and_execute("bendsql", f"sql/{case}", args.database)
                    setup_and_execute("snowsql", f"sql/{case}", args.database, args.warehouse)
        else:
            # Only skip setup for tpcds; run setup for other cases
            # List of cases to skip setup by default (can be extended)
            HEAVY_SETUP_CASES = ["tpcds"]
            if any(case.lower().startswith(prefix) for prefix in HEAVY_SETUP_CASES):
                logger.info(f"Skipping setup and action scripts for heavy setup case: {case}. Use --setup to run them.")
            else:
                logger.info(f"\n--- Setting up for case: {case} (default since not in heavy setup list) ---")
                if args.runbend or args.runsnow:
                    if args.runbend:
                        setup_and_execute("bendsql", f"sql/{case}", args.database)
                    if args.runsnow:
                        setup_and_execute("snowsql", f"sql/{case}", args.database, args.warehouse)
                else:
                    setup_and_execute("bendsql", f"sql/{case}", args.database)
                    setup_and_execute("snowsql", f"sql/{case}", args.database, args.warehouse)

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
