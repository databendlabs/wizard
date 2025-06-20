import argparse
import subprocess
import sys
import os
import re
import time
from datetime import datetime
import csv
import math
import logging

# Global logger instance
logger = logging.getLogger(__name__)


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
    create_query = f"CREATE OR REPLACE DATABASE {database_name};"
    
    start_time = time.time()
    if sql_tool == "bendsql":
        # For bendsql, we need to use a default database when creating a new one
        execute_sql(create_query, sql_tool, "default", warehouse)
    else:
        # For snowsql, we can use the target database name
        execute_sql(create_query, sql_tool, database_name, warehouse)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Database '{database_name}' has been set up. Time: {elapsed_time:.2f}s")
    return elapsed_time


def restart_warehouse(sql_tool, warehouse, database):
    """Restart a specific warehouse by suspending and then resuming it."""
    start_time = time.time()
    
    if sql_tool == "bendsql":
        alter_suspend = f"ALTER WAREHOUSE \"{warehouse}\" SUSPEND;"
    else:
        alter_suspend = f"ALTER WAREHOUSE {warehouse} SUSPEND;"

    logger.info(f"Suspending warehouse {warehouse}...")
    execute_sql(alter_suspend, sql_tool, database, warehouse)

    time.sleep(2)
    if sql_tool == "bendsql":
        alter_resume = f"ALTER WAREHOUSE '{warehouse}' RESUME;"
    else:
        alter_resume = f"ALTER WAREHOUSE {warehouse} RESUME;"

    execute_sql(alter_resume, sql_tool, database, warehouse)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Warehouse {warehouse} restarted. Time: {elapsed_time:.2f}s")
    return elapsed_time


def create_ascii_table(data, headers, title=None):
    """Create an ASCII table from data."""
    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in data:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    
    # Create horizontal line
    h_line = '+' + '+'.join('-' * (w + 2) for w in col_widths) + '+'
    
    # Create table
    table = []
    if title:
        table.append(title)
    table.append(h_line)
    
    # Add headers
    header_row = '|' + '|'.join(' ' + h.ljust(w) + ' ' for h, w in zip(headers, col_widths)) + '|'
    table.append(header_row)
    table.append(h_line)
    
    # Add data rows
    for row in data:
        data_row = '|' + '|'.join(' ' + str(cell).ljust(w) + ' ' for cell, w in zip(row, col_widths)) + '|'
        table.append(data_row)
    
    table.append(h_line)
    return '\n'.join(table)


def execute_sql_file(sql_file, sql_tool, database, warehouse, suspend, is_setup=False):
    """Execute SQL queries from a file using the specified tool and write results to a file."""
    with open(sql_file, "r") as file:
        queries = [query.strip() for query in file.read().split(";") if query.strip()]

    results = []
    result_file_path = "query_results.txt"
    mode = "a" if os.path.exists(result_file_path) else "w"
    
    # Create CSV file for results
    csv_file_path = "result.csv"
    with open(csv_file_path, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["Query", "Time(s)"])  # Header
    
    total_start_time = time.time()
    successful_queries = 0
    total_execution_time = 0.0
    total_restart_time = 0.0
    
    phase = "Setup" if is_setup else "Queries"
    logger.info(f"\n{'='*50}\n{phase} Execution - {sql_tool} - Started at {datetime.now().strftime('%H:%M:%S')}\n{'='*50}")

    with open(result_file_path, mode) as result_file:
        # Add header for this execution
        result_file.write(f"\n{'='*50}\n{phase} Execution - {sql_tool} - {datetime.now()}\n{'='*50}\n\n")
        
        for index, query in enumerate(queries):
            query_start_time = time.time()
            restart_time = 0
            
            try:
                # Print real-time progress
                logger.info(f"\nQuery {index+1}/{len(queries)} - Started at {datetime.now().strftime('%H:%M:%S')}")
                logger.info(f"SQL: {query}")
                
                if suspend:
                    restart_time = restart_warehouse(sql_tool, warehouse, database)
                    total_restart_time += restart_time

                query_exec_start = time.time()
                output = execute_sql(query, sql_tool, database, warehouse)

                if sql_tool == "snowsql":
                    time_elapsed = extract_snowsql_time(output)
                else:
                    time_elapsed = extract_bendsql_time(output)

                if time_elapsed:
                    time_elapsed_float = float(time_elapsed)
                    total_execution_time += time_elapsed_float
                    successful_queries += 1
                    
                    # Write to CSV file
                    with open(csv_file_path, "a", newline="") as csvfile:
                        csv_writer = csv.writer(csvfile)
                        csv_writer.writerow([index + 1, time_elapsed_float])
                
                query_total_time = time.time() - query_start_time
                
                # Print real-time timing information
                logger.info(f"Query {index+1} completed:")
                logger.info(f"  - Server execution time: {time_elapsed}s")
                logger.info(f"  - Total time (including restart): {query_total_time:.2f}s")
                if restart_time > 0:
                    logger.info(f"  - Warehouse restart time: {restart_time:.2f}s")
                
                result_file.write(f"SQL: {query}\n")
                result_file.write(f"Time Elapsed (server): {time_elapsed}s\n")
                result_file.write(f"Total time (including restart): {query_total_time:.2f}s\n\n")
                
                results.append({
                    "query_index": index + 1,
                    "server_time": float(time_elapsed) if isinstance(time_elapsed, str) else time_elapsed,
                    "total_time": query_total_time,
                    "restart_time": restart_time
                })
                
            except Exception as e:
                query_total_time = time.time() - query_start_time
                logger.error(f"Query {index+1} failed: {e}")
                logger.error(f"  - Total time until failure: {query_total_time:.2f}s")
                
                result_file.write(f"SQL: {query}\nError: {e}\n")
                result_file.write(f"Total time until failure: {query_total_time:.2f}s\n\n")
                
                results.append({
                    "query_index": index + 1,
                    "error": str(e),
                    "total_time": query_total_time,
                    "restart_time": restart_time,
                    "server_time": 0.0
                })

    total_wall_time = time.time() - total_start_time
    
    # Create ASCII table for query times
    table_data = []
    for result in results:
        if "error" not in result:
            table_data.append([result["query_index"], f"{result['server_time']:.2f}s"])
    
    # Sort by query index
    table_data.sort(key=lambda x: x[0])
    
    # Create ASCII table
    query_times_table = create_ascii_table(table_data, ["Query", "Time(s)"], f"{phase} Query Execution Times:")
    
    # Print and write summary statistics
    summary = f"""
{phase} Execution Summary ({sql_tool}):
----------------------------------------
Total queries: {len(queries)}
Successful queries: {successful_queries}
Failed queries: {len(queries) - successful_queries}
Total server execution time: {total_execution_time:.2f}s
Total warehouse restart time: {total_restart_time:.2f}s
Total wall clock time: {total_wall_time:.2f}s
Average query time (server): {(total_execution_time / successful_queries if successful_queries else 0):.2f}s

{query_times_table}
"""
    
    logger.info(summary)
    with open(result_file_path, "a") as result_file:
        result_file.write(summary)
    
    return {
        "total_execution_time": total_execution_time,
        "total_wall_time": total_wall_time,
        "total_restart_time": total_restart_time,
        "successful_queries": successful_queries,
        "total_queries": len(queries),
        "results": results
    }


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
        "--suspend",
        default=False,
        action="store_true",
        help="Restart the warehouse before each query",
    )
    parser.add_argument(
        "--case",
        choices=['tpch', 'tpcds'],
        default='tpch',
        help="Specify the benchmark case: TPC-H (default) or TPC-DS",
    )
    return parser.parse_args()


def main():
    # Setup logging
    log_filename = f"benchsb_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )

    args = parse_arguments()

    base_sql_dir = "sql"  # Base directory for SQL files
    database = args.database
    overall_start_time = time.time()

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
        logger.error("Please specify --runbend or --runsnow.")
        sys.exit(1)

    logger.info(f"\n{'='*50}\nStarting benchmark with {sql_tool}\n{'='*50}")
    logger.info(f"Database: {database}")
    logger.info(f"Warehouse: {warehouse}")
    logger.info(f"Timestamp: {datetime.now()}")
    
    setup_stats = {"total_execution_time": 0, "total_wall_time": 0, "total_restart_time": 0, "successful_queries": 0, "total_queries": 0}
    db_setup_time = 0
    
    if args.setup:
        logger.info(f"\n{'='*50}\nStarting setup phase\n{'='*50}")
        db_setup_time = setup_database(database, sql_tool, warehouse)
        # Choose between TPC-H and TPC-DS setup files
        setup_file = os.path.join(sql_dir, "tpcds_setup.sql" if args.case == 'tpcds' else "setup.sql")
        setup_stats = execute_sql_file(setup_file, sql_tool, database, warehouse, False, is_setup=True)
        logger.info(f"Setup completed. Total execution time: {setup_stats['total_execution_time']:.2f}s, Wall time: {setup_stats['total_wall_time']:.2f}s")

    # Choose between TPC-H and TPC-DS queries
    queries_file = os.path.join(sql_dir, "tpcds_queries.sql" if args.case == 'tpcds' else "queries.sql")
    queries_stats = execute_sql_file(queries_file, sql_tool, database, warehouse, args.suspend, is_setup=False)
    logger.info(f"Queries completed. Total execution time: {queries_stats['total_execution_time']:.2f}s, Wall time: {queries_stats['total_wall_time']:.2f}s")

    overall_time = time.time() - overall_start_time
    
    # Print overall summary
    logger.info(f"\n{'='*60}\nFINAL BENCHMARK SUMMARY - {sql_tool.upper()}\n{'='*60}")
    logger.info(f"Database: {database}")
    logger.info(f"Warehouse: {warehouse}")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info(f"{'='*60}")
    
    if args.setup:
        logger.info(f"SETUP PHASE:")
        logger.info(f"  - Database creation time: {db_setup_time:.2f}s")
        logger.info(f"  - Setup queries: {setup_stats['successful_queries']}/{setup_stats['total_queries']} successful")
        logger.info(f"  - Server execution time: {setup_stats['total_execution_time']:.2f}s")
        logger.info(f"  - Warehouse restart time: {setup_stats['total_restart_time']:.2f}s")
        logger.info(f"  - Total wall time: {setup_stats['total_wall_time']:.2f}s")
    
    logger.info(f"\nQUERIES PHASE:")
    logger.info(f"  - Queries: {queries_stats['successful_queries']}/{queries_stats['total_queries']} successful")
    logger.info(f"  - Server execution time: {queries_stats['total_execution_time']:.2f}s")
    logger.info(f"  - Warehouse restart time: {queries_stats['total_restart_time']:.2f}s")
    logger.info(f"  - Total wall time: {queries_stats['total_wall_time']:.2f}s")
    
    logger.info(f"\nOVERALL:")
    total_server_time = setup_stats['total_execution_time'] + queries_stats['total_execution_time']
    total_restart_time = setup_stats['total_restart_time'] + queries_stats['total_restart_time']
    logger.info(f"  - Total server execution time: {total_server_time:.2f}s")
    logger.info(f"  - Total warehouse restart time: {total_restart_time:.2f}s")
    logger.info(f"  - Total benchmark time: {overall_time:.2f}s")
    logger.info(f"{'='*60}")
    
    # Generate comparison table
    headers = ["Metric", "Setup", "Queries", "Overall"]
    data = [
        ["Successful Queries", f"{setup_stats['successful_queries']}/{setup_stats['total_queries']}", f"{queries_stats['successful_queries']}/{queries_stats['total_queries']}", f"{setup_stats['successful_queries'] + queries_stats['successful_queries']}/{setup_stats['total_queries'] + queries_stats['total_queries']}"],
        ["Server Execution Time (s)", f"{setup_stats['total_execution_time']:.2f}", f"{queries_stats['total_execution_time']:.2f}", f"{setup_stats['total_execution_time'] + queries_stats['total_execution_time']:.2f}"],
        ["Warehouse Restart Time (s)", f"{setup_stats['total_restart_time']:.2f}", f"{queries_stats['total_restart_time']:.2f}", f"{setup_stats['total_restart_time'] + queries_stats['total_restart_time']:.2f}"],
        ["Total Wall Time (s)", f"{setup_stats['total_wall_time']:.2f}", f"{queries_stats['total_wall_time']:.2f}", f"{overall_time:.2f}"]
    ]
    
    summary_table = create_ascii_table(data, headers, "Overall Benchmark Summary")
    logger.info(f"\n{summary_table}")
    
    # Create ASCII table for query times if queries were executed
    if 'results' in queries_stats:
        table_data = []
        for result in queries_stats['results']:
            if "error" not in result:
                table_data.append([result["query_index"], f"{result['server_time']:.2f}s"])
        
        # Sort by query index
        table_data.sort(key=lambda x: x[0])
        
        # Create ASCII table
        query_times_table = create_ascii_table(table_data, ["Query", "Time(s)"], "Query Execution Times:")
    else:
        query_times_table = "No query results available."
    
    # Write summary to file
    with open("benchmark_summary.txt", "a") as summary_file:
        summary_file.write(f"\n{'='*60}\nBENCHMARK SUMMARY - {sql_tool.upper()} - {datetime.now()}\n{'='*60}\n")
        summary_file.write(f"Database: {database}\n")
        summary_file.write(f"Warehouse: {warehouse}\n\n")
        
        if args.setup:
            summary_file.write(f"SETUP PHASE:\n")
            summary_file.write(f"  - Database creation time: {db_setup_time:.2f}s\n")
            summary_file.write(f"  - Setup queries: {setup_stats['successful_queries']}/{setup_stats['total_queries']} successful\n")
            summary_file.write(f"  - Server execution time: {setup_stats['total_execution_time']:.2f}s\n")
            summary_file.write(f"  - Warehouse restart time: {setup_stats['total_restart_time']:.2f}s\n")
            summary_file.write(f"  - Total wall time: {setup_stats['total_wall_time']:.2f}s\n\n")
        
        summary_file.write(f"QUERIES PHASE:\n")
        summary_file.write(f"  - Queries: {queries_stats['successful_queries']}/{queries_stats['total_queries']} successful\n")
        summary_file.write(f"  - Server execution time: {queries_stats['total_execution_time']:.2f}s\n")
        summary_file.write(f"  - Warehouse restart time: {queries_stats['total_restart_time']:.2f}s\n")
        summary_file.write(f"  - Total wall time: {queries_stats['total_wall_time']:.2f}s\n\n")
        
        summary_file.write(f"OVERALL:\n")
        summary_file.write(f"  - Total server execution time: {total_server_time:.2f}s\n")
        summary_file.write(f"  - Total warehouse restart time: {total_restart_time:.2f}s\n")
        summary_file.write(f"  - Total benchmark time: {overall_time:.2f}s\n\n")
        
        # Add query times table to summary
        summary_file.write(f"{query_times_table}\n\n")
        summary_file.write(f"{'='*60}\n")


if __name__ == "__main__":
    main()
