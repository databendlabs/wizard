import argparse
import subprocess
import sys
import os
import re
import time
from datetime import datetime
import csv


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
    print(f"Database '{database_name}' has been set up. Time: {elapsed_time:.2f}s")
    return elapsed_time


def restart_warehouse(sql_tool, warehouse, database):
    """Restart a specific warehouse by suspending and then resuming it."""
    start_time = time.time()
    
    if sql_tool == "bendsql":
        alter_suspend = f"ALTER WAREHOUSE \"{warehouse}\" SUSPEND;"
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
    
    elapsed_time = time.time() - start_time
    print(f"Warehouse {warehouse} restarted. Time: {elapsed_time:.2f}s")
    return elapsed_time


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
    print(f"\n{'='*50}\n{phase} Execution - {sql_tool} - Started at {datetime.now().strftime('%H:%M:%S')}\n{'='*50}")

    with open(result_file_path, mode) as result_file:
        # Add header for this execution
        result_file.write(f"\n{'='*50}\n{phase} Execution - {sql_tool} - {datetime.now()}\n{'='*50}\n\n")
        
        for index, query in enumerate(queries):
            query_start_time = time.time()
            restart_time = 0
            
            try:
                # Print real-time progress
                print(f"\nQuery {index+1}/{len(queries)} - Started at {datetime.now().strftime('%H:%M:%S')}")
                print(f"SQL: {query}")
                
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
                print(f"Query {index+1} completed:")
                print(f"  - Server execution time: {time_elapsed}s")
                print(f"  - Total time (including restart): {query_total_time:.2f}s")
                if restart_time > 0:
                    print(f"  - Warehouse restart time: {restart_time:.2f}s")
                
                result_file.write(f"SQL: {query}\n")
                result_file.write(f"Time Elapsed (server): {time_elapsed}s\n")
                result_file.write(f"Total time (including restart): {query_total_time:.2f}s\n\n")
                
                results.append({
                    "query_index": index + 1,
                    "server_time": time_elapsed,
                    "total_time": query_total_time,
                    "restart_time": restart_time
                })
                
            except Exception as e:
                query_total_time = time.time() - query_start_time
                print(f"Query {index+1} failed: {e}")
                print(f"  - Total time until failure: {query_total_time:.2f}s")
                
                result_file.write(f"SQL: {query}\nError: {e}\n")
                result_file.write(f"Total time until failure: {query_total_time:.2f}s\n\n")
                
                results.append({
                    "query_index": index + 1,
                    "error": str(e),
                    "total_time": query_total_time,
                    "restart_time": restart_time
                })

    total_wall_time = time.time() - total_start_time
    
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
"""
    
    print(summary)
    with open(result_file_path, "a") as result_file:
        result_file.write(summary)
    
    return {
        "total_execution_time": total_execution_time,
        "total_wall_time": total_wall_time,
        "total_restart_time": total_restart_time,
        "successful_queries": successful_queries,
        "total_queries": len(queries)
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
        "--tpcds",
        action="store_true",
        help="Run TPC-DS queries instead of TPC-H queries",
    )
    return parser.parse_args()


def main():
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
        print("Please specify --runbend or --runsnow.")
        sys.exit(1)

    print(f"\n{'='*50}\nStarting benchmark with {sql_tool}\n{'='*50}")
    print(f"Database: {database}")
    print(f"Warehouse: {warehouse}")
    print(f"Timestamp: {datetime.now()}")
    
    setup_stats = {"total_execution_time": 0, "total_wall_time": 0, "total_restart_time": 0, "successful_queries": 0, "total_queries": 0}
    db_setup_time = 0
    
    if args.setup:
        print(f"\n{'='*50}\nStarting setup phase\n{'='*50}")
        db_setup_time = setup_database(database, sql_tool, warehouse)
        # Choose between TPC-H and TPC-DS setup files
        setup_file = os.path.join(sql_dir, "tpcds_setup.sql" if args.tpcds else "setup.sql")
        setup_stats = execute_sql_file(setup_file, sql_tool, database, warehouse, False, is_setup=True)
        print(f"Setup completed. Total execution time: {setup_stats['total_execution_time']:.2f}s, Wall time: {setup_stats['total_wall_time']:.2f}s")

    # Choose between TPC-H and TPC-DS queries
    queries_file = os.path.join(sql_dir, "tpcds_queries.sql" if args.tpcds else "queries.sql")
    queries_stats = execute_sql_file(queries_file, sql_tool, database, warehouse, args.suspend, is_setup=False)
    print(f"Queries completed. Total execution time: {queries_stats['total_execution_time']:.2f}s, Wall time: {queries_stats['total_wall_time']:.2f}s")

    overall_time = time.time() - overall_start_time
    
    # Print overall summary
    print(f"\n{'='*60}\nFINAL BENCHMARK SUMMARY - {sql_tool.upper()}\n{'='*60}")
    print(f"Database: {database}")
    print(f"Warehouse: {warehouse}")
    print(f"Timestamp: {datetime.now()}")
    print(f"{'='*60}")
    
    if args.setup:
        print(f"SETUP PHASE:")
        print(f"  - Database creation time: {db_setup_time:.2f}s")
        print(f"  - Setup queries: {setup_stats['successful_queries']}/{setup_stats['total_queries']} successful")
        print(f"  - Server execution time: {setup_stats['total_execution_time']:.2f}s")
        print(f"  - Warehouse restart time: {setup_stats['total_restart_time']:.2f}s")
        print(f"  - Total wall time: {setup_stats['total_wall_time']:.2f}s")
    
    print(f"\nQUERIES PHASE:")
    print(f"  - Queries: {queries_stats['successful_queries']}/{queries_stats['total_queries']} successful")
    print(f"  - Server execution time: {queries_stats['total_execution_time']:.2f}s")
    print(f"  - Warehouse restart time: {queries_stats['total_restart_time']:.2f}s")
    print(f"  - Total wall time: {queries_stats['total_wall_time']:.2f}s")
    
    print(f"\nOVERALL:")
    total_server_time = setup_stats['total_execution_time'] + queries_stats['total_execution_time']
    total_restart_time = setup_stats['total_restart_time'] + queries_stats['total_restart_time']
    print(f"  - Total server execution time: {total_server_time:.2f}s")
    print(f"  - Total warehouse restart time: {total_restart_time:.2f}s")
    print(f"  - Total benchmark time: {overall_time:.2f}s")
    print(f"{'='*60}")
    
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
        summary_file.write(f"  - Total benchmark time: {overall_time:.2f}s\n")
        summary_file.write(f"{'='*60}\n")


if __name__ == "__main__":
    main()
