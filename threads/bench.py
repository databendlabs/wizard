import argparse
import subprocess
import time
import signal
import threading
import os
import re

# Global variables
shutdown_flag = False
operations_lock = threading.Lock()
ongoing_operations = 0
total_operation_time = 0.0
total_executed_operations = 0

DEFAULT_DATABASE = "mytestdb"


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


def execute_sql(query, sql_tool, database, warehouse=None):
    """General function to execute a SQL query using the specified tool."""
    if sql_tool == "bendsql":
        return execute_bendsql(query, database)
    elif sql_tool == "snowsql":
        return execute_snowsql(query, database, warehouse)
    else:
        raise ValueError(f"Invalid sql_tool: {sql_tool}")


def execute_select_query(thread_number, database, sql_tool, warehouse):
    query = f"SELECT * FROM test_table LIMIT 1"
    execute_sql(query, sql_tool, database, warehouse)
    return query


def execute_init(database, sql_tool, warehouse):
    # Initialize the database and table
    default_db = "default" if sql_tool == "bendsql" else database

    try:
        query = f"DROP DATABASE IF EXISTS {database}"
        execute_sql(query, sql_tool, default_db, warehouse)
    except Exception as e:
        print(f"Error dropping database: {e}")

    try:
        query = f"CREATE DATABASE {database}"
        execute_sql(query, sql_tool, default_db, warehouse)
    except Exception as e:
        print(f"Error creating database: {e}")

    try:
        query = f"CREATE TABLE {database}.test_table (id INT, name VARCHAR(255))"
        execute_sql(query, sql_tool, default_db, warehouse)
    except Exception as e:
        print(f"Error creating table: {e}")

    # Insert some data
    for i in range(5):
        try:
            query = f"INSERT INTO {database}.test_table (id, name) VALUES ({i}, 'Name {i}')"
            execute_sql(query, sql_tool, default_db, warehouse)
        except Exception as e:
            print(f"Error inserting data: {e}")


def worker_thread(start_index, end_index, operation_function, sql_tool, database, warehouse):
    global ongoing_operations, total_executed_operations
    
    for i in range(start_index, end_index):
        if shutdown_flag:
            break
            
        with operations_lock:
            ongoing_operations += 1
            
        try:
            thread_id = threading.get_ident()
            query = operation_function(i, database, sql_tool, warehouse)
            
            with operations_lock:
                total_executed_operations += 1
                
            print(f"Thread ID: {thread_id}, Executed Query: {query}")
            
        except Exception as e:
            print(f"Error executing operation {i}: {e}")
        finally:
            with operations_lock:
                ongoing_operations -= 1


def status_monitor():
    global shutdown_flag
    
    start_time = time.time()
    
    while not shutdown_flag:
        time.sleep(1)
        current_time = time.time()
        elapsed_time = current_time - start_time
        
        with operations_lock:
            if elapsed_time > 0:
                throughput = total_executed_operations / elapsed_time
            else:
                throughput = 0
                
            print(
                f"Total elapsed time: {elapsed_time:.2f} seconds, "
                f"Operations executed: {total_executed_operations}, "
                f"Throughput: {throughput:.2f} operations/second, "
                f"Concurrency: {ongoing_operations}"
            )


def benchmark(operation_function, total_operations, num_threads, sql_tool, database, warehouse):
    global shutdown_flag
    
    # Create and start worker threads
    threads = []
    for i in range(num_threads):
        start_index = i * (total_operations // num_threads) + 1
        end_index = start_index + (total_operations // num_threads)
        if i == num_threads - 1:
            end_index += total_operations % num_threads
            
        thread = threading.Thread(
            target=worker_thread,
            args=(start_index, end_index, operation_function, sql_tool, database, warehouse)
        )
        thread.start()
        threads.append(thread)
    
    # Start status monitor thread
    monitor = threading.Thread(target=status_monitor)
    monitor.start()
    
    # Wait for all worker threads to complete
    for thread in threads:
        thread.join()
    
    # Signal the monitor thread to stop
    shutdown_flag = True
    monitor.join()
    
    print("Benchmarking completed.")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Benchmark queries in SQL databases.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--runbend", action="store_true", help="Use bendsql tool")
    group.add_argument("--runsnow", action="store_true", help="Use snowsql tool")

    parser.add_argument(
        "--total", type=int, default=100, help="Total number of operations"
    )
    parser.add_argument(
        "--threads", type=int, default=10, help="Number of threads to use"
    )
    parser.add_argument(
        "--database", default=DEFAULT_DATABASE, help="Database name to use", required=False
    )
    parser.add_argument(
        "--warehouse",
        default="COMPUTE_WH",
        help="Warehouse name for snowsql",
        required=False,
    )
    return parser.parse_args()


def signal_handler(signum, frame):
    global shutdown_flag
    shutdown_flag = True
    print("Interrupt signal received, initiating graceful shutdown.")


def main():
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    # Parse command line arguments
    args = parse_arguments()

    # Determine which SQL tool to use
    if args.runbend:
        sql_tool = "bendsql"
        try:
            warehouse = get_bendsql_warehouse_from_env()
        except ValueError as e:
            print(f"Error getting bendsql warehouse: {e}")
            warehouse = None
    elif args.runsnow:
        sql_tool = "snowsql"
        warehouse = args.warehouse
    else:
        raise ValueError("Must specify either --runbend or --runsnow")

    # Initialize database and table
    execute_init(args.database, sql_tool, warehouse)
    
    # Run the benchmark
    benchmark(execute_select_query, args.total, args.threads, sql_tool, args.database, warehouse)


if __name__ == "__main__":
    main()
