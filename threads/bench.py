import argparse
import subprocess
import time
import signal
import threading
import os
import re
import statistics  # For calculating statistics
import random  # For generating random IDs
import collections  # For tracking concurrency over time

# Global variables
shutdown_flag = False
operations_lock = threading.Lock()
ongoing_operations = 0
total_operation_time = 0.0
total_executed_operations = 0
execution_times = []  # List to store individual execution times
concurrency_samples = []  # List to track concurrency over time
start_times = []  # List to store operation start times
end_times = []  # List to store operation end times
throughput_window = collections.deque(maxlen=10)  # Store last 10 seconds of throughput data
peak_concurrency = 0  # Track peak concurrency
peak_throughput = 0.0  # Track peak throughput
target_operations = 0  # Total operations to execute

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

    start_time = time.time()
    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
        end_time = time.time()
        execution_time = end_time - start_time
        return result.stdout, execution_time
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"snowsql command failed: {e.stderr}")


def execute_bendsql(query, database):
    """Execute an SQL query using bendsql."""
    command = ["bendsql", "--query=" + query, "--database=" + database, "--time=server"]
    
    start_time = time.time()
    result = subprocess.run(command, text=True, capture_output=True)
    end_time = time.time()
    execution_time = end_time - start_time

    if "APIError: ResponseError" in result.stderr:
        raise RuntimeError(
            f"'APIError: ResponseError' found in bendsql output: {result.stderr}"
        )
    elif result.returncode != 0:
        raise RuntimeError(
            f"bendsql command failed with return code {result.returncode}: {result.stderr}"
        )

    return result.stdout, execution_time


def execute_sql(query, sql_tool, database, warehouse=None):
    """General function to execute a SQL query using the specified tool."""
    if sql_tool == "bendsql":
        return execute_bendsql(query, database)
    elif sql_tool == "snowsql":
        return execute_snowsql(query, database, warehouse)
    else:
        raise ValueError(f"Invalid sql_tool: {sql_tool}")


def execute_sql_file(file_path, sql_tool, database, warehouse, replacements=None):
    """Execute SQL statements from a file."""
    try:
        with open(file_path, 'r') as f:
            sql_content = f.read()
        
        # Apply any replacements to the SQL content
        if replacements:
            for key, value in replacements.items():
                sql_content = sql_content.replace(key, str(value))
        
        # Split the SQL content into individual statements
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        for statement in statements:
            if statement:
                execute_sql(statement, sql_tool, database, warehouse)
                
        return True
    except Exception as e:
        print(f"Error executing SQL file {file_path}: {e}")
        return False


def execute_case_action(thread_number, database, sql_tool, warehouse, case_name):
    """Execute an action query from the specified case's action.sql file."""
    # Read from case action file
    action_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"cases/{case_name}/action.sql")
    try:
        with open(action_file, 'r') as f:
            query = f.read().strip()
        
        # Execute the query directly without any parameter substitution
        output, execution_time = execute_sql(query, sql_tool, database, warehouse)
        return query, execution_time
    except Exception as e:
        print(f"Error executing action from {action_file}: {e}")
        return f"Error: {e}", 0


def execute_init(database, sql_tool, warehouse, case_name=None):
    """Initialize the database and tables for benchmarking.
    If case_name is provided, use the setup.sql file from the case directory.
    Otherwise, use the default initialization."""
    default_db = "default" if sql_tool == "bendsql" else database
    
    if case_name:
        # Use the setup file from the case directory
        setup_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"cases/{case_name}/setup.sql")
        if os.path.exists(setup_file):
            print(f"Running setup from {setup_file}...")
            success = execute_sql_file(setup_file, sql_tool, default_db, warehouse)
            if not success:
                print(f"Warning: Setup from {setup_file} failed. Falling back to default initialization.")
                execute_default_init(database, sql_tool, default_db, warehouse)
        else:
            print(f"Setup file not found at {setup_file}. Using default initialization.")
            execute_default_init(database, sql_tool, default_db, warehouse)
    else:
        # Use default initialization
        execute_default_init(database, sql_tool, default_db, warehouse)


def execute_default_init(database, sql_tool, default_db, warehouse):
    """Execute the default initialization for the benchmark."""
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
    global ongoing_operations, total_executed_operations, total_operation_time
    
    for i in range(start_index, end_index):
        if shutdown_flag:
            break
            
        with operations_lock:
            ongoing_operations += 1
            
        try:
            # Record start time of operation
            op_start_time = time.time()
            with operations_lock:
                start_times.append(op_start_time)
            
            query, execution_time = operation_function(i, database, sql_tool, warehouse)
            
            # Record end time of operation
            op_end_time = time.time()
            with operations_lock:
                end_times.append(op_end_time)
                total_executed_operations += 1
                total_operation_time += execution_time
                execution_times.append(execution_time)
                
            # SQL output removed to avoid cluttering the console
                
        except Exception as e:
            print(f"Error executing operation {i}: {e}")
        finally:
            with operations_lock:
                ongoing_operations -= 1


def status_monitor():
    global shutdown_flag, peak_concurrency, peak_throughput, target_operations
    
    start_time = time.time()
    last_ops_count = 0
    last_time = start_time
    
    # For tracking throughput stability
    throughput_history = []
    
    while not shutdown_flag:
        time.sleep(1)
        current_time = time.time()
        elapsed_time = current_time - start_time
        interval_time = current_time - last_time
        
        with operations_lock:
            # Record the current concurrency level
            current_concurrency = ongoing_operations
            concurrency_samples.append(current_concurrency)
            
            # Update peak concurrency if needed
            if current_concurrency > peak_concurrency:
                peak_concurrency = current_concurrency
            
            # Calculate interval throughput
            interval_ops = total_executed_operations - last_ops_count
            interval_throughput = interval_ops / interval_time if interval_time > 0 else 0
            throughput_window.append(interval_throughput)
            throughput_history.append(interval_throughput)
            
            # Update peak throughput if needed
            if interval_throughput > peak_throughput:
                peak_throughput = interval_throughput
            
            # Calculate overall throughput
            overall_throughput = total_executed_operations / elapsed_time if elapsed_time > 0 else 0
            
            # Calculate recent throughput (last 10 seconds or less)
            recent_throughput = sum(throughput_window) / len(throughput_window) if throughput_window else 0
            
            # Calculate completion percentage
            completion_percentage = (total_executed_operations / target_operations * 100) if target_operations > 0 else 0
            
            # Calculate average operation execution time
            avg_op_time = total_operation_time / total_executed_operations if total_executed_operations > 0 else 0
            
            # Calculate concurrency metrics
            concurrency_efficiency = (current_concurrency / peak_concurrency * 100) if peak_concurrency > 0 else 0
            theoretical_max_throughput = current_concurrency / avg_op_time if avg_op_time > 0 else 0
            throughput_efficiency = (recent_throughput / theoretical_max_throughput * 100) if theoretical_max_throughput > 0 else 0
            
            # Calculate estimated time remaining
            if recent_throughput > 0:
                remaining_ops = target_operations - total_executed_operations
                estimated_time_remaining = remaining_ops / recent_throughput
            else:
                estimated_time_remaining = 0
            
            # Format the time remaining
            if estimated_time_remaining > 0:
                hours, remainder = divmod(estimated_time_remaining, 3600)
                minutes, seconds = divmod(remainder, 60)
                time_remaining_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
            else:
                time_remaining_str = "N/A"
            
            # Calculate throughput stability (coefficient of variation) if we have enough samples
            if len(throughput_history) > 5:
                recent_history = throughput_history[-5:]
                throughput_stddev = statistics.stdev(recent_history) if len(recent_history) > 1 else 0
                throughput_mean = statistics.mean(recent_history)
                throughput_cv = (throughput_stddev / throughput_mean * 100) if throughput_mean > 0 else 0
                stability_str = f"Stability: {100-throughput_cv:.1f}% | "
            else:
                stability_str = ""
            
            # Calculate real time per operation (elapsed time / operations)
            real_time_per_op = elapsed_time / total_executed_operations if total_executed_operations > 0 else 0
            
            # Format elapsed time
            hours, remainder = divmod(elapsed_time, 3600)
            minutes, seconds = divmod(remainder, 60)
            elapsed_time_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
            
            print(
                f"Progress: {completion_percentage:.1f}% [{total_executed_operations}/{target_operations}] | "
                f"Elapsed: {elapsed_time_str} | "
                f"Concurrency: {current_concurrency}/{peak_concurrency} | "
                f"Throughput: {recent_throughput:.2f} ops/s (now), {peak_throughput:.2f} ops/s (peak) | "
                f"Time per op: {real_time_per_op:.4f}s/op | "
                f"ETA: {time_remaining_str}"
            )
            
            # Update for next interval
            last_ops_count = total_executed_operations
            last_time = current_time


def calculate_overlap(start_times, end_times):
    """Calculate the maximum number of concurrent operations."""
    if not start_times or not end_times:
        return 0
        
    # Create events for starts and ends
    events = []
    for t in start_times:
        events.append((t, 1))  # 1 for start
    for t in end_times:
        events.append((t, -1))  # -1 for end
        
    # Sort events by time
    events.sort()
    
    # Calculate concurrent operations at each event
    max_concurrent = 0
    current_concurrent = 0
    for _, event_type in events:
        current_concurrent += event_type
        max_concurrent = max(max_concurrent, current_concurrent)
        
    return max_concurrent


def print_summary(case_name=None, sql_tool=None):
    """Print a summary of the benchmark results with focus on concurrency."""
    global total_executed_operations, total_operation_time, execution_times, concurrency_samples, start_times, end_times, peak_concurrency, peak_throughput
    
    if total_executed_operations == 0:
        print("No operations were executed.")
        return
    
    avg_time = total_operation_time / total_executed_operations
    
    if execution_times:
        min_time = min(execution_times)
        max_time = max(execution_times)
        median_time = statistics.median(execution_times)
        if len(execution_times) > 1:
            stddev_time = statistics.stdev(execution_times)
            cv_time = (stddev_time / avg_time) * 100 if avg_time > 0 else 0  # Coefficient of variation as percentage
        else:
            stddev_time = 0
            cv_time = 0
    else:
        min_time = max_time = median_time = stddev_time = cv_time = 0
    
    # Calculate concurrency metrics
    avg_concurrency = sum(concurrency_samples) / len(concurrency_samples) if concurrency_samples else 0
    max_concurrency = max(concurrency_samples) if concurrency_samples else 0
    max_overlap = calculate_overlap(start_times, end_times)
    
    # Calculate concurrency stability (coefficient of variation)
    if len(concurrency_samples) > 1:
        concurrency_stddev = statistics.stdev(concurrency_samples)
        concurrency_cv = (concurrency_stddev / avg_concurrency) * 100 if avg_concurrency > 0 else 0
    else:
        concurrency_cv = 0
    
    # Calculate throughput metrics
    total_time = max(end_times) - min(start_times) if start_times and end_times else 0
    overall_throughput = total_executed_operations / total_time if total_time > 0 else 0
    
    # Calculate theoretical maximum throughput based on average operation time
    theoretical_max_throughput = max_concurrency / avg_time if avg_time > 0 else 0
    throughput_efficiency = (overall_throughput / theoretical_max_throughput) * 100 if theoretical_max_throughput > 0 else 0
    
    # Calculate throughput over time
    if len(start_times) >= 2:
        # Group operations by second
        ops_by_second = {}
        for end_time in end_times:
            second = int(end_time)
            ops_by_second[second] = ops_by_second.get(second, 0) + 1
        
        # Calculate throughput statistics
        throughput_values = list(ops_by_second.values())
        if len(throughput_values) > 1:
            throughput_stddev = statistics.stdev(throughput_values)
            throughput_mean = statistics.mean(throughput_values)
            throughput_cv = (throughput_stddev / throughput_mean) * 100 if throughput_mean > 0 else 0
            throughput_stability = 100 - throughput_cv  # Higher is more stable
        else:
            throughput_stability = 100
    else:
        throughput_stability = 0
    
    print("\n" + "="*50)
    title = "📊 BENCHMARK SUMMARY"
    if case_name:
        title += f" - CASE: {case_name.upper()}"
    if sql_tool:
        tool_name = "DATABEND" if sql_tool == "bendsql" else "SNOWFLAKE"
        title += f" - {tool_name}"
    print(title)
    print("="*50)
    
    # Calculate concurrency utilization percentage
    concurrency_utilization = (avg_concurrency / peak_concurrency * 100) if peak_concurrency > 0 else 0
    
    # Calculate real time per operation (total time / operations)  
    real_time_per_op = total_time / total_executed_operations if total_executed_operations > 0 else 0
    
    # Basic summary
    print(f"• Operations: {total_executed_operations} completed in {total_time:.2f}s")
    print(f"• Concurrency: {peak_concurrency} threads ({concurrency_utilization:.1f}% utilized)")
    
    print("\n📈 PERFORMANCE METRICS:")
    print(f"• Time per operation: {real_time_per_op:.2f}s/op (total time / operations = {total_time:.2f}s/{total_executed_operations})")
    # Latency sections removed as requested
    print(f"• Throughput: {overall_throughput:.2f} ops/s (avg), {peak_throughput:.2f} ops/s (peak)")
    print(f"• Efficiency: {throughput_efficiency:.1f}% of theoretical maximum")
    
    # Create throughput distribution histogram
    if len(end_times) >= 2:
        # Group operations by second
        ops_by_second = {}
        for end_time in end_times:
            second = int(end_time)
            ops_by_second[second] = ops_by_second.get(second, 0) + 1
        
        throughput_values = list(ops_by_second.values())
        
        # Calculate dynamic buckets based on observed throughput
        max_throughput = max(throughput_values) if throughput_values else 0
        
        # Create dynamic buckets based on max throughput
        if max_throughput <= 1:
            # For very low throughput
            buckets = [(0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.0)]
        elif max_throughput <= 5:
            # For low throughput
            buckets = [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]
        elif max_throughput <= 20:
            # For medium throughput
            buckets = [(0, 2), (2, 5), (5, 10), (10, 15), (15, 20)]
        elif max_throughput <= 100:
            # For high throughput
            buckets = [(0, 10), (10, 25), (25, 50), (50, 75), (75, 100)]
        else:
            # For very high throughput
            bucket_size = max_throughput / 5
            buckets = [(i * bucket_size, (i + 1) * bucket_size) for i in range(5)]
        
        bucket_counts = [0] * len(buckets)
        
        # Count values in each bucket
        for value in throughput_values:
            for i, (lower, upper) in enumerate(buckets):
                if lower <= value < upper or (i == len(buckets) - 1 and value >= lower):
                    bucket_counts[i] += 1
                    break
        
        # Calculate percentages
        total_seconds = len(throughput_values)
        percentages = [count / total_seconds * 100 if total_seconds > 0 else 0 for count in bucket_counts]
        
        # Create ASCII histogram
        print("\n📊 THROUGHPUT DISTRIBUTION:")
        for i, (lower, upper) in enumerate(buckets):
            bucket_label = f"{lower}-{upper} ops/s" if upper != float('inf') else f"{lower}+ ops/s"
            bar_length = int(percentages[i] / 5)  # Scale to reasonable length
            bar = "█" * bar_length
            print(f"{bucket_label:10}: {bar} ({percentages[i]:.0f}%)")
    
    print("="*50)


def benchmark(operation_function, total_operations, num_threads, sql_tool, database, warehouse, case_name=None):
    global shutdown_flag, target_operations
    
    # Set the target operations count
    target_operations = total_operations
    
    print(f"Starting benchmark with {num_threads} threads, targeting {total_operations} total operations...")
    print(f"Using {sql_tool} to connect to database '{database}'")
    print("Monitoring concurrency and throughput. Press Ctrl+C to stop.\n")
    
    # Record start time
    benchmark_start_time = time.time()
    
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
    
    # Record end time
    benchmark_end_time = time.time()
    total_benchmark_time = benchmark_end_time - benchmark_start_time
    
    # Signal the monitor thread to stop
    shutdown_flag = True
    monitor.join()
    
    print(f"\nBenchmarking completed in {total_benchmark_time:.2f} seconds.")
    
    # Print summary statistics
    print_summary(case_name, sql_tool)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Benchmark SQL queries with concurrency.")
    
    parser.add_argument("--total", type=int, default=1000, help="Total number of operations to execute")
    parser.add_argument("--threads", type=int, default=10, help="Number of concurrent threads")
    parser.add_argument("--database", type=str, default=DEFAULT_DATABASE, help="Database name to use")
    parser.add_argument("--warehouse", type=str, default="COMPUTE_WH", help="Warehouse name for Snowflake (default: COMPUTE_WH)")
    parser.add_argument("--case", type=str, help="Test case to run (e.g., 'select'). Uses files from cases/<case> directory")
    
    # Add mutually exclusive group for SQL tool selection
    tool_group = parser.add_mutually_exclusive_group(required=True)
    tool_group.add_argument("--runbend", action="store_true", help="Run benchmark using bendsql")
    tool_group.add_argument("--runsnow", action="store_true", help="Run benchmark using snowsql")
    
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
        print(f"Using Snowflake warehouse: {warehouse}")
    else:
        raise ValueError("Must specify either --runbend or --runsnow")

    # Initialize database and table
    print("Initializing database and table...")
    execute_init(args.database, sql_tool, warehouse, args.case)
    print("Initialization complete.")
    
    # Ensure a case is specified
    if not args.case:
        print("Error: You must specify a test case using --case")
        print("Available cases: select")
        return
        
    print(f"Running benchmark with case: {args.case}")
    # Create a function that executes the action from the specified case
    action_func = lambda thread_num, db, tool, wh: execute_case_action(thread_num, db, tool, wh, args.case)
    
    # Run the benchmark
    benchmark(action_func, args.total, args.threads, sql_tool, args.database, warehouse, args.case)


if __name__ == "__main__":
    main()
