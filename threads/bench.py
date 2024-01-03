import argparse
import subprocess
from termcolor import colored
import time
import signal
from multiprocessing import Process, Value, Lock

shutdown_flag = Value("b", False)
operations_lock = Lock()
ongoing_operations = Value("i", 0)
total_operation_time = Value("d", 0.0)
total_executed_operations = Value("i", 0)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Benchmark queries in SQL databases.")
    parser.add_argument(
        "--runbend", action="store_true", help="Run only bendsql queries"
    )
    parser.add_argument(
        "--runsnow", action="store_true", help="Run only snowsql queries"
    )
    parser.add_argument(
        "--creates", action="store_true", help="Benchmark table creation"
    )
    parser.add_argument(
        "--selects", action="store_true", help="Benchmark select queries"
    )
    parser.add_argument(
        "--total", type=int, default=50000, help="Total number of operations"
    )
    parser.add_argument(
        "--threads", type=int, default=50, help="Number of processes to use"
    )
    parser.add_argument(
        "--database", default="testdb", help="Database name to use", required=False
    )
    parser.add_argument(
        "--warehouse",
        default="COMPUTE_WH",
        help="Warehouse name for snowsql",
        required=False,
    )
    return parser.parse_args()


def signal_handler(signum, frame):
    with shutdown_flag.get_lock():
        shutdown_flag.value = True
    print("Interrupt signal received, initiating graceful shutdown.")


def log_operation_start():
    with operations_lock:
        with ongoing_operations.get_lock():
            ongoing_operations.value += 1


def log_operation_end(operation_time):
    with operations_lock:
        with ongoing_operations.get_lock():
            ongoing_operations.value -= 1
        with total_operation_time.get_lock():
            total_operation_time.value += operation_time
        with total_executed_operations.get_lock():
            total_executed_operations.value += 1


def execute_sql(query, sql_tool, database, warehouse=None):
    command = [sql_tool]
    if sql_tool == "snowsql":
        command.extend(
            [
                "--query",
                f'"{query}"',
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

    try:
        start_time = time.time()
        log_operation_start()
        result = subprocess.run(command, text=True, capture_output=True, check=False)
        output = result.stdout
        error = result.stderr
        log_operation_end(time.time() - start_time)

        return output
    except subprocess.CalledProcessError as e:
        print(f"Executing command: {' '.join(command)}")
        error_message = f"{sql_tool} command failed: {e.stderr}"
        print(colored(error_message, "red"))
        exit(1)


def execute_create_table(table_number, database, sql_tool, warehouse):
    query = f"CREATE TABLE IF NOT EXISTS table_{table_number} (id INT, name VARCHAR)"
    execute_sql(query, sql_tool, database, warehouse)


def execute_select_query(thread_number, database, sql_tool, warehouse):
    query = f"SELECT * FROM test_table LIMIT 1"
    execute_sql(query, sql_tool, database, warehouse)


def execute_init(table_number, database, sql_tool, warehouse):
    # Initialize the database and table
    query = f"DROP DATABASE IF EXISTS {database}"
    execute_sql(query, sql_tool, database, warehouse)

    query = f"CREATE DATABASE {database}"
    execute_sql(query, sql_tool, database, warehouse)

    query = f"CREATE TABLE test_table (id INT, name VARCHAR)"
    execute_sql(query, sql_tool, database, warehouse)


def execute_operations_batch(start_index, end_index, operation_function):
    for i in range(start_index, end_index):
        if shutdown_flag.value:
            break
        operation_function(i)


def run_benchmark(operation_function, total_operations, num_threads):
    processes = []
    for i in range(num_threads):
        if shutdown_flag.value:
            break
        start_index = i * (total_operations // num_threads) + 1
        end_index = start_index + (total_operations // num_threads)
        if i == num_threads - 1:
            end_index += total_operations % num_threads
        process = Process(
            target=execute_operations_batch,
            args=(start_index, end_index, operation_function),
        )
        process.start()
        processes.append(process)

    status_process = Process(target=print_status)
    status_process.start()

    for process in processes:
        process.join()

    status_process.join()

    with shutdown_flag.get_lock():
        shutdown_flag.value = True
    print("Benchmarking completed.")


def print_status():
    while not shutdown_flag.value:
        time.sleep(1)
        with operations_lock:
            avg_operation_time = (
                total_operation_time.value / total_executed_operations.value
                if total_executed_operations.value > 0
                else 0
            )
            print(
                f"Execute times: {total_executed_operations.value}, Avg cost: {avg_operation_time:.4f} seconds, Concurrency: {ongoing_operations.value}"
            )


def main():
    args = parse_arguments()
    sql_tool = "snowsql" if args.runsnow else "bendsql"
    warehouse = args.warehouse if args.runsnow else None

    if args.creates:
        execute_init(0, args.database, sql_tool, warehouse)
        operation_function = lambda i: execute_create_table(
            i, args.database, sql_tool, args.warehouse
        )
    elif args.selects:
        execute_init(0, args.database, sql_tool, warehouse)
        operation_function = lambda i: execute_select_query(
            i, args.database, sql_tool, args.warehouse
        )
    else:
        print("No benchmark type specified. Please use '--creates' or '--selects'.")
        return

    run_benchmark(operation_function, args.total, args.threads)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    main()
