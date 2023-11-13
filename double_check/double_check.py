import os
import asyncio
import pandas as pd
from databend_driver import AsyncDatabendClient
import argparse

async def execute_sql_file(conn, file_path):
    print(f"Executing SQL file: {file_path}")
    try:
        with open(file_path, 'r') as file:
            sql_script = file.read()
        commands = sql_script.split(';')
        for command in commands:
            command = command.strip()
            if command != '':
                print(f"Executing command: {command}")
                await conn.exec(command)
        print("SQL file executed successfully.")
    except Exception as e:
        print(f"Error executing SQL file: {e}")
        raise

async def execute_queries_to_file(conn, queries_file, output_file):
    print(f"Executing queries from file: {queries_file} and saving to {output_file}")
    try:
        with open(queries_file, 'r') as file:
            queries = file.read().split(';')
        results = []
        for query in queries:
            query = query.strip()
            if query != '':
                print(f"Executing query: {query}")
                rows = await conn.query_iter(query)
                async for row in rows:
                    results.append(row.values())
        combined_results = pd.DataFrame(results)
        combined_results.to_csv(output_file, index=False)
        print(f"Queries executed and saved to {output_file}")
    except Exception as e:
        print(f"Error executing queries: {e}")
        raise

async def compare_results(file1, file2):
    print(f"Comparing results from {file1} and {file2}")
    try:
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
        comparison_result = df1.equals(df2)
        print("Comparison completed.")
        return comparison_result
    except Exception as e:
        print(f"Error comparing results: {e}")
        raise

async def main():
    # Setup argparse to parse command line arguments
    parser = argparse.ArgumentParser(description='Run Databend queries and compare results.')
    parser.add_argument('--skipsetup', action='store_true', help='Skip the setup SQL execution if provided')
    args = parser.parse_args()

    # Read DSNs from environment variables
    dsn_v1 = os.getenv('DATABEND_DSN_V1', 'default_dsn_v1')
    dsn_v2 = os.getenv('DATABEND_DSN_V2', 'default_dsn_v2')

    # File paths
    setup_file = "sql/setup.sql"  # SQL file for table creation and data insertion
    queries_file = "sql/queries.sql"  # SQL file with queries to execute
    results_v1 = "results/results_v1.csv"
    results_v2 = "results/results_v2.csv"

    print("Starting script execution.")

    try:
        # Setup database (assuming both versions use the same initial setup)
        if not args.skipsetup:
            print("Setting up database using Databend v1 client.")
            client_v1 = AsyncDatabendClient(dsn_v1)
            conn_v1 = await client_v1.get_conn()
            await execute_sql_file(conn_v1, setup_file)
        else:
            print("Skipping setup as per --skipsetup flag.")

        # Execute queries on Databend v1
        print("Executing queries on Databend v1.")
        client_v1 = AsyncDatabendClient(dsn_v1)
        conn_v1 = await client_v1.get_conn()
        await execute_queries_to_file(conn_v1, queries_file, results_v1)

        # Execute queries on Databend v2
        print("Executing queries on Databend v2.")
        client_v2 = AsyncDatabendClient(dsn_v2)
        conn_v2 = await client_v2.get_conn()
        await execute_queries_to_file(conn_v2, queries_file, results_v2)
    except Exception as e:
        print(f"Database connection or execution error: {e}")
        return

    # Compare results
    print("Comparing results between Databend v1 and v2.")
    if await compare_results(results_v1, results_v2):
        print("Results are the same.")
    else:
        print("Results differ.")

    print("Script execution completed.")

asyncio.run(main())
