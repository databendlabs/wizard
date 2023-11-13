import os
import asyncio
import pandas as pd
from databend_driver import AsyncDatabendClient
import argparse
import datetime

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

async def create_database(conn, database_name):
    print(f"Creating database: {database_name}")
    await conn.exec(f"DROP DATABASE IF EXISTS {database_name}")
    await conn.exec(f"CREATE DATABASE {database_name}")

async def execute_sql_file(conn, file_path, database_name):
    print(f"Executing SQL file: {file_path}")
    try:
        with open(file_path, 'r') as file:
            sql_script = file.read()
        commands = sql_script.split(';')
        for idx, command in enumerate(commands, start=1):
            command = command.strip()
            if command != '':
                full_command = f"USE {database_name}; {command}"
                print(f"Executing command #{idx}: {full_command.replace('\n', ' ')}")
                await conn.exec(full_command)
        print("SQL file executed successfully.")
    except Exception as e:
        print(f"Error executing SQL file: {e}")
        raise

async def execute_queries_to_file(conn, queries_file, output_file, database_name):
    print(f"Executing queries from file: {queries_file} and saving to {output_file}")
    executed_queries = []
    try:
        with open(queries_file, 'r') as file:
            queries = file.read().split(';')
        results = []
        for idx, query in enumerate(queries, start=1):
            query = query.strip()
            if query != '':
                full_query = f"USE {database_name}; {query}"
                print(f"Executing query #{idx}: {full_query.replace('\n', ' ')}")
                rows = await conn.query_iter(full_query)
                async for row in rows:
                    results.append(row.values())
                executed_queries.append((idx, query))
        combined_results = pd.DataFrame(results)
        combined_results.to_csv(output_file, index=False)
        print(f"Queries executed and saved to {output_file}")
    except Exception as e:
        print(f"Error executing queries: {e}")
        raise
    return executed_queries

async def compare_results(file1, file2, executed_queries):
    print(f"Comparing results from {file1} and {file2}")
    try:
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
        if df1.shape != df2.shape:
            print("Results differ in shape.")
            return
        for idx, row in enumerate(zip(df1.values, df2.values)):
            row1, row2 = row
            if not (row1 == row2).all():
                query_idx, query_text = executed_queries[idx]
                print(f"Results differ at query #{query_idx}: {query_text}")
                print(f"V1 Result: {row1}")
                print(f"V2 Result: {row2}")
                break
        else:
            print("Results are the same.")
    except Exception as e:
        print(f"Error comparing results: {e}")
        raise

async def main():
    parser = argparse.ArgumentParser(description='Run Databend queries and compare results.')
    parser.add_argument('--skipsetup', action='store_true', help='Skip the setup SQL execution if provided')
    args = parser.parse_args()

    today = datetime.datetime.now().strftime("%Y%m%d")
    database_name = f"double_check_{today}"

    dsn_v1 = os.getenv('DATABEND_DSN_V1', 'default_dsn_v1')
    dsn_v2 = os.getenv('DATABEND_DSN_V2', 'default_dsn_v2')

    setup_file = "sql/setup.sql"
    queries_file = "sql/queries.sql"
    results_dir = "results"
    results_v1 = os.path.join(results_dir, "results_v1.csv")
    results_v2 = os.path.join(results_dir, "results_v2.csv")

    print("Starting script execution.")

    try:
        ensure_dir(results_dir)

        if not args.skipsetup:
            client_v1 = AsyncDatabendClient(dsn_v1)
            conn_v1 = await client_v1.get_conn()
            await create_database(conn_v1, database_name)
            await execute_sql_file(conn_v1, setup_file, database_name)

        client_v1 = AsyncDatabendClient(dsn_v1)
        conn_v1 = await client_v1.get_conn()
        executed_queries_v1 = await execute_queries_to_file(conn_v1, queries_file, results_v1, database_name)

        client_v2 = AsyncDatabendClient(dsn_v2)
        conn_v2 = await client_v2.get_conn()
        executed_queries_v2 = await execute_queries_to_file(conn_v2, queries_file, results_v2, database_name)

        if executed_queries_v1 != executed_queries_v2:
            print("Warning: The queries executed in V1 and V2 are different.")
        await compare_results(results_v1, results_v2, executed_queries_v1)
    except Exception as e:
        print(f"Database connection or execution error: {e}")
    finally:
        await conn_v1.close()
        await conn_v2.close()

    print("Script execution completed.")

asyncio.run(main())
