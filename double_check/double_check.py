import os
from databend import Client
import pandas as pd
import sys

def execute_sql_file(client, file_path):
    try:
        with open(file_path, 'r') as file:
            sql_script = file.read()
        commands = sql_script.split(';')
        for command in commands:
            if command.strip() != '':
                client.execute(command)
    except Exception as e:
        print(f"Error executing SQL file: {e}")
        sys.exit(1)

def execute_queries_to_file(client, queries_file, output_file):
    try:
        with open(queries_file, 'r') as file:
            queries = file.read().split(';')
        results = []
        for query in queries:
            if query.strip() != '':
                result = client.query_dataframe(query)
                results.append(result)
        combined_results = pd.concat(results, ignore_index=True)
        combined_results.to_csv(output_file, index=False)
    except Exception as e:
        print(f"Error executing queries: {e}")
        sys.exit(1)

def compare_results(file1, file2):
    try:
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
        return df1.equals(df2)
    except Exception as e:
        print(f"Error comparing results: {e}")
        sys.exit(1)

# Read DSN from environment variables
dsn_v1 = os.getenv('DATABEND_DSN_V1', 'default_dsn_v1')
dsn_v2 = os.getenv('DATABEND_DSN_V2', 'default_dsn_v2')

# File paths
setup_file = "sql/setup.sql"  # SQL file for table creation and data insertion
queries_file = "sql/queries.sql"  # SQL file with queries to execute
results_v1 = "results/results_v1.csv"
results_v2 = "results/results_v2.csv"


try:
    # Setup database (assuming both versions use the same initial setup)
    client_v1 = Client.from_url(dsn_v1)
    execute_sql_file(client_v1, setup_file)

    # Execute queries on Databend v1
    execute_queries_to_file(client_v1, queries_file, results_v1)

    # Execute queries on Databend v2
    client_v2 = Client.from_url(dsn_v2)
    execute_queries_to_file(client_v2, queries_file, results_v2)
except Exception as e:
    print(f"Database connection or execution error: {e}")
    sys.exit(1)

# Compare results
if compare_results(results_v1, results_v2):
    print("Results are the same.")
else:
    print("Results differ.")

