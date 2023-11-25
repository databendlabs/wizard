import argparse
import os
from sqlalchemy import create_engine, text
from termcolor import colored


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Check SQL scripts on Databend and Snowflake."
    )
    parser.add_argument("--database", help="Database name", required=True)
    args = parser.parse_args()
    return args.database


def execute_sql_scripts(engine, script_path, database_name):
    with open(script_path, "r") as file:
        sql_script = file.read()
    queries = sql_script.split(";")
    for query in queries:
        if query.strip():
            print(f"Executing query for {database_name}:")
            print(query)
            # Execute each query
            with engine.begin() as conn:
                conn.execute(text(query))
            print(f"Query executed for {database_name}.")


def fetch_query_results(engine, query, database_name):
    with engine.begin() as conn:
        print(f"Fetching results for {database_name}:")
        print(query)
        result = conn.execute(text(query))
        results = result.fetchall()
        print(f"Results fetched for {database_name}.")
        return results


def main():
    database = parse_arguments()

    # Read database connection DSNs from environment variables with format hints
    databend_dsn = os.environ.get("DATABEND_DSN")
    snowflake_dsn = os.environ.get("SNOWFLAKE_DSN")

    if not databend_dsn:
        print(
            "Please set the DATABEND_DSN environment variable in the format 'databend://{username}:{password}@{host_port_name}/{database_name}?secure=false'"
        )
        return

    if not snowflake_dsn:
        print(
            "Please set the SNOWFLAKE_DSN environment variable in the format 'snowflake://USER:PASSWORD@ACCOUNT.snowflakecomputing.com/DB'"
        )
        return

    # Create engine for Databend and Snowflake
    databend_engine = create_engine(databend_dsn)
    snowflake_engine = create_engine(snowflake_dsn)

    # Execute setup scripts
    execute_sql_scripts(databend_engine, "sql/bend/setup.sql", database)
    execute_sql_scripts(snowflake_engine, "sql/snow/setup.sql", database)

    # Execute action scripts
    execute_sql_scripts(databend_engine, "sql/bend/action.sql", database)
    execute_sql_scripts(snowflake_engine, "sql/snow/action.sql", database)

    # Compare results from check.sql
    with open("sql/check.sql", "r") as file:
        check_queries = file.read().split(";")
    for query in check_queries:
        if query.strip():
            bend_result = fetch_query_results(databend_engine, query, database)
            snow_result = fetch_query_results(snowflake_engine, query, database)

            if bend_result == snow_result:
                print(colored("OK", "green"))
            else:
                print(colored("ERROR", "red"))
                print("Query:")
                print(query)
                break


if __name__ == "__main__":
    main()
