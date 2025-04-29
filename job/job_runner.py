#!/usr/bin/env python3
"""
Job Runner for BendSQL

This script consolidates the functionality of env.sh, load.sh, and run.py into a single Python file.
It provides capabilities to:
1. Set up the BendSQL environment
2. Execute setup.sql to create schema and load data from public AWS bucket
3. Run SQL queries from the queries directory

Usage:
    python job_runner.py [--setup] [--run] [--analyze] [--accurate-histograms]

Options:
    --setup                 Execute setup.sql to create schema and load data
    --run                   Run SQL queries from the queries directory
    --analyze               Analyze tables after setup
    --accurate-histograms   Use accurate histograms when analyzing tables (requires --analyze)
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


class BendSQLRunner:
    """Class to handle BendSQL operations."""

    def __init__(self):
        self.database = "imdb"

    def execute_bendsql(self, query, print_query=True):
        """
        Execute an SQL query using bendsql.
        
        Args:
            query (str): SQL query to execute
            print_query (bool): Whether to print the query before execution
            
        Returns:
            str: Command output
        """
        if print_query:
            print(f">>>> {query}")
        
        command = ["bendsql", "--query=" + query, "--database=" + self.database, "--time=server"]
        result = subprocess.run(command, text=True, capture_output=True)
        
        if "APIError: ResponseError" in result.stderr:
            print(f"'APIError: ResponseError' found in bendsql output: {result.stderr}")
            print("<<<<")
            return None
        elif result.returncode != 0:
            print(f"bendsql command failed with return code {result.returncode}: {result.stderr}")
            print("<<<<")
            return None
        
        if print_query:
            print(result.stdout)
            print("<<<<")
        
        return result.stdout

    def extract_time(self, output):
        """
        Extract execution time from the bendsql output.
        
        Args:
            output (str): Command output
            
        Returns:
            str: Execution time
        """
        match = re.search(r"([0-9.]+)$", output)
        if not match:
            print("Could not extract time from output.")
            return "N/A"
        return match.group(1)

    def execute_sql_file(self, file_path):
        """
        Execute SQL queries from a file.
        
        Args:
            file_path (str): Path to the SQL file
            
        Returns:
            bool: True if successful, False otherwise
        """
        print(f"Executing SQL file: {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                sql_script = f.read()
                
            # Split the script into individual queries
            queries = [query.strip() for query in sql_script.split(';') if query.strip()]
            
            # Execute each query
            for query in queries:
                result = self.execute_bendsql(query)
                if result is None:
                    print(f"Failed to execute query: {query[:100]}...")
                    return False
            
            return True
        except Exception as e:
            print(f"Error executing SQL file: {e}")
            return False

    def setup_database(self):
        """
        Set up the database by executing setup.sql.
        """
        print("Setting up database...")
        
        setup_file = "setup.sql"
        if not os.path.exists(setup_file):
            print(f"Error: {setup_file} not found")
            return False
        
        success = self.execute_sql_file(setup_file)
        if success:
            print("Database setup completed successfully")
        else:
            print("Database setup failed")
        
        return success

    def analyze_tables(self, accurate_histograms=False):
        """
        Analyze tables to generate statistics.
        
        Args:
            accurate_histograms (bool): Whether to use accurate histograms
        """
        print("Analyzing tables...")
        
        # List of tables to analyze
        tables = [
            "aka_name", "aka_title", "cast_info", "char_name", "comp_cast_type",
            "company_name", "company_type", "complete_cast", "info_type", "keyword",
            "kind_type", "link_type", "movie_companies", "movie_info", "movie_info_idx",
            "movie_keyword", "movie_link", "name", "person_info", "role_type", "title"
        ]
        
        # Analyze each table
        for table in tables:
            print(f"Analyzing table {table}...")
            
            # Analyze table with or without accurate histograms
            if accurate_histograms:
                analyze_query = f"set enable_analyze_histogram = 1; analyze table imdb.{table}"
            else:
                analyze_query = f"analyze table imdb.{table}"
            
            self.execute_bendsql(analyze_query)
        
        print("Table analysis completed")

    def run_queries(self):
        """Run SQL queries from the queries directory."""
        print("Starting query execution...")
        
        dir_path = "queries/"
        if not os.path.exists(dir_path):
            print(f"Error: {dir_path} directory not found")
            return False
        
        # Read the SQL files in `queries` directory
        for file in sorted(os.listdir(dir_path)):
            if file.endswith(".sql"):
                print(f"\nExecuting queries from file: {file}")
                with open(Path(dir_path + file), "r") as f:
                    content = f.read()
                    queries = [
                        query.strip() for query in content.split(";") if query.strip()
                    ]
                    # Execute the SQL
                    for query in queries:
                        print(f"Executing query: {query[:100]}...")
                        output = self.execute_bendsql(query)
                        if output:
                            time_elapsed = self.extract_time(output)
                            print(f"Time elapsed: {time_elapsed} seconds\n")
        
        print("Query execution completed.")
        return True


def main():
    """Main function to parse arguments and execute commands."""
    parser = argparse.ArgumentParser(
        description="BendSQL Job Runner - Set up database and execute queries"
    )
    parser.add_argument("--setup", action="store_true", help="Execute setup.sql to create schema and load data")
    parser.add_argument("--run", action="store_true", help="Run SQL queries from the queries directory")
    parser.add_argument("--analyze", action="store_true", help="Analyze tables after setup")
    parser.add_argument("--accurate-histograms", action="store_true", help="Use accurate histograms when analyzing tables")
    
    args = parser.parse_args()
    
    # Create BendSQL runner
    runner = BendSQLRunner()
    
    # Execute commands based on arguments
    if args.setup:
        success = runner.setup_database()
        if not success:
            sys.exit(1)
    
    if args.analyze:
        runner.analyze_tables(args.accurate_histograms)
    
    if args.run:
        success = runner.run_queries()
        if not success:
            sys.exit(1)
    
    # If no arguments provided, show help
    if not (args.setup or args.run or args.analyze):
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()