#!/usr/bin/env python3
"""
Job Runner for BendSQL

This script consolidates the functionality of env.sh, load.sh, and run.py into a single Python file.
It provides capabilities to:
1. Set up the BendSQL environment
2. Execute setup.sql to create schema and load data from public AWS bucket
3. Run SQL queries from the queries directory
4. Compare query performance with three different analyze methods:
   - Raw (no analyze)
   - Standard analyze
   - Analyze with accurate histograms

Usage:
    python job_runner.py [--setup] [--run] [--analyze] [--accurate-histograms] [--compare]

Options:
    --setup                 Execute setup.sql to create schema and load data
    --run                   Run SQL queries from the queries directory
    --analyze               Analyze tables after setup
    --accurate-histograms   Use accurate histograms when analyzing tables (requires --analyze)
    --compare               Compare query performance with different analyze methods
"""

import argparse
import os
import re
import subprocess
import sys
import time
import json
from pathlib import Path
from datetime import datetime


class BendSQLRunner:
    """Class to handle BendSQL operations."""

    def __init__(self):
        self.database = "imdb"
        self.log_file = None

    def setup_logging(self, log_dir="logs"):
        """
        Set up logging to a file.
        
        Args:
            log_dir (str): Directory to store log files
        """
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(log_dir, f"bendsql_run_{timestamp}.log")
        self.log_file = open(log_file_path, 'w')
        self.log(f"Log started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(f"Database: {self.database}")
        return log_file_path

    def log(self, message):
        """
        Log a message to both console and log file.
        
        Args:
            message (str): Message to log
        """
        print(message)
        if self.log_file:
            self.log_file.write(f"{message}\n")
            self.log_file.flush()

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
            self.log(f">>>> {query}")
        
        command = ["bendsql", "--query=" + query, "--database=" + self.database, "--time=server"]
        result = subprocess.run(command, text=True, capture_output=True)
        
        if "APIError: ResponseError" in result.stderr:
            self.log(f"'APIError: ResponseError' found in bendsql output: {result.stderr}")
            self.log("<<<<")
            return None
        elif result.returncode != 0:
            self.log(f"bendsql command failed with return code {result.returncode}: {result.stderr}")
            self.log("<<<<")
            return None
        
        if print_query:
            self.log(result.stdout)
            self.log("<<<<")
        
        return result.stdout

    def extract_time(self, output):
        """
        Extract execution time from the bendsql output.
        
        Args:
            output (str): Command output
            
        Returns:
            float: Execution time in seconds, or None if not found
        """
        if not output:
            return None
            
        match = re.search(r"([0-9.]+)$", output)
        if not match:
            self.log("Could not extract time from output.")
            return None
        return float(match.group(1))

    def execute_sql_file(self, file_path):
        """
        Execute SQL queries from a file.
        
        Args:
            file_path (str): Path to the SQL file
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.log(f"Executing SQL file: {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                sql_script = f.read()
                
            # Split the script into individual queries
            queries = [query.strip() for query in sql_script.split(';') if query.strip()]
            
            # Execute each query
            for query in queries:
                result = self.execute_bendsql(query)
                if result is None:
                    self.log(f"Failed to execute query: {query[:100]}...")
                    return False
            
            return True
        except Exception as e:
            self.log(f"Error executing SQL file: {e}")
            return False

    def setup_database(self):
        """
        Set up the database by executing setup.sql.
        
        Returns:
            bool: True if successful, False otherwise
        """
        self.log("Setting up database...")
        
        setup_file = "setup.sql"
        if not os.path.exists(setup_file):
            self.log(f"Error: {setup_file} not found")
            return False
        
        success = self.execute_sql_file(setup_file)
        if success:
            self.log("Database setup completed successfully")
        else:
            self.log("Database setup failed")
        
        return success

    def analyze_tables(self, analyze_method="none"):
        """
        Analyze tables to generate statistics.
        
        Args:
            analyze_method (str): Method to use for analyzing tables:
                - "none": No analyze (raw)
                - "standard": Standard analyze
                - "histogram": Analyze with accurate histograms
            
        Returns:
            dict: Dictionary containing histogram data for each table, or False if failed
        """
        if analyze_method == "none":
            self.log("Skipping table analysis (using raw tables)...")
            return {}
        
        self.log(f"Analyzing tables using method: {analyze_method}")
        
        # List of tables to analyze
        tables = [
            "aka_name", "aka_title", "cast_info", "char_name", "comp_cast_type",
            "company_name", "company_type", "complete_cast", "info_type", "keyword",
            "kind_type", "link_type", "movie_companies", "movie_info", "movie_info_idx",
            "movie_keyword", "movie_link", "name", "person_info", "role_type", "title"
        ]
        
        
        # Analyze each table
        for table in tables:
            self.log(f"Analyzing table {table} using method: {analyze_method}")
            
            # Analyze table based on the specified method
            if analyze_method == "histogram":
                analyze_query = f"set enable_analyze_histogram = 1; analyze table imdb.{table}"
                self.log(f"Using accurate histograms for table {table}")
            elif analyze_method == "standard":
                analyze_query = f"analyze table imdb.{table}"
                self.log(f"Using standard analyze for table {table}")
            else:
                self.log(f"Invalid analyze method: {analyze_method}")
                return False
            
            result = self.execute_bendsql(analyze_query)
            if result is None:
                return False
        
        self.log(f"Table analysis completed using method: {analyze_method}")
        return True

    def run_queries(self, analyze_method="none", collect_times=False):
        """
        Run SQL queries from the queries directory.
        
        Args:
            analyze_method (str): Method used for analyzing tables (for logging)
            collect_times (bool): Whether to collect and return execution times
            
        Returns:
            dict: Dictionary of query execution times if collect_times is True, otherwise True/False for success
        """
        self.log(f"Starting query execution with analyze method: {analyze_method}")
        
        dir_path = "queries/"
        if not os.path.exists(dir_path):
            self.log(f"Error: {dir_path} directory not found")
            return False if not collect_times else {}
        
        query_times = {}
        
        # Read the SQL files in `queries` directory
        for file in sorted(os.listdir(dir_path)):
            if file.endswith(".sql"):
                self.log(f"\nExecuting queries from file: {file}")
                with open(Path(dir_path + file), "r") as f:
                    content = f.read()
                    queries = [
                        query.strip() for query in content.split(";") if query.strip()
                    ]
                    # Execute the SQL
                    for i, query in enumerate(queries):
                        query_id = f"{file}_{i+1}"
                        self.log(f"Executing query {query_id} with analyze method {analyze_method}: {query[:100]}...")
                        output = self.execute_bendsql(query)
                        if output:
                            time_elapsed = self.extract_time(output)
                            if time_elapsed is not None:
                                self.log(f"Time elapsed for query {query_id} with analyze method {analyze_method}: {time_elapsed} seconds\n")
                                if collect_times:
                                    query_times[query_id] = {
                                        "query": query,
                                        "time": time_elapsed,
                                        "analyze_method": analyze_method
                                    }
        
        self.log(f"Query execution completed with analyze method: {analyze_method}")
        return query_times if collect_times else True

    def compare_analyze_methods(self):
        """
        Compare query performance with different analyze methods.
        
        This function:
        1. Sets up the database
        2. Runs queries with no analyze (raw)
        3. Analyzes tables with standard analyze
        4. Runs queries with standard analyze
        5. Analyzes tables with histograms
        6. Runs queries with histogram analyze
        7. Generates a comparison report
        
        Returns:
            bool: True if successful, False otherwise
        """
        # Set up logging
        log_file = self.setup_logging()
        self.log("Starting comparison of analyze methods...")
        self.log("This will compare query performance with three different analyze methods:")
        self.log("1. Raw (no analyze)")
        self.log("2. Standard analyze")
        self.log("3. Analyze with accurate histograms")
        
        # Step 1: Set up the database
        self.log("\n=== STEP 1: Setting up the database ===")
        if not self.setup_database():
            self.log("Database setup failed. Aborting comparison.")
            return False
        
        # Step 2: Run queries with no analyze (raw)
        self.log("\n=== STEP 2: Running queries with no analyze (raw) ===")
        raw_times = self.run_queries(analyze_method="none", collect_times=True)
        
        # Step 3: Analyze tables with standard analyze
        self.log("\n=== STEP 3: Analyzing tables with standard analyze ===")
        standard_histogram_data = self.analyze_tables(analyze_method="standard")
        if standard_histogram_data is False:
            self.log("Standard analyze failed. Aborting comparison.")
            return False
        
        # Step 4: Run queries with standard analyze
        self.log("\n=== STEP 4: Running queries with standard analyze ===")
        standard_times = self.run_queries(analyze_method="standard", collect_times=True)
        
        # Step 5: Analyze tables with histograms
        self.log("\n=== STEP 5: Analyzing tables with accurate histograms ===")
        histogram_data = self.analyze_tables(analyze_method="histogram")
        if histogram_data is False:
            self.log("Histogram analyze failed. Aborting comparison.")
            return False
        
        # Step 6: Run queries with histogram analyze
        self.log("\n=== STEP 6: Running queries with histogram analyze ===")
        histogram_times = self.run_queries(analyze_method="histogram", collect_times=True)
        
        # Step 7: Generate comparison report
        self.log("\n=== STEP 7: Generating comparison report ===")
        success = self.generate_comparison_report(raw_times, standard_times, histogram_times, 
                                                standard_histogram_data, histogram_data)
        
        if success:
            self.log("Comparison completed successfully.")
        else:
            self.log("Comparison report generation failed.")
        
        if self.log_file:
            self.log_file.close()
            self.log(f"Log file saved to: {log_file}")
        
        return success

    def generate_comparison_report(self, raw_times, standard_times, histogram_times, 
                                 standard_histogram_data, histogram_data):
        """
        Generate a comparison report between different analyze methods.
        
        Args:
            raw_times (dict): Dictionary of query execution times with no analyze
            standard_times (dict): Dictionary of query execution times with standard analyze
            histogram_times (dict): Dictionary of query execution times with histogram analyze
            standard_histogram_data (dict): Dictionary of histogram data with standard analyze
            histogram_data (dict): Dictionary of histogram data with histogram analyze
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.log("Generating comparison report...")
        
        # Create results directory if it doesn't exist
        results_dir = "results"
        os.makedirs(results_dir, exist_ok=True)
        
        # Generate timestamp for filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create comparison data
        comparison_data = []
        
        # Get all unique query IDs
        all_query_ids = sorted(set(list(raw_times.keys()) + 
                                  list(standard_times.keys()) + 
                                  list(histogram_times.keys())))
        
        for query_id in all_query_ids:
            raw_time = raw_times.get(query_id, {}).get("time", "N/A")
            std_time = standard_times.get(query_id, {}).get("time", "N/A")
            hist_time = histogram_times.get(query_id, {}).get("time", "N/A")
            
            # Calculate differences and determine fastest method
            fastest = "N/A"
            raw_vs_std = "N/A"
            raw_vs_hist = "N/A"
            std_vs_hist = "N/A"
            
            if isinstance(raw_time, (int, float)) and isinstance(std_time, (int, float)):
                raw_vs_std = std_time - raw_time
                raw_vs_std_percent = (raw_vs_std / raw_time) * 100 if raw_time > 0 else 0
            else:
                raw_vs_std_percent = "N/A"
                
            if isinstance(raw_time, (int, float)) and isinstance(hist_time, (int, float)):
                raw_vs_hist = hist_time - raw_time
                raw_vs_hist_percent = (raw_vs_hist / raw_time) * 100 if raw_time > 0 else 0
            else:
                raw_vs_hist_percent = "N/A"
                
            if isinstance(std_time, (int, float)) and isinstance(hist_time, (int, float)):
                std_vs_hist = hist_time - std_time
                std_vs_hist_percent = (std_vs_hist / std_time) * 100 if std_time > 0 else 0
            else:
                std_vs_hist_percent = "N/A"
            
            # Determine fastest method
            times = []
            if isinstance(raw_time, (int, float)):
                times.append(("Raw", raw_time))
            if isinstance(std_time, (int, float)):
                times.append(("Standard", std_time))
            if isinstance(hist_time, (int, float)):
                times.append(("Histogram", hist_time))
                
            if times:
                fastest = min(times, key=lambda x: x[1])[0]
            
            query_text = (raw_times.get(query_id, {}).get("query", "") or 
                         standard_times.get(query_id, {}).get("query", "") or 
                         histogram_times.get(query_id, {}).get("query", ""))
            
            comparison_data.append({
                "query_id": query_id,
                "query": query_text[:100] + "..." if len(query_text) > 100 else query_text,
                "raw_time": raw_time,
                "standard_time": std_time,
                "histogram_time": hist_time,
                "raw_vs_std": raw_vs_std,
                "raw_vs_std_percent": raw_vs_std_percent,
                "raw_vs_hist": raw_vs_hist,
                "raw_vs_hist_percent": raw_vs_hist_percent,
                "std_vs_hist": std_vs_hist,
                "std_vs_hist_percent": std_vs_hist_percent,
                "fastest": fastest
            })
        
        # Save raw data as JSON
        json_file = os.path.join(results_dir, f"analyze_comparison_{timestamp}.json")
        with open(json_file, 'w') as f:
            json.dump({
                "raw_times": raw_times,
                "standard_times": standard_times,
                "histogram_times": histogram_times,
                "standard_histogram_data": standard_histogram_data,
                "histogram_data": histogram_data,
                "comparison": comparison_data
            }, f, indent=2)
        
        # Generate HTML report
        html_file = os.path.join(results_dir, f"analyze_comparison_{timestamp}.html")
        self.generate_html_report(comparison_data, standard_histogram_data, histogram_data, html_file)
        
        # Generate text report
        text_file = os.path.join(results_dir, f"analyze_comparison_{timestamp}.txt")
        self.generate_text_report(comparison_data, standard_histogram_data, histogram_data, text_file)
        
        self.log(f"Comparison reports generated:")
        self.log(f"  - HTML: {html_file}")
        self.log(f"  - Text: {text_file}")
        self.log(f"  - JSON: {json_file}")
        
        return True

    def generate_html_report(self, comparison_data, standard_histogram_data, histogram_data, output_file):
        """
        Generate an HTML report from comparison data.
        
        Args:
            comparison_data (list): List of dictionaries containing comparison data
            standard_histogram_data (dict): Dictionary of histogram data with standard analyze
            histogram_data (dict): Dictionary of histogram data with histogram analyze
            output_file (str): Path to output HTML file
        """
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Analyze Method Comparison</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                h1, h2, h3 { color: #333; }
                table { border-collapse: collapse; width: 100%; margin-top: 20px; margin-bottom: 40px; }
                th, td { padding: 8px; text-align: left; border: 1px solid #ddd; }
                th { background-color: #f2f2f2; }
                tr:nth-child(even) { background-color: #f9f9f9; }
                .faster { font-weight: bold; }
                .raw-faster { color: blue; }
                .standard-faster { color: green; }
                .histogram-faster { color: purple; }
                .summary { margin-top: 20px; padding: 10px; background-color: #f0f0f0; border-radius: 5px; }
                .histogram-section { margin-top: 40px; }
                .histogram-table { font-size: 0.9em; }
                .positive { color: red; }
                .negative { color: green; }
                .tabs { display: flex; margin-bottom: 10px; }
                .tab { padding: 10px 20px; cursor: pointer; border: 1px solid #ccc; background-color: #f1f1f1; }
                .tab.active { background-color: #ddd; }
                .tab-content { display: none; }
                .tab-content.active { display: block; }
            </style>
            <script>
                function openTab(evt, tabName) {
                    var i, tabcontent, tablinks;
                    tabcontent = document.getElementsByClassName("tab-content");
                    for (i = 0; i < tabcontent.length; i++) {
                        tabcontent[i].style.display = "none";
                    }
                    tablinks = document.getElementsByClassName("tab");
                    for (i = 0; i < tablinks.length; i++) {
                        tablinks[i].className = tablinks[i].className.replace(" active", "");
                    }
                    document.getElementById(tabName).style.display = "block";
                    evt.currentTarget.className += " active";
                }
            </script>
        </head>
        <body>
            <h1>Analyze Method Comparison</h1>
            <p>Comparison of query execution times between raw (no analyze), standard analyze, and analyze with accurate histograms.</p>
            
            <div class="summary">
                <h2>Summary</h2>
        """
        
        # Calculate summary statistics
        total_queries = len(comparison_data)
        raw_faster = sum(1 for item in comparison_data if item["fastest"] == "Raw")
        standard_faster = sum(1 for item in comparison_data if item["fastest"] == "Standard")
        histogram_faster = sum(1 for item in comparison_data if item["fastest"] == "Histogram")
        na_count = sum(1 for item in comparison_data if item["fastest"] == "N/A")
        
        # Add summary to HTML
        html += f"""
                <p>Total Queries: {total_queries}</p>
                <p>Raw Faster: {raw_faster} ({raw_faster/total_queries*100:.1f}%)</p>
                <p>Standard Analyze Faster: {standard_faster} ({standard_faster/total_queries*100:.1f}%)</p>
                <p>Histogram Analyze Faster: {histogram_faster} ({histogram_faster/total_queries*100:.1f}%)</p>
                <p>N/A: {na_count} ({na_count/total_queries*100:.1f}%)</p>
            </div>
            
            <div class="tabs">
                <button class="tab active" onclick="openTab(event, 'QueryComparison')">Query Comparison</button>
                <button class="tab" onclick="openTab(event, 'HistogramData')">Histogram Data</button>
            </div>
            
            <div id="QueryComparison" class="tab-content active">
                <h2>Query Execution Time Comparison</h2>
                <table>
                    <tr>
                        <th>Query ID</th>
                        <th>Raw Time (s)</th>
                        <th>Standard Time (s)</th>
                        <th>Histogram Time (s)</th>
                        <th>Std vs Raw (%)</th>
                        <th>Hist vs Raw (%)</th>
                        <th>Hist vs Std (%)</th>
                        <th>Fastest Method</th>
                    </tr>
        """
        
        # Add rows for each query
        for item in comparison_data:
            faster_class = ""
            if item["fastest"] == "Raw":
                faster_class = "raw-faster"
            elif item["fastest"] == "Standard":
                faster_class = "standard-faster"
            elif item["fastest"] == "Histogram":
                faster_class = "histogram-faster"
            
            raw_time_str = f"{item['raw_time']:.3f}" if isinstance(item['raw_time'], (int, float)) else item['raw_time']
            std_time_str = f"{item['standard_time']:.3f}" if isinstance(item['standard_time'], (int, float)) else item['standard_time']
            hist_time_str = f"{item['histogram_time']:.3f}" if isinstance(item['histogram_time'], (int, float)) else item['histogram_time']
            
            # Format percentages with colors
            raw_vs_std_class = "positive" if isinstance(item['raw_vs_std_percent'], (int, float)) and item['raw_vs_std_percent'] > 0 else "negative"
            raw_vs_hist_class = "positive" if isinstance(item['raw_vs_hist_percent'], (int, float)) and item['raw_vs_hist_percent'] > 0 else "negative"
            std_vs_hist_class = "positive" if isinstance(item['std_vs_hist_percent'], (int, float)) and item['std_vs_hist_percent'] > 0 else "negative"
            
            raw_vs_std_str = f"{item['raw_vs_std_percent']:.1f}%" if isinstance(item['raw_vs_std_percent'], (int, float)) else item['raw_vs_std_percent']
            raw_vs_hist_str = f"{item['raw_vs_hist_percent']:.1f}%" if isinstance(item['raw_vs_hist_percent'], (int, float)) else item['raw_vs_hist_percent']
            std_vs_hist_str = f"{item['std_vs_hist_percent']:.1f}%" if isinstance(item['std_vs_hist_percent'], (int, float)) else item['std_vs_hist_percent']
            
            html += f"""
                <tr>
                    <td title="{item['query']}">{item['query_id']}</td>
                    <td>{raw_time_str}</td>
                    <td>{std_time_str}</td>
                    <td>{hist_time_str}</td>
                    <td class="{raw_vs_std_class}">{raw_vs_std_str}</td>
                    <td class="{raw_vs_hist_class}">{raw_vs_hist_str}</td>
                    <td class="{std_vs_hist_class}">{std_vs_hist_str}</td>
                    <td class="faster {faster_class}">{item['fastest']}</td>
                </tr>
            """
        
        html += """
                </table>
            </div>
            
            <div id="HistogramData" class="tab-content">
                <h2>Histogram Data Comparison</h2>
                <p>This section compares the histogram data between standard analyze and analyze with accurate histograms.</p>
        """
        
        # Add histogram data comparison
        for table in sorted(set(list(standard_histogram_data.keys()) + list(histogram_data.keys()))):
            html += f"""
                <div class="histogram-section">
                    <h3>Table: {table}</h3>
                    <div class="tabs">
                        <button class="tab active" onclick="openTab(event, '{table}_standard')">Standard Analyze</button>
                        <button class="tab" onclick="openTab(event, '{table}_histogram')">Histogram Analyze</button>
                    </div>
                    
                    <div id="{table}_standard" class="tab-content active">
                        <h4>Standard Analyze Histogram</h4>
                        <pre class="histogram-table">{standard_histogram_data.get(table, 'No histogram data available')}</pre>
                    </div>
                    
                    <div id="{table}_histogram" class="tab-content">
                        <h4>Accurate Histogram</h4>
                        <pre class="histogram-table">{histogram_data.get(table, 'No histogram data available')}</pre>
                    </div>
                </div>
            """
        
        html += """
            </div>
        </body>
        </html>
        """
        
        with open(output_file, 'w') as f:
            f.write(html)

    def generate_text_report(self, comparison_data, standard_histogram_data, histogram_data, output_file):
        """
        Generate a text report from comparison data.
        
        Args:
            comparison_data (list): List of dictionaries containing comparison data
            standard_histogram_data (dict): Dictionary of histogram data with standard analyze
            histogram_data (dict): Dictionary of histogram data with histogram analyze
            output_file (str): Path to output text file
        """
        # Calculate summary statistics
        total_queries = len(comparison_data)
        raw_faster = sum(1 for item in comparison_data if item["fastest"] == "Raw")
        standard_faster = sum(1 for item in comparison_data if item["fastest"] == "Standard")
        histogram_faster = sum(1 for item in comparison_data if item["fastest"] == "Histogram")
        na_count = sum(1 for item in comparison_data if item["fastest"] == "N/A")
        
        with open(output_file, 'w') as f:
            f.write("=== Analyze Method Comparison ===\n\n")
            f.write("Comparison of query execution times between raw (no analyze), standard analyze, and analyze with accurate histograms.\n\n")
            
            f.write("=== Summary ===\n")
            f.write(f"Total Queries: {total_queries}\n")
            f.write(f"Raw Faster: {raw_faster} ({raw_faster/total_queries*100:.1f}%)\n")
            f.write(f"Standard Analyze Faster: {standard_faster} ({standard_faster/total_queries*100:.1f}%)\n")
            f.write(f"Histogram Analyze Faster: {histogram_faster} ({histogram_faster/total_queries*100:.1f}%)\n")
            f.write(f"N/A: {na_count} ({na_count/total_queries*100:.1f}%)\n\n")
            
            f.write("=== Query Execution Time Comparison ===\n\n")
            
            # Format header
            f.write(f"{'Query ID':<15} {'Raw (s)':<12} {'Standard (s)':<12} {'Histogram (s)':<12} {'Std vs Raw':<12} {'Hist vs Raw':<12} {'Hist vs Std':<12} {'Fastest':<10}\n")
            f.write("-" * 95 + "\n")
            
            # Add rows for each query
            for item in comparison_data:
                raw_time = f"{item['raw_time']:.3f}" if isinstance(item['raw_time'], (int, float)) else item['raw_time']
                std_time = f"{item['standard_time']:.3f}" if isinstance(item['standard_time'], (int, float)) else item['standard_time']
                hist_time = f"{item['histogram_time']:.3f}" if isinstance(item['histogram_time'], (int, float)) else item['histogram_time']
                
                raw_vs_std = f"{item['raw_vs_std_percent']:.1f}%" if isinstance(item['raw_vs_std_percent'], (int, float)) else item['raw_vs_std_percent']
                raw_vs_hist = f"{item['raw_vs_hist_percent']:.1f}%" if isinstance(item['raw_vs_hist_percent'], (int, float)) else item['raw_vs_hist_percent']
                std_vs_hist = f"{item['std_vs_hist_percent']:.1f}%" if isinstance(item['std_vs_hist_percent'], (int, float)) else item['std_vs_hist_percent']
                
                f.write(f"{item['query_id']:<15} {raw_time:<12} {std_time:<12} {hist_time:<12} {raw_vs_std:<12} {raw_vs_hist:<12} {std_vs_hist:<12} {item['fastest']:<10}\n")
                
                # Add query text on the next line, indented
                f.write(f"  Query: {item['query']}\n\n")
            
            f.write("\n=== Histogram Data Comparison ===\n\n")
            
            # Add histogram data comparison
            for table in sorted(set(list(standard_histogram_data.keys()) + list(histogram_data.keys()))):
                f.write(f"Table: {table}\n")
                f.write("=" * 80 + "\n\n")
                
                f.write("Standard Analyze Histogram:\n")
                f.write("-" * 80 + "\n")
                f.write(f"{standard_histogram_data.get(table, 'No histogram data available')}\n\n")
                
                f.write("Accurate Histogram:\n")
                f.write("-" * 80 + "\n")
                f.write(f"{histogram_data.get(table, 'No histogram data available')}\n\n")
                
                f.write("\n")


def main():
    """Main function to parse arguments and execute commands."""
    parser = argparse.ArgumentParser(
        description="BendSQL Job Runner - Set up database and execute queries"
    )
    parser.add_argument("--setup", action="store_true", help="Execute setup.sql to create schema and load data")
    parser.add_argument("--run", action="store_true", help="Run SQL queries from the queries directory")
    parser.add_argument("--analyze", action="store_true", help="Analyze tables after setup")
    parser.add_argument("--accurate-histograms", action="store_true", help="Use accurate histograms when analyzing tables")
    parser.add_argument("--compare", action="store_true", help="Compare query performance with different analyze methods")
    
    args = parser.parse_args()
    
    # Create BendSQL runner
    runner = BendSQLRunner()
    
    # Execute commands based on arguments
    if args.compare:
        # Compare analyze methods (this includes setup, analyze, and run)
        success = runner.compare_analyze_methods()
        if not success:
            sys.exit(1)
    else:
        # Execute individual commands
        if args.setup:
            success = runner.setup_database()
            if not success:
                sys.exit(1)
        
        if args.analyze:
            analyze_method = "histogram" if args.accurate_histograms else "standard"
            success = runner.analyze_tables(analyze_method=analyze_method)
            if success is False:  # Check for False specifically, as {} is a valid return
                sys.exit(1)
        
        if args.run:
            success = runner.run_queries()
            if not success:
                sys.exit(1)
    
    # If no arguments provided, show help
    if not (args.setup or args.run or args.analyze or args.compare):
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()