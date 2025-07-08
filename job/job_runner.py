#!/usr/bin/env python3

import os
import re
import sys
import time
import json
import argparse
import subprocess
import threading
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from termcolor import colored
from jinja2 import Template

# Global database context management
class DatabaseContext:
    def __init__(self):
        self.current_database = None
        self.lock = threading.Lock()
    
    def set_database(self, database_name):
        with self.lock:
            self.current_database = database_name
            print(f"    📍 Global database context updated to: {database_name}")
    
    def get_database(self):
        with self.lock:
            return self.current_database

# Global instance
db_context = DatabaseContext()

@dataclass
class QueryResult:
    """Query execution result"""
    name: str
    sql: str
    success: bool
    duration: float
    error: str = ""
    output: str = ""

@dataclass
class StageResult:
    """Stage execution result"""
    name: str
    queries: List[QueryResult]
    duration: float
    success: bool

@dataclass
class TestResult:
    """Complete test result"""
    name: str
    stages: List[StageResult]
    success: bool
    total_duration: float

@dataclass
class ComparisonQueryResult:
    """Query result for comparison analysis"""
    name: str
    sql_preview: str
    raw_time: float
    standard_time: float
    histogram_time: float
    std_vs_raw: float
    hist_vs_raw: float
    hist_vs_std: float
    fastest_method: str
    raw_explain: str = ""
    standard_explain: str = ""
    histogram_explain: str = ""

@dataclass
class ComparisonData:
    """Complete comparison analysis data"""
    total_queries: int
    raw_faster: int
    standard_faster: int
    histogram_faster: int
    raw_faster_pct: float
    standard_faster_pct: float
    histogram_faster_pct: float
    queries: List[ComparisonQueryResult]
    top_improvements: List[Dict]
    regressions: List[Dict]

class BendSQLExecutor:
    """BendSQL command executor"""
    
    def __init__(self, job_runner=None):
        self.job_runner = job_runner
        
    def _extract_time_from_output(self, output):
        """Extract execution time from the bendsql output."""
        match = re.search(r"([0-9.]+)$", output)
        return match.group(1) if match else None
    
    def execute_query(self, query: str, timeout: int = 300, use_server_time: bool = True) -> Tuple[bool, str, float]:
        """Execute a single SQL query"""
        start_time = time.time()
        
        try:
            # Build complete SQL with database context handling
            bendsql_cmd, final_sql = self._build_bendsql_command(query, use_server_time)
            
            # Log execution details
            current_db = db_context.get_database()
            db_info = f" [DB: {current_db}]" if current_db else ""
            print(f"    Executing{db_info}: {final_sql[:100]}{'...' if len(final_sql) > 100 else ''}")
            
            env = os.environ.copy()
            
            result = subprocess.run(
                bendsql_cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env
            )
            
            duration = time.time() - start_time
            success = result.returncode == 0
            output = result.stdout if success else result.stderr
            
            # If using server time and successful, extract the time from output
            if use_server_time and success:
                server_time = self._extract_time_from_output(output)
                if server_time is not None:
                    duration = float(server_time)
            
            status = "✅ Success" if success else "❌ Failed"
            print(f"    {status} in {duration:.2f}s")
            
            # Print total elapsed time if job_runner is available
            if self.job_runner:
                total_elapsed = self.job_runner.get_elapsed_time()
                print(f"    Total elapsed time: {total_elapsed}")
            
            # Print error details if failed
            if not success and output:
                print(f"    Error: {output}")
            
            return success, output, duration
            
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            print(f"    ⏰ Timeout after {timeout}s")
            return False, f"Query timeout after {timeout} seconds", duration
        except Exception as e:
            duration = time.time() - start_time
            print(f"    💥 Exception in {duration:.2f}s: {str(e)}")
            return False, str(e), duration
    
    def _build_bendsql_command(self, sql: str, use_server_time: bool = True) -> Tuple[List[str], str]:
        """Build bendsql command with database context handling"""
        # Extract database name from USE statement
        database_name, cleaned_sql = self._extract_database_from_use_statement(sql)
        
        # Update global database context if USE statement found
        if database_name:
            db_context.set_database(database_name)
        
        # Get current database context
        current_db = database_name or db_context.get_database()
        
        # Build command
        bendsql_cmd = ['bendsql']
        if current_db:
            bendsql_cmd.extend(['-D', current_db])
        
        # Use cleaned SQL if USE statement was found, otherwise use original
        final_sql = cleaned_sql if database_name else sql
        bendsql_cmd.append('--query=' + final_sql)
        
        # Add --time=server flag for more accurate timing
        if use_server_time:
            bendsql_cmd.append('--time=server')
        
        return bendsql_cmd, final_sql
    
    def _extract_database_from_use_statement(self, sql: str) -> Tuple[Optional[str], str]:
        """
        Extract the last database name from USE statements and return SQL with all USE statements removed.
        """
        # Regex to find all USE statements, case-insensitive
        use_pattern = re.compile(r'USE\s+([`\'"]?[\w_]+[`\'"]?)\s*;?', re.IGNORECASE)
        
        database_name = None
        matches = list(use_pattern.finditer(sql))
        
        if matches:
            # Get the last matched database name
            last_match = matches[-1]
            db_name = last_match.group(1)
            
            # Remove quotes
            if db_name.startswith('`') and db_name.endswith('`'):
                database_name = db_name[1:-1]
            elif db_name.startswith('"') and db_name.endswith('"'):
                database_name = db_name[1:-1]
            elif db_name.startswith("'") and db_name.endswith("'"):
                database_name = db_name[1:-1]
            else:
                database_name = db_name

        # Remove all USE statements from the SQL
        cleaned_sql = use_pattern.sub('', sql).strip()
        
        return database_name, cleaned_sql
    
    def execute_sql_file(self, file_path: Path) -> List[QueryResult]:
        """Execute SQL queries from a file"""
        results = []
        
        try:
            with open(file_path, 'r') as f:
                sql_script = f.read()
            
            # Split into individual queries
            queries = [q.strip() for q in sql_script.split(';') if q.strip()]
            
            for i, query in enumerate(queries):
                query_name = f"{file_path.stem}_{i+1}"
                success, output, duration = self.execute_query(query)
                
                results.append(QueryResult(
                    name=query_name,
                    sql=query,
                    success=success,
                    duration=duration,
                    error="" if success else output,
                    output=output if success else ""
                ))
                
        except Exception as e:
            results.append(QueryResult(
                name=file_path.stem,
                sql="",
                success=False,
                duration=0.0,
                error=str(e)
            ))
        
        return results
        
    def execute_explain(self, sql: str) -> str:
        """Execute EXPLAIN on SQL, but only for SELECT statements"""
        try:
            # Extract the SQL statement after USE statement if present
            parts = sql.split(';')
            use_statement = ""
            select_statement = ""
            current_db = "imdb"
            
            # Find USE statement and SELECT statement
            for part in parts:
                part = part.strip()
                if part and part.lower().startswith('use '):
                    use_statement = part + ";"
                    # Extract database name for normalization later
                    db_name = part.lower().replace('use ', '').strip()
                    if db_name in ["imdb_raw", "imdb_std", "imdb_hist"]:
                        current_db = db_name
                elif part and part.lower().startswith('select'):
                    select_statement = part
            
            # If no SELECT statement found, return empty string
            if not select_statement:
                return ""
            
            # Build EXPLAIN query, preserving USE statement for correct database context
            if use_statement:
                explain_sql = f"{use_statement} EXPLAIN {select_statement}"
            else:
                explain_sql = f"EXPLAIN {select_statement}"
                
            # Execute EXPLAIN query without using server time flag
            # as --time=server with EXPLAIN doesn't return the actual plan
            success, output, _ = self.execute_query(explain_sql, use_server_time=False)
            if success:
                # Normalize database names in the output to imdb_[x] format
                normalized_output = output.strip()
                # Replace actual database names with imdb_[x]
                normalized_output = normalized_output.replace("imdb_raw", "imdb_[x]")
                normalized_output = normalized_output.replace("imdb_std", "imdb_[x]")
                normalized_output = normalized_output.replace("imdb_hist", "imdb_[x]")
                return normalized_output
            else:
                return f"Error executing EXPLAIN: {output}"
        except Exception as e:
            return f"Exception during EXPLAIN: {str(e)}"

class SQLFileParser:
    """SQL file parser for query discovery"""
    
    @staticmethod
    def find_sql_files(directory: Path, pattern: str = "*.sql") -> List[Path]:
        """Find SQL files in directory"""
        if not directory.exists():
            return []
        return sorted(directory.glob(pattern))
    
    @staticmethod
    def get_sql_files(directory: str, pattern: str = "*.sql") -> List[Path]:
        """Get SQL files from directory path"""
        dir_path = Path(directory)
        if not dir_path.exists():
            return []
        return sorted(dir_path.glob(pattern))
    
    @staticmethod
    def extract_time_from_output(output: str) -> Optional[float]:
        """Extract execution time from bendsql output"""
        if not output:
            return None
        match = re.search(r"([0-9.]+)$", output)
        return float(match.group(1)) if match else None

class ReportGenerator:
    """HTML report generator using templates"""
    
    def __init__(self, template_dir: Path = None):
        self.template_dir = template_dir or Path(__file__).parent / "templates"
    
    def generate_html_report(self, results: List[TestResult], output_path: Path):
        """Generate HTML report from test results with dynamic updates"""
        # Always reload template for dynamic updates
        template_content = self._load_template("report.html")
        template = Template(template_content)
        
        # Calculate summary statistics
        total_tests = len(results)
        passed_tests = sum(1 for r in results if r.success)
        failed_tests = total_tests - passed_tests
        total_duration = sum(r.total_duration for r in results)
        
        # Calculate additional metrics
        total_queries = sum(len(stage.queries) for result in results for stage in result.stages)
        passed_queries = sum(1 for result in results for stage in result.stages for query in stage.queries if query.success)
        failed_queries = total_queries - passed_queries
        
        # Render template with comprehensive data
        html_content = template.render(
            results=results,
            total_tests=total_tests,
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            total_duration=total_duration,
            total_queries=total_queries,
            passed_queries=passed_queries,
            failed_queries=failed_queries,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            generation_time=datetime.now().isoformat(),
            success_rate=round((passed_tests / total_tests * 100) if total_tests > 0 else 0, 1),
            query_success_rate=round((passed_queries / total_queries * 100) if total_queries > 0 else 0, 1)
        )
        
        # Write to file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            f.write(html_content)
    
    def _load_template(self, template_name: str) -> str:
        """Load template from file"""
        template_path = self.template_dir / template_name
        with open(template_path, 'r') as f:
            return f.read()

class JobRunner:
    """Main job runner class"""
    
    def __init__(self, args=None):
        self.start_time = time.time()
        self.executor = BendSQLExecutor(job_runner=self)
        self.parser = SQLFileParser()
        self.reporter = ReportGenerator()
        self.results: List[TestResult] = []
        self.args = args
        # Default number of check runs for comparison analysis
        self.check_runs = getattr(self.args, 'check_runs', 3)
        self.log(f"Job started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
    def get_elapsed_time(self) -> str:
        """Get formatted elapsed time since job started"""
        elapsed = time.time() - self.start_time
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    
    def log(self, message: str, level: str = "info"):
        """Log message with color coding"""
        colors = {
            "info": "cyan",
            "success": "green", 
            "error": "red",
            "warning": "yellow"
        }
        color = colors.get(level, "white")
        print(colored(f"[{level.upper()}] {message}", color))
    
    def setup_all_databases(self) -> bool:
        """Set up all three databases sequentially, with parallel execution within each SQL file"""
        self.log("Setting up all three databases: imdb_raw, imdb_std, imdb_hist", "info")
        start_time = time.time()
        
        # Define setup directories for each database type
        db_setups = [
            {"name": "imdb_raw", "dir": Path("sql/setup/raw")},
            {"name": "imdb_std", "dir": Path("sql/setup/std")},
            {"name": "imdb_hist", "dir": Path("sql/setup/hist")}
        ]
        
        # Execute setup for each database sequentially
        self.log("Setting up databases sequentially...", "info")
        
        for db_config in db_setups:
            db_name = db_config["name"]
            setup_dir = db_config["dir"]
            
            self.log(f"Setting up {db_name}...", "info")
            
            # Find all SQL files in the setup directory
            if not setup_dir.exists():
                self.log(f"Setup directory not found: {setup_dir}", "error")
                return False
                
            # Get all SQL files and sort them by name to ensure correct execution order
            # This will execute files in order: 01_setup.sql, 02_table.sql, 03_copy.sql, 04_analyze.sql
            setup_files = sorted(list(setup_dir.glob("*.sql")))
            if not setup_files:
                self.log(f"No SQL files found in {setup_dir}", "warning")
                continue
                
            self.log(f"Found {len(setup_files)} SQL files for {db_name}: {[f.name for f in setup_files]}", "info")
                
            # Execute each setup file in order, with parallel execution within each file
            for setup_file in setup_files:
                self.log(f"Executing {db_name}/{setup_file.name}...", "info")
                # Use parallel execution for statements within the file
                file_results = self._execute_sql_file_parallel(setup_file)
                
                # Check if this file's execution was successful
                if not all(r.success for r in file_results):
                    self.log(f"Setup failed for {db_name} during {setup_file.name}", "error")
                    return False
            
            # All files for this database executed successfully
            self.log(f"Setup completed for {db_name}", "success")
        
        duration = time.time() - start_time
        self.log(f"All three databases set up successfully in {duration:.2f}s", "success")
        return True
        
    def _finalize_setup_results(self, query_results, duration, success):
        """Create and store test results for database setup"""
        stage = StageResult(
            name="setup",
            queries=query_results,
            duration=duration,
            success=success
        )
        
        test_result = TestResult(
            name="database_setup",
            stages=[stage],
            success=success,
            total_duration=duration
        )
        
        self.results.append(test_result)
        
        if success:
            self.log(f"Database setup completed in {duration:.2f}s", "success")
        else:
            self.log(f"Database setup failed in {duration:.2f}s", "error")
        
        return success
    
    def run_queries(self, queries_dir: str = "sql/check", database: str = None) -> bool:
        """Run queries from directory, optionally specifying a database context"""
        queries_path = Path(queries_dir)
        if not queries_path.exists():
            self.log(f"Queries directory not found: {queries_path}", "error")
            return False
            
        self.log(f"Running queries from {queries_dir}{f' on database {database}' if database else ''}", "info")
        start_time = time.time()
        
        # Find all SQL files
        sql_files = self.parser.find_sql_files(queries_path)
        if not sql_files:
            self.log(f"No SQL files found in {queries_path}", "warning")
            return True
            
        # Apply limit if specified
        limit = getattr(self.args, 'limit', None)
        if limit and limit < len(sql_files):
            sql_files = sql_files[:limit]
            self.log(f"Limiting to first {limit} queries out of {len(sql_files)} total", "info")
            
        # Execute each SQL file
        query_results = []
        for sql_file in sql_files:
            self.log(f"Executing {sql_file.name}...", "info")
            
            # If database is specified, prepend USE statement to ensure correct database context
            if database:
                # Read the SQL content
                with open(sql_file, 'r') as f:
                    sql_content = f.read()
                # Prepend USE statement
                sql_with_db = f"USE {database}; {sql_content}"
                # Execute the query with database context
                success, output, duration = self.executor.execute_query(sql_with_db)
                
                query_result = QueryResult(
                    name=sql_file.name,
                    sql=sql_with_db,
                    success=success,
                    duration=duration,
                    error="" if success else output,
                    output=output if success else ""
                )
                query_results.append(query_result)
                
                if not success:
                    self.log(f"Query execution failed for {sql_file.name}: {output}", "error")
            else:
                # Execute the SQL file normally
                file_results = self.executor.execute_sql_file(sql_file)
                query_results.extend(file_results)
                
                # Check if this file's execution was successful
                if not all(r.success for r in file_results):
                    self.log(f"Query execution failed for {sql_file.name}", "error")
                
        # Calculate overall success and duration
        duration = time.time() - start_time
        success = all(r.success for r in query_results)
        
        # Create and store test results
        test_result = TestResult(
            name=f"check_queries{f'_{database}' if database else ''}",
            stages=[StageResult(
                name=f"check_queries{f'_{database}' if database else ''}",
                queries=query_results,
                duration=duration,
                success=success
            )],
            success=success,
            total_duration=duration
        )
        
        self.results.append(test_result)
        
        if success:
            self.log(f"All queries executed successfully in {duration:.2f}s", "success")
        else:
            self.log(f"Some queries failed in {duration:.2f}s", "error")
            
        return success
        
    def run_comparison_analysis(self) -> bool:
        """Run comparison analysis between different database configurations"""
        self.log("Starting comparison analysis...", "info")
        start_time = time.time()
        
        # Check if setup is required
        setup_required = getattr(self.args, 'setup', False)
        if setup_required:
            self.log("Setup flag detected, setting up all three databases...", "info")
            if not self.setup_all_databases():
                self.log("Failed to set up databases", "error")
                return False
        else:
            self.log("Using existing databases for comparison", "info")
        
        # Find SQL files
        sql_dir = Path("sql/check")
        all_sql_files = self.parser.find_sql_files(sql_dir)
        
        if not all_sql_files:
            self.log("No SQL files found in sql/check directory", "error")
            return False
            
        # Apply limit if specified
        limit = getattr(self.args, 'limit', None)
        if limit and limit > 0 and limit < len(all_sql_files):
            sql_files = all_sql_files[:limit]
            self.log(f"Limiting to first {limit} queries out of {len(all_sql_files)} total", "info")
        else:
            sql_files = all_sql_files
        
        # Store results for each database
        raw_results = {}
        standard_results = {}
        histogram_results = {}
        raw_explains = {}
        standard_explains = {}
        histogram_explains = {}
        
        # Define database configurations
        databases = [
            {"name": "imdb_raw", "results": raw_results, "explains": raw_explains},
            {"name": "imdb_std", "results": standard_results, "explains": standard_explains},
            {"name": "imdb_hist", "results": histogram_results, "explains": histogram_explains}
        ]
        
        # Function to run a single SQL file on a specific database
        def run_sql_on_database(sql_file, database_config):
            database_name = database_config["name"]
            results_dict = database_config["results"]
            explains_dict = database_config["explains"]
            
            self.log(f"Executing {sql_file.name} on {database_name}...", "info")
            db_context.set_database(database_name)
            
            # Read the SQL content
            with open(sql_file, 'r') as f:
                sql_content = f.read()
            
            # Prepend USE statement to ensure correct database context
            sql_with_db = f"USE {database_name}; {sql_content}"
            
            # Run the query multiple times and take the shortest duration
            num_runs = self.check_runs
            durations = []
            success = False
            output = ""
            
            for run in range(num_runs):
                self.log(f"Run {run+1}/{num_runs} for {sql_file.name} on {database_name}...", "info")
                run_success, run_output, run_duration = self.executor.execute_query(sql_with_db, use_server_time=True)
                
                if run_success:
                    durations.append(run_duration)
                    success = True
                    output = run_output
                else:
                    self.log(f"Run {run+1} failed for {sql_file.name} on {database_name}: {run_output}", "error")
                    output = run_output
            
            if success and durations:
                # Use the shortest duration from all successful runs
                min_duration = min(durations)
                self.log(f"Best time for {sql_file.name} on {database_name}: {min_duration:.2f}s (from {len(durations)} successful runs)", "success")
                results_dict[sql_file.name] = min_duration
                
                # Get explain plan
                self.log(f"Getting EXPLAIN plan for {sql_file.name} on {database_name}...", "info")
                explains_dict[sql_file.name] = self.executor.execute_explain(sql_with_db)
            else:
                self.log(f"All runs failed for {sql_file.name} on {database_name}: {output}", "error")
        
        # Execute each SQL file on all databases before moving to the next SQL file
        for sql_file in sql_files:
            self.log(f"Processing SQL file: {sql_file.name}", "info")
            for database_config in databases:
                run_sql_on_database(sql_file, database_config)
        
        # Generate comparison data
        comparison_data = self._generate_comparison_data(
            raw_results, standard_results, histogram_results, 
            raw_explains, standard_explains, histogram_explains, 
            sql_files
        )
        
        # Store comparison results
        total_duration = time.time() - start_time
        test_result = TestResult(
            name="comparison_analysis",
            stages=[],
            success=True,
            total_duration=total_duration
        )
        self.results.append(test_result)
        
        # Generate comparison report
        self._generate_comparison_report(comparison_data)
        
        self.log(f"Comparison analysis completed in {total_duration:.2f}s", "success")
        return True
    
    def _generate_comparison_data(self, raw_results: Dict, standard_results: Dict,
                                histogram_results: Dict, raw_explains: Dict, 
                                standard_explains: Dict, histogram_explains: Dict,
                                sql_files: List[Path]) -> ComparisonData:
        """Generate comparison data structure"""
        queries = []
        raw_faster = 0
        standard_faster = 0
        histogram_faster = 0
        
        for sql_file in sql_files:
            filename = sql_file.name
            if filename not in raw_results or filename not in standard_results or filename not in histogram_results:
                continue
            
            raw_time = raw_results[filename]
            standard_time = standard_results[filename]
            histogram_time = histogram_results[filename]
            
            # Calculate percentage differences
            std_vs_raw = ((standard_time - raw_time) / raw_time) * 100
            hist_vs_raw = ((histogram_time - raw_time) / raw_time) * 100
            hist_vs_std = ((histogram_time - standard_time) / standard_time) * 100
            
            # Determine fastest method
            times = {"Raw": raw_time, "Standard": standard_time, "Histogram": histogram_time}
            fastest_method = min(times, key=times.get)
            
            if fastest_method == "Raw":
                raw_faster += 1
            elif fastest_method == "Standard":
                standard_faster += 1
            else:
                histogram_faster += 1
            
            # Get SQL preview
            sql_preview = self._get_sql_preview(sql_file)
            
            # Get explain plans if available
            raw_explain = raw_explains.get(filename, "")
            standard_explain = standard_explains.get(filename, "")
            histogram_explain = histogram_explains.get(filename, "")
            
            query_result = ComparisonQueryResult(
                name=filename,
                sql_preview=sql_preview,
                raw_time=raw_time,
                standard_time=standard_time,
                histogram_time=histogram_time,
                std_vs_raw=std_vs_raw,
                hist_vs_raw=hist_vs_raw,
                hist_vs_std=hist_vs_std,
                fastest_method=fastest_method,
                raw_explain=raw_explain,
                standard_explain=standard_explain,
                histogram_explain=histogram_explain
            )
            queries.append(query_result)
        
        total_queries = len(queries)
        
        # Calculate top improvements and regressions
        top_improvements = []
        regressions = []
        
        for query in queries:
            # Find best improvement
            improvements = [
                ("Standard", -query.std_vs_raw) if query.std_vs_raw < 0 else None,
                ("Histogram", -query.hist_vs_raw) if query.hist_vs_raw < 0 else None
            ]
            improvements = [imp for imp in improvements if imp is not None]
            
            if improvements:
                best_improvement = max(improvements, key=lambda x: x[1])
                if best_improvement[1] > 5:  # Only significant improvements
                    top_improvements.append({
                        "name": query.name,
                        "method": best_improvement[0],
                        "improvement": best_improvement[1]
                    })
            
            # Find regressions
            if query.std_vs_raw > 10:
                regressions.append({
                    "name": query.name,
                    "method": "Standard",
                    "regression": query.std_vs_raw
                })
            if query.hist_vs_raw > 10:
                regressions.append({
                    "name": query.name,
                    "method": "Histogram",
                    "regression": query.hist_vs_raw
                })
        
        # Sort by improvement/regression amount
        top_improvements.sort(key=lambda x: x["improvement"], reverse=True)
        regressions.sort(key=lambda x: x["regression"], reverse=True)
        
        return ComparisonData(
            total_queries=total_queries,
            raw_faster=raw_faster,
            standard_faster=standard_faster,
            histogram_faster=histogram_faster,
            raw_faster_pct=(raw_faster / total_queries * 100) if total_queries > 0 else 0,
            standard_faster_pct=(standard_faster / total_queries * 100) if total_queries > 0 else 0,
            histogram_faster_pct=(histogram_faster / total_queries * 100) if total_queries > 0 else 0,
            queries=queries,
            top_improvements=top_improvements[:10],  # Top 10
            regressions=regressions[:10]  # Top 10
        )
    
    def _get_sql_preview(self, sql_file: Path) -> str:
        """Get SQL content for display"""
        try:
            with open(sql_file, 'r') as f:
                content = f.read().strip()
                # Remove comments but preserve structure
                lines = [line for line in content.split('\n') if not line.strip().startswith('--')]
                # Return the full SQL content
                return '\n'.join(lines)
        except:
            return "SQL preview not available"
            
    def _execute_sql_file_parallel(self, file_path: Path, num_threads: int = 11) -> List[QueryResult]:
        """Execute SQL file with parallel execution for statements with the same prefix"""
        results = []
        
        try:
            # Read the SQL file
            with open(file_path, 'r') as f:
                sql_script = f.read()
            
            # Split into individual queries
            queries = [q.strip() for q in sql_script.split(';') if q.strip()]
            self.log(f"Found {len(queries)} SQL statements to execute in {file_path.name}", "info")
            
            # Group queries by their prefix for parallel execution
            query_groups = self._group_queries_by_prefix(queries)
            self.log(f"Grouped into {len(query_groups)} prefix groups for parallel execution", "info")
            
            # Create a lock for thread-safe result appending
            result_lock = threading.Lock()
            
            # Process each group of queries with the same prefix
            group_id = 0
            for prefix, group_queries in query_groups.items():
                group_id += 1
                self.log(f"Processing group {group_id} with prefix '{prefix}' ({len(group_queries)} statements)", "info")
                
                # Function to execute a query in parallel
                def execute_query(query, query_id):
                    query_name = f"{file_path.stem}_group{group_id}_{query_id}"
                    success, output, duration = self.executor.execute_query(query)
                    
                    query_result = QueryResult(
                        name=query_name,
                        sql=query,
                        success=success,
                        duration=duration,
                        error="" if success else output,
                        output=output if success else ""
                    )
                    
                    # Thread-safe append to results
                    with result_lock:
                        results.append(query_result)
                
                # Create and start threads for this group
                threads = []
                for i, query in enumerate(group_queries):
                    thread = threading.Thread(target=execute_query, args=(query, i+1))
                    threads.append(thread)
                    thread.start()
                    
                    # Limit the number of concurrent threads
                    if len(threads) >= num_threads:
                        # Wait for all threads to complete before starting more
                        for t in threads:
                            t.join()
                        threads = []
                
                # Wait for any remaining threads to complete
                for thread in threads:
                    thread.join()
                    
                self.log(f"Completed group {group_id} with prefix '{prefix}'", "info")
            
            self.log(f"All statements executed, total: {len(results)} queries", "info")
            
        except Exception as e:
            self.log(f"Error in parallel execution: {str(e)}", "error")
            results.append(QueryResult(
                name=f"{file_path.stem}_error",
                sql="",
                success=False,
                duration=0.0,
                error=str(e),
                output=""
            ))
        
        return results
        
    def _group_queries_by_prefix(self, queries):
        """Group SQL queries by their prefix (first word)"""
        groups = {}
        
        for query in queries:
            # Skip empty queries
            if not query.strip():
                continue
                
            # Extract the first word as the prefix
            words = query.strip().split()
            if not words:
                continue
                
            prefix = words[0].upper()
            
            # Add to the appropriate group
            if prefix not in groups:
                groups[prefix] = []
            groups[prefix].append(query)
        
        return groups
        
    def _get_sql_content(self, sql_file: Path) -> str:
        """Get full SQL content from file"""
        try:
            with open(sql_file, 'r') as f:
                content = f.read().strip()
                # Remove comments
                lines = [line for line in content.split('\n') if line.strip() and not line.strip().startswith('--')]
                return '\n'.join(lines)
        except:
            return ""
    
    def _generate_comparison_report(self, comparison_data: ComparisonData):
        """Generate comparison report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path("results") / f"analyze_comparison_{timestamp}.html"
        
        # Load comparison template
        template_content = self._load_comparison_template()
        template = Template(template_content)
        
        html_content = template.render(
            comparison_data=comparison_data,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        
        # Write to file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        self.log(f"Comparison report generated: {output_path}", "success")
    
    def _load_comparison_template(self) -> str:
        """Load comparison template from file"""
        template_path = Path(__file__).parent / "templates" / "comparison.html"
        with open(template_path, 'r') as f:
            return f.read()
    
    def generate_report(self, output_dir: str = "results") -> Path:
        """Generate HTML report with dynamic template updates"""
        # Generate timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(output_dir) / f"job_report_{timestamp}.html"
        
        # Also create a fixed filename based on date only
        date_only = datetime.now().strftime("%Y%m%d")
        fixed_output_path = Path(output_dir) / f"job_report_{date_only}.html"
        
        # Generate report with dynamic template
        self.reporter.generate_html_report(self.results, output_path)
        
        # Copy to fixed date-based filename (overwrite if exists)
        import shutil
        shutil.copy2(output_path, fixed_output_path)
        
        self.log(f"Report generated: {output_path}", "success")
        self.log(f"Fixed report: {fixed_output_path}", "success")
        
        return fixed_output_path

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="BendSQL Job Runner")
    parser.add_argument("--compare", action="store_true", help="Run comparison analysis between different analyze methods")
    parser.add_argument("--setup", action="store_true", help="Set up databases before running analysis")
    parser.add_argument("--limit", type=int, help="Limit the number of queries to run from the check directory")
    parser.add_argument("--check-runs", type=int, default=3, help="Number of times to run each check statement (default: 3)")
    
    args = parser.parse_args()
    
    runner = JobRunner(args)
    
    try:
        # Only setup database if --setup flag is provided
        if args.setup:
            runner.log(f"Setup flag detected, setting up databases... [Elapsed: {runner.get_elapsed_time()}]")
            if not runner.setup_all_databases():
                sys.exit(1)
            runner.log(f"Database setup completed. [Elapsed: {runner.get_elapsed_time()}]")
                
        if args.compare:
            # Run comparison analysis
            runner.log(f"Starting comparison analysis... [Elapsed: {runner.get_elapsed_time()}]")
            if not runner.run_comparison_analysis():
                sys.exit(1)
            runner.log(f"Comparison analysis completed. [Elapsed: {runner.get_elapsed_time()}]")
        elif not args.setup:  # Only run default behavior if neither --setup nor --compare is specified
            # Default behavior: run queries from check directory
            runner.log(f"Starting query execution... [Elapsed: {runner.get_elapsed_time()}]")
            if not runner.run_queries("sql/check"):
                sys.exit(1)
            runner.log(f"Query execution completed. [Elapsed: {runner.get_elapsed_time()}]")

        
        # Always generate report if any operations were performed (except for comparison which generates its own)
        if runner.results and not args.compare:
            runner.log(f"Generating report... [Elapsed: {runner.get_elapsed_time()}]")
            runner.generate_report()
            
        # Print final execution time
        total_time = runner.get_elapsed_time()
        runner.log(f"Job completed successfully. Total execution time: {total_time}", "success")
            
    except KeyboardInterrupt:
        runner.log(f"Operation cancelled by user after {runner.get_elapsed_time()}", "warning")
        sys.exit(1)
    except Exception as e:
        runner.log(f"Unexpected error after {runner.get_elapsed_time()}: {e}", "error")
        sys.exit(1)

if __name__ == "__main__":
    main()
