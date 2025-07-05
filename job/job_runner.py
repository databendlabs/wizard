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
    
    def __init__(self):
        pass
    
    def execute_query(self, query: str, timeout: int = 300) -> Tuple[bool, str, float]:
        """Execute a single SQL query"""
        start_time = time.time()
        
        try:
            # Build complete SQL with database context handling
            bendsql_cmd, final_sql = self._build_bendsql_command(query)
            
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
            
            status = "✅ Success" if success else "❌ Failed"
            print(f"    {status} in {duration:.2f}s")
            
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
    
    def _build_bendsql_command(self, sql: str) -> Tuple[List[str], str]:
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
    
    def __init__(self):
        self.executor = BendSQLExecutor()
        self.parser = SQLFileParser()
        self.reporter = ReportGenerator()
        self.results: List[TestResult] = []
    
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
    
    def setup_database(self) -> bool:
        """Set up database from setup.sql"""
        setup_file = Path("sql/setup.sql")
        if not setup_file.exists():
            self.log(f"Setup file not found: {setup_file}", "error")
            return False
        
        self.log("Setting up database...", "info")
        start_time = time.time()
        
        query_results = self.executor.execute_sql_file(setup_file)
        duration = time.time() - start_time
        success = all(r.success for r in query_results)
        
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
    
    def analyze_tables(self, method: str = "standard") -> bool:
        """Analyze tables with specified method"""
        tables = [
            "aka_name", "aka_title", "cast_info", "char_name", "comp_cast_type",
            "company_name", "company_type", "complete_cast", "info_type", "keyword",
            "kind_type", "link_type", "movie_companies", "movie_info", "movie_info_idx",
            "movie_keyword", "movie_link", "name", "person_info", "role_type", "title"
        ]
        
        self.log(f"Analyzing tables with method: {method}", "info")
        start_time = time.time()
        query_results = []
        
        # Get current database from global context
        current_db = db_context.get_database()
        if not current_db:
            self.log("No database context set. Please use 'USE database;' statement first.", "error")
            return False
        
        for table in tables:
            if method == "histogram":
                query = f"set enable_analyze_histogram = 1; analyze table {current_db}.{table}"
            else:
                query = f"analyze table {current_db}.{table}"
            
            success, output, duration = self.executor.execute_query(query)
            query_results.append(QueryResult(
                name=f"analyze_{table}",
                sql=query,
                success=success,
                duration=duration,
                error="" if success else output,
                output=output if success else ""
            ))
        
        total_duration = time.time() - start_time
        success = all(r.success for r in query_results)
        
        stage = StageResult(
            name=f"analyze_{method}",
            queries=query_results,
            duration=total_duration,
            success=success
        )
        
        test_result = TestResult(
            name=f"table_analysis_{method}",
            stages=[stage],
            success=success,
            total_duration=total_duration
        )
        
        self.results.append(test_result)
        
        if success:
            self.log(f"Table analysis completed in {total_duration:.2f}s", "success")
        else:
            self.log(f"Table analysis failed in {total_duration:.2f}s", "error")
        
        return success
    
    def run_queries(self, queries_dir: str = "sql/check") -> bool:
        """Run queries from directory"""
        queries_path = Path(queries_dir)
        if not queries_path.exists():
            self.log(f"Queries directory not found: {queries_path}", "error")
            return False
        
        sql_files = self.parser.find_sql_files(queries_path)
        if not sql_files:
            self.log(f"No SQL files found in {queries_path}", "warning")
            return True
        
        self.log(f"Running {len(sql_files)} query files...", "info")
        start_time = time.time()
        all_query_results = []
        
        for sql_file in sql_files:
            self.log(f"Executing {sql_file.name}...", "info")
            query_results = self.executor.execute_sql_file(sql_file)
            all_query_results.extend(query_results)
        
        total_duration = time.time() - start_time
        success = all(r.success for r in all_query_results)
        
        stage = StageResult(
            name="queries",
            queries=all_query_results,
            duration=total_duration,
            success=success
        )
        
        test_result = TestResult(
            name="query_execution",
            stages=[stage],
            success=success,
            total_duration=total_duration
        )
        
        self.results.append(test_result)
        
        if success:
            self.log(f"Query execution completed in {total_duration:.2f}s", "success")
        else:
            self.log(f"Query execution failed in {total_duration:.2f}s", "error")
        
        return success
    
    def run_comparison_analysis(self) -> bool:
        """Run comparison analysis between different analyze methods"""
        self.log("Starting comparison analysis...", "info")
        
        # Get all SQL files
        sql_files = self.parser.get_sql_files("sql/check")
        if not sql_files:
            self.log("No SQL files found for comparison", "error")
            return False
        
        # Store results for each method
        raw_results = {}
        standard_results = {}
        histogram_results = {}
        
        # Run queries without analyze (raw)
        self.log("Running queries without analyze (raw)...", "info")
        for sql_file in sql_files:
            self.log(f"Executing {sql_file.name} (raw)...", "info")
            query_results = self.executor.execute_sql_file(sql_file)
            if query_results:
                raw_results[sql_file.name] = query_results[0].duration
        
        # Run with standard analyze
        self.log("Running queries with standard analyze...", "info")
        if not self.analyze_tables("standard"):
            self.log("Standard analyze failed", "error")
            return False
        
        for sql_file in sql_files:
            self.log(f"Executing {sql_file.name} (standard)...", "info")
            query_results = self.executor.execute_sql_file(sql_file)
            if query_results:
                standard_results[sql_file.name] = query_results[0].duration
        
        # Run with histogram analyze
        self.log("Running queries with histogram analyze...", "info")
        if not self.analyze_tables("histogram"):
            self.log("Histogram analyze failed", "error")
            return False
        
        for sql_file in sql_files:
            self.log(f"Executing {sql_file.name} (histogram)...", "info")
            query_results = self.executor.execute_sql_file(sql_file)
            if query_results:
                histogram_results[sql_file.name] = query_results[0].duration
        
        # Generate comparison data
        comparison_data = self._generate_comparison_data(raw_results, standard_results, histogram_results, sql_files)
        
        # Store comparison results
        test_result = TestResult(
            name="comparison_analysis",
            stages=[],
            success=True,
            total_duration=0
        )
        self.results.append(test_result)
        
        # Generate comparison report
        self._generate_comparison_report(comparison_data)
        
        self.log("Comparison analysis completed", "success")
        return True
    
    def _generate_comparison_data(self, raw_results: Dict, standard_results: Dict,
                                histogram_results: Dict, sql_files: List[Path]) -> ComparisonData:
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
            
            query_result = ComparisonQueryResult(
                name=filename,
                sql_preview=sql_preview,
                raw_time=raw_time,
                standard_time=standard_time,
                histogram_time=histogram_time,
                std_vs_raw=std_vs_raw,
                hist_vs_raw=hist_vs_raw,
                hist_vs_std=hist_vs_std,
                fastest_method=fastest_method
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
        """Get SQL preview for tooltip"""
        try:
            with open(sql_file, 'r') as f:
                content = f.read().strip()
                # Remove comments and clean up
                lines = [line.strip() for line in content.split('\n') if line.strip() and not line.strip().startswith('--')]
                preview = ' '.join(lines)
                return preview[:200] + "..." if len(preview) > 200 else preview
        except:
            return "SQL preview not available"
    
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
    parser.add_argument("--setup", action="store_true", help="Setup database")
    parser.add_argument("--analyze", choices=["standard", "histogram"], help="Analyze tables")
    parser.add_argument("--run", action="store_true", help="Run queries")
    parser.add_argument("--all", action="store_true", help="Run setup, analyze, and queries")
    parser.add_argument("--compare", action="store_true", help="Run comparison analysis between different analyze methods")
    
    args = parser.parse_args()
    
    if not any([args.setup, args.analyze, args.run, args.all, args.compare]):
        parser.print_help()
        return
    
    runner = JobRunner()
    
    try:
        if args.compare:
            # Run comparison analysis
            if not runner.setup_database():
                sys.exit(1)
            if not runner.run_comparison_analysis():
                sys.exit(1)
        elif args.all:
            # Run complete workflow
            if not runner.setup_database():
                sys.exit(1)
            if not runner.analyze_tables("standard"):
                sys.exit(1)
            if not runner.run_queries():
                sys.exit(1)
        else:
            # Run individual steps
            if args.setup and not runner.setup_database():
                sys.exit(1)
            if args.analyze and not runner.analyze_tables(args.analyze):
                sys.exit(1)
            if args.run and not runner.run_queries():
                sys.exit(1)
        
        # Always generate report if any operations were performed (except for comparison which generates its own)
        if runner.results and not args.compare:
            runner.generate_report()
            
    except KeyboardInterrupt:
        runner.log("Operation cancelled by user", "warning")
        sys.exit(1)
    except Exception as e:
        runner.log(f"Unexpected error: {e}", "error")
        sys.exit(1)

if __name__ == "__main__":
    main()
