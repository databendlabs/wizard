import argparse
import re
import sys
import subprocess
import time
import os
import math
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from collections import defaultdict

# Configure logging to capture all output
class DualLogger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, 'w')
    
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()
    
    def flush(self):
        self.terminal.flush()
        self.log.flush()
    
    def close(self):
        self.log.close()

# Set up dual output
log_filename = f"checksb_{datetime.now():%Y%m%d_%H%M%S}.log"
sys.stdout = DualLogger(log_filename)

logger = logging.getLogger(__name__)

@dataclass
class TestResult:
    case: str
    passed: int = 0
    failed: int = 0
    total: int = 0
    elapsed: float = 0.0
    errors: List[Tuple[str, str, str, str]] = field(default_factory=list)
    
    @property
    def success(self) -> bool:
        return self.failed == 0
    
    @property
    def pass_rate(self) -> float:
        return (self.passed / self.total * 100) if self.total > 0 else 0

class ProgressTracker:
    def __init__(self):
        self.start_time = time.time()
        self.current_case = ""
        self.current_step = ""
        self.cases_completed = 0
        self.total_cases = 0
        self.queries_tested = 0
        self.total_queries_all_cases = 0
    
    def update(self, case: str = None, step: str = None):
        if case:
            self.current_case = case
        if step:
            self.current_step = step
        self._print_status()
    
    def _print_status(self):
        elapsed = time.time() - self.start_time
        eta = self._calculate_eta(elapsed)
        
        status_parts = [
            f"[{datetime.now():%H:%M:%S}]",
            f"Case: {self.current_case}",
            f"Step: {self.current_step}",
            f"Progress: {self.cases_completed}/{self.total_cases} cases",
            f"Elapsed: {elapsed:.1f}s",
            f"ETA: {eta}"
        ]
        
        print(" | ".join(status_parts))
    
    def _calculate_eta(self, elapsed: float) -> str:
        if self.cases_completed == 0:
            return "calculating..."
        
        avg_time = elapsed / self.cases_completed
        remaining = self.total_cases - self.cases_completed
        eta_seconds = avg_time * remaining
        
        if eta_seconds < 60:
            return f"{eta_seconds:.0f}s"
        elif eta_seconds < 3600:
            return f"{eta_seconds/60:.1f}m"
        else:
            return f"{eta_seconds/3600:.1f}h"

class SQLExecutor:
    def __init__(self, tool: str, database: str, warehouse: Optional[str] = None):
        self.tool = tool
        self.database = database
        self.warehouse = warehouse
        self.progress = ProgressTracker()
    
    def execute(self, query: str, description: str = "") -> str:
        if description:
            print(f"  → Executing: {description}")
        
        cmd = self._build_command(query)
        result = subprocess.run(cmd, text=True, capture_output=True)
        
        if self._is_error(result):
            return f"__ERROR__:{result.stdout}\n{result.stderr}"
        
        return result.stdout.replace("None", "NULL")
    
    def _build_command(self, query: str) -> List[str]:
        if self.tool == "snowsql":
            cmd = [
                "snowsql", "--query", query, "--dbname", self.database,
                "--schemaname", "PUBLIC", "-o", "output_format=tsv",
                "-o", "header=false", "-o", "timing=false", "-o", "friendly=false"
            ]
            if self.warehouse:
                cmd.extend(["--warehouse", self.warehouse])
            return cmd
        
        return ["bendsql", f"--query={query}", "-D", self.database]
    
    def _is_error(self, result: subprocess.CompletedProcess) -> bool:
        output = result.stdout + result.stderr
        return any(err in output.lower() for err in ["error", "unknown function"])

class QueryComparator:
    @staticmethod
    def normalize_line(line: str) -> str:
        import math
        
        # Split by tabs to preserve column structure
        parts = line.split('\t')
        normalized_parts = []
        
        for part in parts:
            part = part.strip()  # Remove leading/trailing whitespace
            if not part:
                normalized_parts.append('')
                continue
                
            try:
                # Handle special float values first
                if part.lower() in ['inf', 'infinity', '+inf', '+infinity']:
                    normalized_parts.append('inf')
                    continue
                elif part.lower() in ['-inf', '-infinity']:
                    normalized_parts.append('-inf')
                    continue
                elif part.lower() in ['nan', '+nan', '-nan']:
                    normalized_parts.append('nan')
                    continue
                
                # Try to parse as number
                num = round(float(part), 3)
                
                # Handle extremely large finite values
                if abs(num) > 1.79e+308:  # Approximate double max
                    normalized_parts.append('inf' if num > 0 else '-inf')
                elif math.isinf(num):
                    normalized_parts.append('inf' if num > 0 else '-inf')
                elif math.isnan(num):
                    normalized_parts.append('nan')
                else:
                    normalized_parts.append(str(int(num)) if num == int(num) else str(num))
            except ValueError:
                # Handle boolean case normalization
                if part.lower() == 'true':
                    normalized_parts.append('true')
                elif part.lower() == 'false':
                    normalized_parts.append('false')
                # Handle timestamp precision normalization
                elif ':' in part and '.' in part:
                    # Normalize timestamp precision (e.g., 00:00:00.000000 -> 00:00:00.000)
                    if part.count('.') == 1:
                        time_part, fraction = part.split('.')
                        # Truncate to 3 decimal places for millisecond precision
                        normalized_fraction = fraction[:3].ljust(3, '0')
                        normalized_parts.append(f"{time_part}.{normalized_fraction}")
                    else:
                        normalized_parts.append(part)
                else:
                    normalized_parts.append(part)
        
        return '\t'.join(normalized_parts)
    
    @classmethod
    def compare(cls, bend_result: str, snow_result: str, query: str = None) -> Tuple[bool, Optional[str]]:
        # Check for empty result sets
        bend_lines = [cls.normalize_line(line) for line in bend_result.splitlines() if line.strip()]
        snow_lines = [cls.normalize_line(line) for line in snow_result.splitlines() if line.strip()]
        
        # Mark empty result sets as unsuitable
        if len(bend_lines) == 0 and len(snow_lines) == 0:
            return True, "UNSUITABLE: empty result set"
        elif len(bend_lines) == 0:
            return False, "UNSUITABLE: bendsql returned empty result set"
        elif len(snow_lines) == 0:
            return False, "UNSUITABLE: snowsql returned empty result set"
        
        # Continue with regular comparison if not empty
        if bend_result == snow_result:
            return True, "exact match"
        
        if bend_lines == snow_lines:
            return True, "numeric normalization"
        
        return False, cls._generate_diff(bend_lines, snow_lines, query)
    
    @staticmethod
    def _generate_diff(databend_lines, snowflake_lines, query=None):
        """Generate contextual diff output showing only different rows with context"""
        try:
            from tabulate import tabulate
        except ImportError:
            # Fallback to simple format if tabulate not available
            return QueryComparator._generate_contextual_simple_diff(databend_lines, snowflake_lines, query)
        
        diff_output = []
        
        # Add SQL query at the top if provided
        if query:
            diff_output.append(" SQL Query that caused the difference:")
            diff_output.append("┌─────────────────────────────────────────────────────────────────────────┐")
            # Split query into lines and format each line
            query_lines = query.strip().split('\n')
            for line in query_lines:
                formatted_line = f"│ {line:<71} │"
                diff_output.append(formatted_line)
            diff_output.append("└─────────────────────────────────────────────────────────────────────────┘")
            diff_output.append("")  # Empty line
        
        # Process data and find differences
        all_data = []
        diff_rows = []  # Track which rows have differences
        
        for i, (db_line, sf_line) in enumerate(zip(databend_lines, snowflake_lines)):
            db_cols = QueryComparator.normalize_line(db_line).split('\t')
            sf_cols = QueryComparator.normalize_line(sf_line).split('\t')
            max_cols = max(len(db_cols), len(sf_cols))
            
            # Pad shorter list with empty strings
            while len(db_cols) < max_cols:
                db_cols.append('')
            while len(sf_cols) < max_cols:
                sf_cols.append('')
            
            all_data.append((db_cols, sf_cols))
            
            # Check if this row has differences
            if db_cols != sf_cols:
                diff_rows.append(i)
        
        # If no differences found, show summary
        if not diff_rows:
            diff_output.append("No differences found in the result sets.")
            return "\n".join(diff_output)
        
        # Determine number of columns to show
        max_cols_to_show = 4  # Default limit
        
        # Check if there are differences in hidden columns
        has_hidden_differences = False
        for db_cols, sf_cols in all_data:
            max_cols = max(len(db_cols), len(sf_cols))
            if max_cols > max_cols_to_show:
                for j in range(max_cols_to_show, max_cols):
                    db_val = db_cols[j] if j < len(db_cols) else ''
                    sf_val = sf_cols[j] if j < len(sf_cols) else ''
                    if db_val != sf_val:
                        has_hidden_differences = True
                        break
            if has_hidden_differences:
                break
        
        # If hidden differences found, show all columns
        if has_hidden_differences:
            max_cols_to_show = max(max(len(db_cols), len(sf_cols)) for db_cols, sf_cols in all_data)
        
        # Generate contextual diff - show only different rows with context
        context_lines = 2  # Number of context lines before and after differences
        
        # Find ranges of rows to display (differences + context)
        display_ranges = []
        for diff_row in diff_rows:
            start = max(0, diff_row - context_lines)
            end = min(len(all_data), diff_row + context_lines + 1)
            display_ranges.append((start, end))
        
        # Merge overlapping ranges
        merged_ranges = []
        for start, end in sorted(display_ranges):
            if merged_ranges and start <= merged_ranges[-1][1]:
                merged_ranges[-1] = (merged_ranges[-1][0], max(merged_ranges[-1][1], end))
            else:
                merged_ranges.append((start, end))
        
        # Generate headers
        headers = [f"col{i+1}" for i in range(max_cols_to_show)]
        table_headers = ["Row", "Engine"] + headers + ["Status"]
        
        # Generate table data for each range
        total_diff_rows = len(diff_rows)
        total_rows = len(all_data)
        
        diff_output.append(f" Contextual Diff (showing {total_diff_rows} different rows out of {total_rows} total rows):")
        diff_output.append("")
        
        for range_idx, (start, end) in enumerate(merged_ranges):
            if range_idx > 0:
                diff_output.append("")
                diff_output.append("..." + "─" * 50 + "...")
                diff_output.append("")
            
            table_data = []
            
            for i in range(start, end):
                db_cols, sf_cols = all_data[i]
                
                # Find differences in displayed columns
                differences = []
                for j in range(max_cols_to_show):
                    db_val = db_cols[j] if j < len(db_cols) else ''
                    sf_val = sf_cols[j] if j < len(sf_cols) else ''
                    if db_val != sf_val:
                        differences.append(j)
                
                # Check for differences in hidden columns
                hidden_differences = False
                max_cols = max(len(db_cols), len(sf_cols))
                if max_cols > max_cols_to_show:
                    for j in range(max_cols_to_show, max_cols):
                        db_val = db_cols[j] if j < len(db_cols) else ''
                        sf_val = sf_cols[j] if j < len(sf_cols) else ''
                        if db_val != sf_val:
                            hidden_differences = True
                            break
                
                # Prepare row data
                db_row_data = [f"Row {i+1}", "Databend"]
                sf_row_data = ["", "Snowflake"]
                
                # Add column values
                for j in range(max_cols_to_show):
                    db_val = db_cols[j] if j < len(db_cols) else ''
                    sf_val = sf_cols[j] if j < len(sf_cols) else ''
                    
                    if j in differences:
                        db_row_data.append(db_val + " ←")
                        sf_row_data.append(sf_val + " ←")
                    else:
                        db_row_data.append(db_val)
                        sf_row_data.append(sf_val)
                
                # Add status - show X for rows with differences (visible or hidden)
                if differences or hidden_differences:
                    db_row_data.append("")
                    sf_row_data.append("X")
                else:
                    db_row_data.append("")
                    sf_row_data.append("")  # No status for matching rows
                
                table_data.append(db_row_data)
                table_data.append(sf_row_data)
            
            # Generate table using tabulate with prettier formatting
            table_str = tabulate(
                table_data,
                headers=table_headers,
                tablefmt="fancy_grid",
                stralign="left",
                numalign="right",
                floatfmt=".2f"
            )
            diff_output.append(table_str)
        
        # Add summary information
        diff_output.append(f"\nSummary:")
        diff_output.append(f"  Found {total_diff_rows} different rows out of {total_rows} total rows.")
        if has_hidden_differences:
            diff_output.append("  Differences found in hidden columns (beyond column 4).")
        diff_output.append(f"  Showing context of ±{context_lines} lines around differences.")
        
        return "\n".join(diff_output)
    
    @staticmethod
    def _generate_simple_diff(databend_lines, snowflake_lines, query=None):
        """Fallback simple diff format when tabulate is not available"""
        diff_output = []
        
        if query:
            diff_output.append(" SQL Query:")
            diff_output.append("-" * 60)
            diff_output.append(query)
            diff_output.append("-" * 60)
            diff_output.append("")
        
        diff_output.append(" COMPARISON RESULTS:")
        diff_output.append("=" * 60)
        
        for i, (db_line, sf_line) in enumerate(zip(databend_lines, snowflake_lines)):
            db_cols = QueryComparator.normalize_line(db_line).split('\t')
            sf_cols = QueryComparator.normalize_line(sf_line).split('\t')
            
            diff_output.append(f"\nRow {i+1}:")
            diff_output.append(f"  Databend:  {' | '.join(db_cols[:4])}")
            diff_output.append(f"  Snowflake: {' | '.join(sf_cols[:4])}")
            
            if db_cols != sf_cols:
                diff_output.append("  Status: X MISMATCH")
            else:
                diff_output.append("  Status: O MATCH")
        
        return "\n".join(diff_output)
    
    @staticmethod
    def _generate_contextual_simple_diff(databend_lines, snowflake_lines, query=None):
        """Fallback contextual simple diff format when tabulate is not available"""
        diff_output = []
        
        if query:
            diff_output.append(" SQL Query:")
            diff_output.append("-" * 60)
            diff_output.append(query)
            diff_output.append("-" * 60)
            diff_output.append("")
        
        # Find rows with differences
        diff_rows = []
        all_data = []
        
        for i, (db_line, sf_line) in enumerate(zip(databend_lines, snowflake_lines)):
            db_cols = QueryComparator.normalize_line(db_line).split('\t')
            sf_cols = QueryComparator.normalize_line(sf_line).split('\t')
            all_data.append((db_cols, sf_cols))
            
            if db_cols != sf_cols:
                diff_rows.append(i)
        
        if not diff_rows:
            diff_output.append("No differences found in the result sets.")
            return "\n".join(diff_output)
        
        # Generate contextual diff
        context_lines = 2
        total_diff_rows = len(diff_rows)
        total_rows = len(all_data)
        
        diff_output.append(f" CONTEXTUAL COMPARISON RESULTS:")
        diff_output.append(f" (showing {total_diff_rows} different rows out of {total_rows} total rows)")
        diff_output.append("=" * 60)
        
        # Find ranges of rows to display (differences + context)
        display_ranges = []
        for diff_row in diff_rows:
            start = max(0, diff_row - context_lines)
            end = min(len(all_data), diff_row + context_lines + 1)
            display_ranges.append((start, end))
        
        # Merge overlapping ranges
        merged_ranges = []
        for start, end in sorted(display_ranges):
            if merged_ranges and start <= merged_ranges[-1][1]:
                merged_ranges[-1] = (merged_ranges[-1][0], max(merged_ranges[-1][1], end))
            else:
                merged_ranges.append((start, end))
        
        # Display each range
        for range_idx, (start, end) in enumerate(merged_ranges):
            if range_idx > 0:
                diff_output.append("")
                diff_output.append("..." + "-" * 50 + "...")
                diff_output.append("")
            
            for i in range(start, end):
                db_cols, sf_cols = all_data[i]
                
                diff_output.append(f"\nRow {i+1}:")
                diff_output.append(f"  Databend:  {' | '.join(db_cols[:4])}")
                diff_output.append(f"  Snowflake: {' | '.join(sf_cols[:4])}")
                
                if db_cols != sf_cols:
                    diff_output.append("  Status: X MISMATCH")
                else:
                    diff_output.append("  Status: O MATCH")
        
        diff_output.append(f"\nSummary:")
        diff_output.append(f"  Found {total_diff_rows} different rows out of {total_rows} total rows.")
        diff_output.append(f"  Showing context of ±{context_lines} lines around differences.")
        
        return "\n".join(diff_output)

class CheckSB:
    def __init__(self, args):
        self.args = args
        self.bend_executor = SQLExecutor("bendsql", args.database)
        self.snow_executor = SQLExecutor("snowsql", args.database, args.warehouse)
        self.results = {}
        self.progress = ProgressTracker()
    
    def run(self):
        cases = self._get_cases()
        skip_list = {s.strip().lower() for s in self.args.skip.split(",") if s}
        
        self.progress.total_cases = len(cases)
        self._print_header(f"SQL Compatibility Test", cases)
        
        for idx, case in enumerate(cases, 1):
            if case.lower() in skip_list:
                print(f"\n Skipping {case} (--skip argument)")
                self.progress.cases_completed += 1
                continue
            
            self.progress.update(case=case, step="initializing")
            
            # Enhanced case separator
            print(f"\n\n{'#' * 80}")
            print(f"{'#' * 80}")
            print(f"##  [{idx}/{len(cases)}] CASE: {case.upper()}")
            print(f"{'#' * 80}")
            print(f"{'#' * 80}\n")
            
            result = self._run_case(case)
            self.results[case] = result
            self.progress.cases_completed += 1
            
            self._print_case_summary(result)
        
        self._print_final_summary()
    
    def _get_cases(self) -> List[str]:
        if self.args.case.lower() == "all":
            return sorted([d.name for d in Path("sql").iterdir() if d.is_dir()])
        return [c.strip() for c in self.args.case.split(",")]
    
    def _run_case(self, case: str) -> TestResult:
        base_dir = Path("sql") / case
        
        if not self.args.check_only:
            self._setup_case(base_dir, case)
        else:
            print(f"\n  Skipping setup and action phases (--check-only mode)")
        
        self.progress.update(step="running comparison checks")
        return self._check_case(base_dir / "check.sql", case)
    
    def _setup_case(self, base_dir: Path, case: str):
        tools = []
        
        if self.args.runbend:
            tools = [("bendsql", self.bend_executor, "bend")]
        elif self.args.runsnow:
            tools = [("snowsql", self.snow_executor, "snow")]
        else:
            tools = [
                ("bendsql", self.bend_executor, "bend"),
                ("snowsql", self.snow_executor, "snow")
            ]
        
        for tool_name, executor, subdir in tools:
            self.progress.update(step=f"setting up with {tool_name}")
            print(f"\n  Setting up {case} with {tool_name}")
            
            # Database setup
            self.progress.update(step=f"{tool_name}: creating database")
            print(f"    • Creating database {self.args.database}")
            self._setup_database(executor)
            
            # Setup scripts
            setup_path = base_dir / subdir / "setup.sql"
            self.progress.update(step=f"{tool_name}: running setup.sql")
            print(f"    • Running {setup_path}")
            self._execute_script(setup_path, executor)
            
            # Action scripts
            action_path = base_dir / "action.sql"
            self.progress.update(step=f"{tool_name}: running action.sql")
            print(f"    • Running {action_path}")
            self._execute_script(action_path, executor)
            
            print(f"    Setup complete")
    
    def _setup_database(self, executor: SQLExecutor):
        db = self.args.database
        base_executor = SQLExecutor(executor.tool, "default", executor.warehouse)
        base_executor.execute(f"DROP DATABASE IF EXISTS {db}", "drop database")
        base_executor.execute(f"CREATE DATABASE {db}", "create database")
    
    def _execute_script(self, script_path: Path, executor: SQLExecutor):
        with open(script_path) as f:
            queries = [q.strip() for q in f.read().split(";") if q.strip()]
        
        for i, query in enumerate(queries, 1):
            executor.execute(query, f"query {i}/{len(queries)}")
    
    def _check_case(self, check_path: Path, case: str) -> TestResult:
        result = TestResult(case=case)
        start_time = time.time()
        
        print(f"\n  Running comparison checks")
        
        with open(check_path) as f:
            queries = [q.strip() for q in f.read().split(";") if q.strip()]
        
        result.total = len(queries)
        
        # Only run comparison if neither runbend nor runsnow is specified
        if self.args.runbend or self.args.runsnow:
            print(f"    Skipping comparison (single tool mode)")
            result.elapsed = time.time() - start_time
            return result
        
        for i, query in enumerate(queries, 1):
            query_id = self._extract_query_id(query, i)
            self.progress.update(step=f"checking query {i}/{len(queries)}: {query_id}")
            
            print(f"\n    [{i}/{len(queries)}] Testing: {query_id}")
            
            print(f"      • Executing on bendsql...", end="", flush=True)
            bend_result = self.bend_executor.execute(query)
            print(" done")
            
            print(f"      • Executing on snowsql...", end="", flush=True)
            snow_result = self.snow_executor.execute(query)
            print(" done")
            
            if self._handle_errors(bend_result, snow_result, query_id, result):
                continue
            
            print(f"      • Comparing results...", end="", flush=True)
            match, match_type = QueryComparator.compare(bend_result, snow_result, query)
            
            if match:
                result.passed += 1
                print(f" MATCH ({match_type})")
            else:
                result.failed += 1
                result.errors.append((query_id, match_type, bend_result, snow_result))
                print(f" MISMATCH")
                print(f"        {match_type}")
        
        result.elapsed = time.time() - start_time
        return result
    
    def _handle_errors(self, bend_result: str, snow_result: str, query_id: str, result: TestResult) -> bool:
        bend_err = bend_result.startswith("__ERROR__:")
        snow_err = snow_result.startswith("__ERROR__:")
        
        if bend_err or snow_err:
            result.failed += 1
            bend_msg = bend_result[9:][:100] if bend_err else "OK"
            snow_msg = snow_result[9:][:100] if snow_err else "OK"
            result.errors.append((query_id, "Execution Error", bend_msg, snow_msg))
            
            print(f" ERROR")
            if bend_err:
                print(f"        bendsql: {bend_msg}")
            if snow_err:
                print(f"        snowsql: {snow_msg}")
            return True
        return False
    
    def _extract_query_id(self, query: str, index: int) -> str:
        match = re.search(r"--\s*([\w-]+):", query)
        return match.group(1) if match else f"Query-{index}"
    
    def _print_header(self, title: str, cases: List[str]):
        print(f"\n{'=' * 80}")
        print(f"{title.center(80)}")
        print(f"{'=' * 80}")
        print(f"Cases to run: {', '.join(cases)}")
        print(f"Database: {self.args.database}")
        
        # Extract warehouse from BENDSQL_DSN if available
        dsn_warehouse = extract_warehouse_from_dsn()
        if dsn_warehouse:
            print(f"bendsql warehouse: {dsn_warehouse}")
        
        print(f"snowsql warehouse: {self.args.warehouse}")
        # Get CLI versions
        bendsql_version, snowsql_version = get_cli_versions()
        print(f"bendsql --version: {bendsql_version}")
        print(f"snowsql --version: {snowsql_version}")
        
        if self.args.runbend:
            mode = "bendsql only"
        elif self.args.runsnow:
            mode = "snowsql only"
        else:
            mode = "full comparison"
        
        if self.args.check_only:
            mode += " (check-only)"
        
        print(f"Mode: {mode}")
        print(f"{'=' * 80}")
    
    def _print_case_summary(self, result: TestResult):
        status = "PASSED" if result.success else "FAILED"
        
        print(f"\n  Case Summary: {result.case}")
        print(f"     Status: {status}")
        print(f"     Results: {result.passed}/{result.total} passed ({result.pass_rate:.1f}%)")
        print(f"     Time: {result.elapsed:.1f}s")
        
        if result.errors:
            print(f"     Failed queries:")
            for query_id, _, _, _ in result.errors[:3]:
                print(f"       - {query_id}")
            if len(result.errors) > 3:
                print(f"       ... and {len(result.errors) - 3} more")
    
    def _print_final_summary(self):
        total_elapsed = time.time() - self.progress.start_time
        
        # Big separator for final summary
        print(f"\n\n{'='*80}")
        print(f"{'='*80}")
        print("FINAL SUMMARY - ALL CASES".center(80))
        print(f"{'='*80}")
        print(f"{'='*80}\n")
        
        # Individual case summaries
        print(f"Individual Case Results:\n")
        
        for i, (case, result) in enumerate(self.results.items(), 1):
            status_icon = "PASSED" if result.success else "FAILED"
            
            print(f"  {i}. {case}:")
            print(f"     Status: {status_icon}")
            print(f"     Queries: {result.passed}/{result.total} passed ({result.pass_rate:.1f}%)")
            print(f"     Time: {result.elapsed:.1f}s")
            
            if result.errors:
                print(f"     Failed queries: {len(result.errors)}")
                for query_id, error_type, _, _ in result.errors[:2]:
                    print(f"       - {query_id} ({error_type})")
                if len(result.errors) > 2:
                    print(f"       ... and {len(result.errors) - 2} more")
            print()
        
        # Overall statistics
        print(f"\n{'─'*80}")
        print(f"Overall Statistics:")
        
        total_cases = len(self.results)
        passed_cases = sum(1 for r in self.results.values() if r.success)
        failed_cases = total_cases - passed_cases
        total_queries = sum(r.total for r in self.results.values())
        passed_queries = sum(r.passed for r in self.results.values())
        
        print(f"   Total Cases: {total_cases}")
        print(f"   Passed Cases: {passed_cases} ({passed_cases/total_cases*100:.1f}%)")
        print(f"   Failed Cases: {failed_cases} ({failed_cases/total_cases*100:.1f}%)")
        print(f"   Total Queries: {total_queries}")
        print(f"   Passed Queries: {passed_queries} ({passed_queries/total_queries*100:.1f}%)")
        print(f"   Total Time: {total_elapsed:.1f}s")
        
        # Environment information
        print(f"\n{'─'*80}")
        print(f"Environment Information:")
        
        # Get CLI versions
        bendsql_version, snowsql_version = get_cli_versions()
        
        # Get warehouse info
        bend_warehouse = extract_warehouse_from_dsn() or "version-test"
        snow_warehouse = self.args.warehouse
        
        # Determine mode
        if self.args.runbend:
            mode = "bendsql only"
        elif self.args.runsnow:
            mode = "snowsql only"
        else:
            mode = "full comparison"
        
        if self.args.check_only:
            mode += " (check-only)"
        
        # Get cases list
        cases_list = list(self.results.keys())
        cases_str = ", ".join(cases_list) if len(cases_list) <= 3 else f"{', '.join(cases_list[:3])}, ..."
        
        print(f"   Cases to run: {cases_str}")
        print(f"   Database: {self.args.database}")
        print(f"   bendsql warehouse: {bend_warehouse}")
        print(f"   snowsql warehouse: {snow_warehouse}")
        
        # Get database engine versions
        databend_version, snowflake_version = get_database_versions(self.bend_executor, self.snow_executor)
        print(f"   Databend version: {databend_version}")
        print(f"   Snowflake version: {snowflake_version}")
        print(f"   bendsql --version: {bendsql_version}")
        print(f"   snowsql --version: {snowsql_version}")
        
        print(f"   Mode: {mode}")
        
        # Final verdict
        print(f"\n{'─'*80}")
        if failed_cases == 0:
            print(f"\nALL TESTS PASSED!")
        else:
            failed_case_names = [case for case, r in self.results.items() if not r.success]
            print(f"\n{failed_cases} CASE(S) FAILED")
            print(f"   Failed cases: {', '.join(failed_case_names)}")
        
        # Concise summary for easy copy-paste to developers
        print()
        for case, result in self.results.items():
            print(f"{case} ... {result.passed} passed, {result.failed} failed")

def extract_warehouse_from_dsn() -> Optional[str]:
    """Extract warehouse name from BENDSQL_DSN environment variable."""
    dsn = os.environ.get('BENDSQL_DSN', '')
    if not dsn:
        return None
    
    # Format: databend://bohu:xx!@tnscfp003--pr18131.gw.aws-us-east-2.default.databend.com
    match = re.search(r'@[^@]+--([^.]+)\.', dsn)
    if match:
        return match.group(1)
    return None

def get_cli_versions() -> Tuple[str, str]:
    """Get the versions of bendsql and snowsql CLI tools."""
    bendsql_version = "Not installed"
    snowsql_version = "Not installed"
    
    try:
        result = subprocess.run(["bendsql", "--version"], text=True, capture_output=True)
        if result.returncode == 0:
            bendsql_version = result.stdout.strip()
    except FileNotFoundError:
        pass
    
    try:
        result = subprocess.run(["snowsql", "--version"], text=True, capture_output=True)
        if result.returncode == 0:
            snowsql_version = result.stdout.strip()
    except FileNotFoundError:
        pass
    
    return bendsql_version, snowsql_version

def get_database_versions(bend_executor: SQLExecutor, snow_executor: SQLExecutor) -> Tuple[str, str]:
    """Get the versions of Databend and Snowflake database engines."""
    databend_version = "Not available"
    snowflake_version = "Not available"
    
    # Get Databend version
    try:
        result = bend_executor.execute("select version();", "Getting Databend version")
        if not result.startswith("__ERROR__:"):
            # Extract version from result, typically in format like "version()\nDatabendQuery v1.2.x-..."
            lines = result.strip().split('\n')
            if len(lines) >= 2:
                databend_version = lines[1].strip()
            elif len(lines) == 1 and lines[0] != "version()":
                databend_version = lines[0].strip()
    except Exception:
        pass
    
    # Get Snowflake version
    try:
        result = snow_executor.execute("select CURRENT_VERSION();", "Getting Snowflake version")
        if not result.startswith("__ERROR__:"):
            # Extract version from result, typically in format like "CURRENT_VERSION()\n8.x.x"
            lines = result.strip().split('\n')
            if len(lines) >= 2:
                snowflake_version = lines[1].strip()
            elif len(lines) == 1 and lines[0] != "CURRENT_VERSION()":
                snowflake_version = lines[0].strip()
    except Exception:
        pass
    
    return databend_version, snowflake_version

def main():
    parser = argparse.ArgumentParser(description="Compare SQL execution on different databases")
    parser.add_argument("--database", default="checksb_db", help="Database name")
    parser.add_argument("--warehouse", default="COMPUTE_WH", help="Warehouse for snowsql")
    parser.add_argument("--case", required=True, help="Cases to execute (comma-separated or 'all')")
    parser.add_argument("--runbend", action="store_true", help="Run only bendsql")
    parser.add_argument("--runsnow", action="store_true", help="Run only snowsql")
    parser.add_argument("--skip", default="", help="Cases to skip (comma-separated)")
    parser.add_argument("--summary-only", action="store_true", help="Show only summary, suppress detailed diff tables")
    parser.add_argument("--check-only", action="store_true", help="Only run check.sql, skip setup and action phases")
    
    args = parser.parse_args()
    
    try:
        CheckSB(args).run()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Ensure log file is properly closed
        if hasattr(sys.stdout, 'close'):
            sys.stdout.close()
            sys.stdout = sys.__stdout__

if __name__ == "__main__":
    main()
