import argparse
import subprocess
import sys
import os
import re
import time
from datetime import datetime
import csv
import math
import logging

# Global logger instance
logger = logging.getLogger(__name__)


def get_bendsql_warehouse_from_env():
    """Retrieve warehouse name from the environment variable."""
    dsn = os.environ.get("BENDSQL_DSN", "")

    # Try to match the first format
    match = re.search(r"--([\w-]+)\.gw", dsn)
    if match:
        return match.group(1)

    # Try to match the second format
    match = re.search(r"warehouse=([\w-]+)", dsn)
    if match:
        return match.group(1)

    raise ValueError("Could not extract warehouse name from BENDSQL_DSN.")


def execute_snowsql(query, database, warehouse):
    """Execute an SQL query using snowsql."""
    command = [
        "snowsql",
        "--warehouse",
        warehouse,
        "--schemaname",
        "PUBLIC",
        "--dbname",
        database,
        "-q",
        query,
    ]

    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"snowsql command failed: {e.stderr}")


def extract_snowsql_time(output):
    """Extract execution time from the snowsql output."""
    match = re.search(r"Time Elapsed:\s*([0-9.]+)s", output)
    return match.group(1) if match else None


def execute_bendsql(query, database):
    """Execute an SQL query using bendsql."""
    command = ["bendsql", "--query=" + query, "--database=" + database, "--time=server"]
    result = subprocess.run(command, text=True, capture_output=True)

    if "APIError: ResponseError" in result.stderr:
        raise RuntimeError(
            f"'APIError: ResponseError' found in bendsql output: {result.stderr}"
        )
    elif result.returncode != 0:
        raise RuntimeError(
            f"bendsql command failed with return code {result.returncode}: {result.stderr}"
        )

    return result.stdout


def extract_bendsql_time(output):
    """Extract execution time from the bendsql output."""
    match = re.search(r"([0-9.]+)$", output)
    return match.group(1) if match else None


def execute_sql(query, sql_tool, database, warehouse=None):
    """General function to execute a SQL query using the specified tool."""
    if sql_tool == "snowsql":
        return execute_snowsql(query, database, warehouse)
    elif sql_tool == "bendsql":
        return execute_bendsql(query, database)
    else:
        raise ValueError(f"Unsupported SQL tool: {sql_tool}")


def execute_bendsql_flamegraph(query, database):
    """Execute EXPLAIN PERF query and return flamegraph SVG content."""
    import subprocess
    import time
    import re
    
    try:
        # Prepare EXPLAIN PERF query
        explain_query = f"EXPLAIN PERF {query}"
        
        # Execute bendsql command with --quote-style never and database
        command = ["bendsql", "--quote-style", "never", "--database", database]
        
        start_time = time.time()
        process = subprocess.Popen(
            command, 
            stdin=subprocess.PIPE, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True
        )
        stdout, stderr = process.communicate(input=explain_query)
        end_time = time.time()
        execution_time = end_time - start_time
        
        if "APIError: ResponseError" in stderr:
            logger.error(f"Flamegraph generation failed - APIError: {stderr}")
            return None, 0
        elif process.returncode != 0:
            logger.error(f"Flamegraph generation failed with return code {process.returncode}: {stderr}")
            return None, 0
            
        # Debug: Log the actual output to understand what we're getting
        logger.info(f"Bendsql output length: {len(stdout)} characters")
        logger.info(f"Bendsql output preview: {stdout[:500]}..." if len(stdout) > 500 else f"Full bendsql output: {stdout}")
        
        # Since bendsql returns complete HTML with flamegraph, use the full content
        if stdout and len(stdout) > 1000 and "flamegraph" in stdout.lower():
            logger.info(f"Flamegraph HTML content extracted successfully (execution time: {execution_time:.2f}s)")
            return stdout, execution_time
        else:
            logger.error("No flamegraph content found in bendsql output")
            logger.error(f"Output contains: {repr(stdout[:200])}")
            return None, 0
        
    except Exception as e:
        logger.error(f"Error generating flamegraph: {e}")
        return None, 0


def setup_flamegraph_directory(base_dir, benchmark_case):
    """Create organized directory structure for flamegraphs using template."""
    from datetime import datetime
    import os
    
    # Create timestamped directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    flamegraph_dir = os.path.join(base_dir, f"{benchmark_case}_{timestamp}")
    os.makedirs(flamegraph_dir, exist_ok=True)
    
    # Load index.html template
    template_path = os.path.join(os.path.dirname(__file__), "templates", "flamegraph_index.html")
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
    except FileNotFoundError:
        logger.error(f"Template file not found: {template_path}")
        return None
    
    # Replace placeholders in template using string replacement
    content = template_content.replace("{benchmark_case}", benchmark_case.upper())
    content = content.replace("{timestamp}", timestamp)
    content = content.replace("{generation_time}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    # Write initial flamegraph HTML file with _flame.html suffix
    flamegraph_filename = f"{benchmark_case}_{timestamp}_flame.html"
    index_path = os.path.join(flamegraph_dir, flamegraph_filename)
    with open(index_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    logger.info(f"Flamegraph directory created: {flamegraph_dir}")
    logger.info(f"Flamegraph file: {index_path}")
    return flamegraph_dir


# Global storage for flamegraph data
flamegraph_data_storage = []

def get_flamegraph_filename(flamegraph_dir):
    """Generate flamegraph filename with _flame.html suffix based on directory name."""
    dir_name = os.path.basename(flamegraph_dir)
    return f"{dir_name}_flame.html"

def initialize_flamegraph_index(flamegraph_dir):
    """Initialize empty flamegraph HTML file at the start."""
    flamegraph_filename = get_flamegraph_filename(flamegraph_dir)
    index_path = os.path.join(flamegraph_dir, flamegraph_filename)
    
    try:
        # Read template
        template_path = os.path.join(os.path.dirname(__file__), "templates", "flamegraph_index.html")

        with open(template_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract benchmark case and timestamp from directory name
        dir_name = os.path.basename(flamegraph_dir)
        parts = dir_name.split('_')
        benchmark_case = parts[0].upper() if parts else 'BENCHMARK'
        timestamp = '_'.join(parts[1:]) if len(parts) > 1 else 'UNKNOWN'
        
        # Replace template variables
        from datetime import datetime
        generation_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        content = content.replace('{benchmark_case}', benchmark_case)
        content = content.replace('{timestamp}', timestamp)
        content = content.replace('{generation_time}', generation_time)
        
        # Initialize with empty content for dynamic parts
        content = content.replace('{{QUERY_ITEMS}}', '')
        content = content.replace('{{FLAMEGRAPH_TEMPLATES}}', '')
        
        # Write initial empty index
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"📊 Initialized flamegraph file: {index_path}")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize flamegraph index: {e}")


def update_flamegraph_index_incremental(flamegraph_dir, query_index, sql_query, flamegraph_content, execution_time):
    """Update flamegraph HTML file incrementally after each query."""
    flamegraph_filename = get_flamegraph_filename(flamegraph_dir)
    index_path = os.path.join(flamegraph_dir, flamegraph_filename)
    
    try:
        # Read current index content
        with open(index_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Format complete SQL query for display (including comments)
        clean_query = sql_query.strip()
        # Keep all lines including comments, just clean up extra whitespace
        lines = [line.rstrip() for line in clean_query.split('\n')]
        # Remove empty lines but keep comments
        formatted_lines = [line for line in lines if line.strip()]
        sql_display = '\n'.join(formatted_lines)
        
        # Create new query item HTML
        query_item = f'''            <li class="query-item">
                <div class="query-header">
                    <div class="query-title">Query {query_index:02d}</div>
                    <div class="query-time">{execution_time:.3f}s</div>
                </div>
                <div class="query-sql"><pre>{sql_display}</pre></div>
                <button class="flamegraph-toggle" onclick="toggleFlamegraph({query_index})">
                    🔥 View Flamegraph Analysis
                </button>
                <div class="flamegraph-content" id="flamegraph-{query_index}" style="display: none;">
                    <div class="loading">Loading flamegraph...</div>
                </div>
            </li>'''
        
        # Create flamegraph template with SVG content only
        flamegraph_template = f'''        <script type="text/html" id="flamegraph-template-{query_index}">
{flamegraph_content}
        </script>'''
        
        # Find insertion points and add new content
        # Insert query item before the closing </ul> of query-list
        query_list_end = content.find('</ul>', content.find('class="query-list"'))
        if query_list_end != -1:
            content = content[:query_list_end] + query_item + '\n        ' + content[query_list_end:]
        
        # Insert template before the closing </body>
        body_end = content.rfind('</body>')
        if body_end != -1:
            content = content[:body_end] + flamegraph_template + '\n    ' + content[body_end:]
        
        # Write updated content
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"📊 Updated flamegraph file with Query {query_index:02d}: {index_path}")
        
    except Exception as e:
        logger.error(f"❌ Failed to update flamegraph index: {e}")


def generate_complete_flamegraph_index(flamegraph_dir):
    """Generate complete index.html with all collected flamegraph data."""
    global flamegraph_data_storage
    
    logger.info(f"🔥 Generating complete flamegraph index with {len(flamegraph_data_storage)} queries")
    
    if not flamegraph_data_storage:
        logger.warning("No flamegraph data collected, creating empty index")
        # Create empty index anyway
        query_items_html = '<li class="query-item"><div class="query-header"><div class="query-title">No queries executed</div></div></li>'
        templates_html = ''
    else:
        # Generate query items and templates
        query_items = []
        flamegraph_templates = []
        
        for data in flamegraph_data_storage:
            query_index = data['query_index']
            sql_preview = data.get('sql_preview', 'SQL query')
            flamegraph_content = data['flamegraph_content']
            execution_time = data.get('execution_time', 0)
            
            # Create query item HTML
            query_item = f'''            <li class="query-item">
                <div class="query-header">
                    <div class="query-title">Query {query_index:02d}</div>
                    <div class="query-time">{execution_time:.3f}s</div>
                </div>
                <div class="query-sql">{sql_preview}</div>
                <button class="flamegraph-toggle" onclick="toggleFlamegraph({query_index})">
                    🔥 View Flamegraph Analysis
                </button>
                <div class="flamegraph-content" id="flamegraph-{query_index}" style="display: none;">
                    <div class="loading">Loading flamegraph...</div>
                </div>
            </li>'''
            query_items.append(query_item)
            
            # Create flamegraph template
            flamegraph_template = f'''        <template id="flamegraph-template-{query_index}">
{flamegraph_content}
        </template>'''
            flamegraph_templates.append(flamegraph_template)
        
        query_items_html = '\n'.join(query_items)
        templates_html = '\n'.join(flamegraph_templates)
        
    flamegraph_filename = get_flamegraph_filename(flamegraph_dir)
    index_path = os.path.join(flamegraph_dir, flamegraph_filename)
    
    try:
        # Read template
        template_path = os.path.join(os.path.dirname(__file__), "templates", "flamegraph_index.html")

        with open(template_path, 'r', encoding='utf-8') as f:
            content = f.read()

        
        # Check if placeholders exist in template
        has_query_placeholder = '{{QUERY_ITEMS}}' in content
        has_template_placeholder = '{{FLAMEGRAPH_TEMPLATES}}' in content

        
        # Log the content we're about to replace with


        # Replace placeholders with generated content
        content_before = content
        content = content.replace('{{QUERY_ITEMS}}', query_items_html)
        content = content.replace('{{FLAMEGRAPH_TEMPLATES}}', templates_html)
        
        # Check if replacement actually happened
        query_replaced = '{{QUERY_ITEMS}}' not in content
        template_replaced = '{{FLAMEGRAPH_TEMPLATES}}' not in content

        
        # Write complete content
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(content)

            
        logger.info(f"📊 Flamegraph file generated with {len(flamegraph_data_storage)} queries: {index_path}")
        
        # Clear the storage for next run
        flamegraph_data_storage.clear()
        
    except Exception as e:
        logger.error(f"❌ Failed to generate flamegraph index: {e}")


def setup_database(database_name, sql_tool, warehouse):
    """Set up the database by dropping and creating it."""
    create_query = f"CREATE OR REPLACE DATABASE {database_name};"
    
    start_time = time.time()
    if sql_tool == "bendsql":
        # For bendsql, we need to use a default database when creating a new one
        execute_sql(create_query, sql_tool, "default", warehouse)
    else:
        # For snowsql, we can use the target database name
        execute_sql(create_query, sql_tool, database_name, warehouse)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Database '{database_name}' has been set up. Time: {elapsed_time:.2f}s")
    return elapsed_time


def restart_warehouse(sql_tool, warehouse, database):
    """Restart a specific warehouse by suspending and then resuming it."""
    start_time = time.time()
    
    if sql_tool == "bendsql":
        alter_suspend = f"ALTER WAREHOUSE \"{warehouse}\" SUSPEND;"
    else:
        alter_suspend = f"ALTER WAREHOUSE {warehouse} SUSPEND;"

    logger.info(f"Suspending warehouse {warehouse}...")
    execute_sql(alter_suspend, sql_tool, database, warehouse)

    time.sleep(2)
    if sql_tool == "bendsql":
        alter_resume = f"ALTER WAREHOUSE '{warehouse}' RESUME;"
    else:
        alter_resume = f"ALTER WAREHOUSE {warehouse} RESUME;"

    execute_sql(alter_resume, sql_tool, database, warehouse)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Warehouse {warehouse} restarted. Time: {elapsed_time:.2f}s")
    return elapsed_time


def create_ascii_table(data, headers, title=None):
    """Create an ASCII table from data."""
    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in data:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    
    # Create horizontal line
    h_line = '+' + '+'.join('-' * (w + 2) for w in col_widths) + '+'
    
    # Create table
    table = []
    if title:
        table.append(title)
    table.append(h_line)
    
    # Add headers
    header_row = '|' + '|'.join(' ' + h.ljust(w) + ' ' for h, w in zip(headers, col_widths)) + '|'
    table.append(header_row)
    table.append(h_line)
    
    # Add data rows
    for row in data:
        data_row = '|' + '|'.join(' ' + str(cell).ljust(w) + ' ' for cell, w in zip(row, col_widths)) + '|'
        table.append(data_row)
    
    table.append(h_line)
    return '\n'.join(table)


def execute_sql_file(sql_file, sql_tool, database, warehouse, suspend, is_setup=False, flamegraph_enabled=False, flamegraph_dir=None):
    global flamegraph_data_storage

    
    # Initialize flamegraph index at the start if flamegraph is enabled
    if flamegraph_enabled and flamegraph_dir and not is_setup:
        initialize_flamegraph_index(flamegraph_dir)
    """Execute SQL queries from a file using the specified tool and write results to a file."""
    with open(sql_file, "r") as file:
        queries = [query.strip() for query in file.read().split(";") if query.strip()]

    results = []
    result_file_path = "query_results.txt"
    mode = "a" if os.path.exists(result_file_path) else "w"
    
    # Create CSV file for results
    csv_file_path = "result.csv"
    with open(csv_file_path, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["Query", "Time(s)"])  # Header
    
    total_start_time = time.time()
    successful_queries = 0
    total_execution_time = 0.0
    total_restart_time = 0.0
    
    phase = "Setup" if is_setup else "Queries"
    logger.info(f"\n{'='*50}\n{phase} Execution - {sql_tool} - Started at {datetime.now().strftime('%H:%M:%S')}\n{'='*50}")

    with open(result_file_path, mode) as result_file:
        # Add header for this execution
        result_file.write(f"\n{'='*50}\n{phase} Execution - {sql_tool} - {datetime.now()}\n{'='*50}\n\n")
        
        for index, query in enumerate(queries):
            query_start_time = time.time()
            restart_time = 0
            
            try:
                # Print real-time progress
                logger.info(f"\nQuery {index+1}/{len(queries)} - Started at {datetime.now().strftime('%H:%M:%S')}")
                logger.info(f"SQL: {query}")
                
                if suspend:
                    restart_time = restart_warehouse(sql_tool, warehouse, database)
                    total_restart_time += restart_time

                query_exec_start = time.time()
                output = execute_sql(query, sql_tool, database, warehouse)

                # Generate flamegraph if enabled and using bendsql
                flamegraph_file = None
                flamegraph_time = 0
                if flamegraph_enabled and sql_tool == "bendsql" and flamegraph_dir and not is_setup:

                    flamegraph_content, flamegraph_time = execute_bendsql_flamegraph(query, database)
                    if flamegraph_content:
                        logger.info(f"🔥 Flamegraph generated for Query {index+1:02d}")
                        # Update flamegraph index immediately
                        update_flamegraph_index_incremental(flamegraph_dir, index+1, query, flamegraph_content, flamegraph_time)
                    else:
                        logger.warning(f"⚠️ No flamegraph content generated for Query {index+1:02d}")
                
                if sql_tool == "snowsql":
                    time_elapsed = extract_snowsql_time(output)
                else:
                    time_elapsed = extract_bendsql_time(output)

                if time_elapsed:
                    time_elapsed_float = float(time_elapsed)
                    total_execution_time += time_elapsed_float
                    successful_queries += 1
                    
                    # Write to CSV file
                    with open(csv_file_path, "a", newline="") as csvfile:
                        csv_writer = csv.writer(csvfile)
                        csv_writer.writerow([index + 1, time_elapsed_float])
                
                query_total_time = time.time() - query_start_time
                
                # Print real-time timing information
                logger.info(f"Query {index+1} completed:")
                logger.info(f"  - Server execution time: {time_elapsed}s")
                logger.info(f"  - Total time (including restart): {query_total_time:.2f}s")
                if restart_time > 0:
                    logger.info(f"  - Warehouse restart time: {restart_time:.2f}s")
                
                result_file.write(f"SQL: {query}\n")
                result_file.write(f"Time Elapsed (server): {time_elapsed}s\n")
                result_file.write(f"Total time (including restart): {query_total_time:.2f}s\n\n")
                
                results.append({
                    "query_index": index + 1,
                    "server_time": float(time_elapsed) if isinstance(time_elapsed, str) else time_elapsed,
                    "total_time": query_total_time,
                    "restart_time": restart_time
                })
                
            except Exception as e:
                query_total_time = time.time() - query_start_time
                logger.error(f"Query {index+1} failed: {e}")
                logger.error(f"  - Total time until failure: {query_total_time:.2f}s")
                
                result_file.write(f"SQL: {query}\nError: {e}\n")
                result_file.write(f"Total time until failure: {query_total_time:.2f}s\n\n")
                
                results.append({
                    "query_index": index + 1,
                    "error": str(e),
                    "total_time": query_total_time,
                    "restart_time": restart_time,
                    "server_time": 0.0
                })

    total_wall_time = time.time() - total_start_time
    
    # Create ASCII table for query times
    table_data = []
    for result in results:
        if "error" not in result:
            table_data.append([result["query_index"], f"{result['server_time']:.2f}s"])
    
    # Sort by query index
    table_data.sort(key=lambda x: x[0])
    
    # Create ASCII table
    query_times_table = create_ascii_table(table_data, ["Query", "Time(s)"], f"{phase} Query Execution Times:")
    
    # Print and write summary statistics
    summary = f"""
{phase} Execution Summary ({sql_tool}):
----------------------------------------
Total queries: {len(queries)}
Successful queries: {successful_queries}
Failed queries: {len(queries) - successful_queries}
Total server execution time: {total_execution_time:.2f}s
Total warehouse restart time: {total_restart_time:.2f}s
Total wall clock time: {total_wall_time:.2f}s
Average query time (server): {(total_execution_time / successful_queries if successful_queries else 0):.2f}s

{query_times_table}
"""
    
    logger.info(summary)
    with open(result_file_path, "a") as result_file:
        result_file.write(summary)
    
    # Flamegraph index is now updated incrementally after each query
    # No need for final generation step
    
    return {
        "total_execution_time": total_execution_time,
        "total_wall_time": total_wall_time,
        "total_restart_time": total_restart_time,
        "successful_queries": successful_queries,
        "total_queries": len(queries),
        "results": results
    }


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Run SQL queries using bendsql or snowsql."
    )
    parser.add_argument("--database", help="Database name", required=True)
    parser.add_argument(
        "--warehouse",
        default="COMPUTE_WH",
        help="Warehouse name for snowsql",
        required=False,
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Setup the database by executing the setup SQL",
    )
    parser.add_argument(
        "--runbend", action="store_true", help="Run only bendsql setup and action"
    )
    parser.add_argument(
        "--runsnow", action="store_true", help="Run only snowsql setup and action"
    )
    parser.add_argument(
        "--suspend",
        default=False,
        action="store_true",
        help="Restart the warehouse before each query",
    )
    parser.add_argument(
        "--case",
        choices=['tpch', 'tpcds'],
        default='tpch',
        help="Specify the benchmark case: TPC-H (default) or TPC-DS",
    )
    parser.add_argument(
        "--flamegraph",
        action="store_true",
        help="Enable flamegraph generation using EXPLAIN PERF",
    )
    parser.add_argument(
        "--flamegraph-dir",
        default="./flamegraphs",
        help="Directory to store flamegraph HTML files (default: ./flamegraphs)",
    )

    return parser.parse_args()


def main():
    # Setup logging
    log_filename = f"benchsb_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )

    args = parse_arguments()

    base_sql_dir = "sql"  # Base directory for SQL files
    database = args.database
    overall_start_time = time.time()

    if args.runbend:
        sql_tool = "bendsql"
        sql_dir = os.path.join(base_sql_dir, "bend")
        warehouse = get_bendsql_warehouse_from_env()
    elif args.runsnow:
        sql_tool = "snowsql"
        sql_dir = os.path.join(base_sql_dir, "snow")
        warehouse = args.warehouse
        # Disable caching of results
        execute_sql(
            "ALTER ACCOUNT SET USE_CACHED_RESULT=FALSE;", sql_tool, database, warehouse
        )
    else:
        logger.error("Please specify --runbend or --runsnow.")
        sys.exit(1)

    logger.info(f"\n{'='*50}\nStarting benchmark with {sql_tool}\n{'='*50}")
    logger.info(f"Database: {database}")
    logger.info(f"Warehouse: {warehouse}")
    logger.info(f"Timestamp: {datetime.now()}")
    
    # Initialize flamegraph settings
    flamegraph_dir = None

    if args.flamegraph:
        if sql_tool != "bendsql":
            logger.warning("⚠️  Flamegraph is only supported with bendsql (--runbend). Disabling flamegraph.")
            args.flamegraph = False
        else:
            flamegraph_dir = setup_flamegraph_directory(args.flamegraph_dir, args.case)
            logger.info(f"🔥 Flamegraph enabled - Output directory: {flamegraph_dir}")
            

    
    setup_stats = {"total_execution_time": 0, "total_wall_time": 0, "total_restart_time": 0, "successful_queries": 0, "total_queries": 0}
    db_setup_time = 0
    
    if args.setup:
        logger.info(f"\n{'='*50}\nStarting setup phase\n{'='*50}")
        db_setup_time = setup_database(database, sql_tool, warehouse)
        # Choose between TPC-H and TPC-DS setup files
        setup_file = os.path.join(sql_dir, "tpcds_setup.sql" if args.case == 'tpcds' else "setup.sql")
        setup_stats = execute_sql_file(setup_file, sql_tool, database, warehouse, False, is_setup=True, flamegraph_enabled=args.flamegraph, flamegraph_dir=flamegraph_dir)
        logger.info(f"Setup completed. Total execution time: {setup_stats['total_execution_time']:.2f}s, Wall time: {setup_stats['total_wall_time']:.2f}s")

    # Choose between TPC-H and TPC-DS queries
    queries_file = os.path.join(sql_dir, "tpcds_queries.sql" if args.case == 'tpcds' else "queries.sql")
    queries_stats = execute_sql_file(queries_file, sql_tool, database, warehouse, args.suspend, is_setup=False, flamegraph_enabled=args.flamegraph, flamegraph_dir=flamegraph_dir)
    logger.info(f"Queries completed. Total execution time: {queries_stats['total_execution_time']:.2f}s, Wall time: {queries_stats['total_wall_time']:.2f}s")

    overall_time = time.time() - overall_start_time
    
    # Print overall summary
    logger.info(f"\n{'='*60}\nFINAL BENCHMARK SUMMARY - {sql_tool.upper()}\n{'='*60}")
    logger.info(f"Database: {database}")
    logger.info(f"Warehouse: {warehouse}")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info(f"{'='*60}")
    
    if args.setup:
        logger.info(f"SETUP PHASE:")
        logger.info(f"  - Database creation time: {db_setup_time:.2f}s")
        logger.info(f"  - Setup queries: {setup_stats['successful_queries']}/{setup_stats['total_queries']} successful")
        logger.info(f"  - Server execution time: {setup_stats['total_execution_time']:.2f}s")
        logger.info(f"  - Warehouse restart time: {setup_stats['total_restart_time']:.2f}s")
        logger.info(f"  - Total wall time: {setup_stats['total_wall_time']:.2f}s")
    
    logger.info(f"\nQUERIES PHASE:")
    logger.info(f"  - Queries: {queries_stats['successful_queries']}/{queries_stats['total_queries']} successful")
    logger.info(f"  - Server execution time: {queries_stats['total_execution_time']:.2f}s")
    logger.info(f"  - Warehouse restart time: {queries_stats['total_restart_time']:.2f}s")
    logger.info(f"  - Total wall time: {queries_stats['total_wall_time']:.2f}s")
    
    logger.info(f"\nOVERALL:")
    total_server_time = setup_stats['total_execution_time'] + queries_stats['total_execution_time']
    total_restart_time = setup_stats['total_restart_time'] + queries_stats['total_restart_time']
    logger.info(f"  - Total server execution time: {total_server_time:.2f}s")
    logger.info(f"  - Total warehouse restart time: {total_restart_time:.2f}s")
    logger.info(f"  - Total benchmark time: {overall_time:.2f}s")
    logger.info(f"{'='*60}")
    
    # Generate comparison table
    headers = ["Metric", "Setup", "Queries", "Overall"]
    data = [
        ["Successful Queries", f"{setup_stats['successful_queries']}/{setup_stats['total_queries']}", f"{queries_stats['successful_queries']}/{queries_stats['total_queries']}", f"{setup_stats['successful_queries'] + queries_stats['successful_queries']}/{setup_stats['total_queries'] + queries_stats['total_queries']}"],
        ["Server Execution Time (s)", f"{setup_stats['total_execution_time']:.2f}", f"{queries_stats['total_execution_time']:.2f}", f"{setup_stats['total_execution_time'] + queries_stats['total_execution_time']:.2f}"],
        ["Warehouse Restart Time (s)", f"{setup_stats['total_restart_time']:.2f}", f"{queries_stats['total_restart_time']:.2f}", f"{setup_stats['total_restart_time'] + queries_stats['total_restart_time']:.2f}"],
        ["Total Wall Time (s)", f"{setup_stats['total_wall_time']:.2f}", f"{queries_stats['total_wall_time']:.2f}", f"{overall_time:.2f}"]
    ]
    
    summary_table = create_ascii_table(data, headers, "Overall Benchmark Summary")
    logger.info(f"\n{summary_table}")
    
    # Add flamegraph summary if enabled
    if args.flamegraph and flamegraph_dir:
        logger.info(f"\n{'='*60}")
        logger.info(f"🔥 FLAMEGRAPH SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"  - Flamegraph directory: {flamegraph_dir}")
        logger.info(f"  - Generated flamegraphs: {queries_stats['successful_queries']} files")

        logger.info(f"{'='*60}")
    
    # Create ASCII table for query times if queries were executed
    if 'results' in queries_stats:
        table_data = []
        for result in queries_stats['results']:
            if "error" not in result:
                table_data.append([result["query_index"], f"{result['server_time']:.2f}s"])
        
        # Sort by query index
        table_data.sort(key=lambda x: x[0])
        
        # Create ASCII table
        query_times_table = create_ascii_table(table_data, ["Query", "Time(s)"], "Query Execution Times:")
    else:
        query_times_table = "No query results available."
    
    # Write summary to file
    with open("benchmark_summary.txt", "a") as summary_file:
        summary_file.write(f"\n{'='*60}\nBENCHMARK SUMMARY - {sql_tool.upper()} - {datetime.now()}\n{'='*60}\n")
        summary_file.write(f"Database: {database}\n")
        summary_file.write(f"Warehouse: {warehouse}\n\n")
        
        if args.setup:
            summary_file.write(f"SETUP PHASE:\n")
            summary_file.write(f"  - Database creation time: {db_setup_time:.2f}s\n")
            summary_file.write(f"  - Setup queries: {setup_stats['successful_queries']}/{setup_stats['total_queries']} successful\n")
            summary_file.write(f"  - Server execution time: {setup_stats['total_execution_time']:.2f}s\n")
            summary_file.write(f"  - Warehouse restart time: {setup_stats['total_restart_time']:.2f}s\n")
            summary_file.write(f"  - Total wall time: {setup_stats['total_wall_time']:.2f}s\n\n")
        
        summary_file.write(f"QUERIES PHASE:\n")
        summary_file.write(f"  - Queries: {queries_stats['successful_queries']}/{queries_stats['total_queries']} successful\n")
        summary_file.write(f"  - Server execution time: {queries_stats['total_execution_time']:.2f}s\n")
        summary_file.write(f"  - Warehouse restart time: {queries_stats['total_restart_time']:.2f}s\n")
        summary_file.write(f"  - Total wall time: {queries_stats['total_wall_time']:.2f}s\n\n")
        
        summary_file.write(f"OVERALL:\n")
        summary_file.write(f"  - Total server execution time: {total_server_time:.2f}s\n")
        summary_file.write(f"  - Total warehouse restart time: {total_restart_time:.2f}s\n")
        summary_file.write(f"  - Total benchmark time: {overall_time:.2f}s\n\n")
        
        # Add query times table to summary
        summary_file.write(f"{query_times_table}\n\n")
        summary_file.write(f"{'='*60}\n")


if __name__ == "__main__":
    main()
