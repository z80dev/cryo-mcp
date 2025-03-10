"""SQL query functionality for Cryo MCP using DuckDB."""
import os
import re
import json
from pathlib import Path
import duckdb
from typing import Dict, Any, List, Optional, Union

# Default SQL query timeout in seconds
DEFAULT_QUERY_TIMEOUT = 30

def get_data_directory() -> Path:
    """Get the data directory where Cryo files are stored."""
    default_data_dir = str(Path.home() / ".cryo-mcp" / "data")
    return Path(os.environ.get("CRYO_DATA_DIR", default_data_dir))

def create_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with appropriate settings."""
    # In-memory database can't be read-only, so we always use read_only=False
    conn = duckdb.connect(database=":memory:", read_only=False)
    
    # Configure DuckDB settings for performance and safety
    conn.execute("SET memory_limit='4GB'")
    conn.execute("SET max_expression_depth=10000")
    
    # Note: query_timeout_ms setting might not be available in all DuckDB versions
    try:
        conn.execute(f"SET query_timeout_ms={DEFAULT_QUERY_TIMEOUT * 1000}")
    except Exception:
        pass  # Ignore if setting doesn't exist
    
    return conn

def list_available_tables() -> List[Dict[str, Any]]:
    """List all available tables from downloaded data files."""
    data_dir = get_data_directory()
    
    # Find all parquet files in the data directory (including the latest subdirectory)
    parquet_files = list(data_dir.glob("**/*.parquet"))
    
    tables = []
    for file_path in parquet_files:
        # Extract dataset name from filename
        name = file_path.stem.split("__")[0]
        if "__" in file_path.stem:
            name = file_path.stem.split("__")[0]
        else:
            # Try to extract from other naming patterns
            name_match = re.match(r'([a-z_]+)_', file_path.stem)
            if name_match:
                name = name_match.group(1)
            else:
                name = file_path.stem
        
        # Get file stats
        stats = file_path.stat()
        
        # Try to extract block range from filename
        block_range = ""
        blocks_match = re.search(r'blocks__(\d+)_to_(\d+)', str(file_path))
        if blocks_match:
            block_range = f"{blocks_match.group(1)}:{blocks_match.group(2)}"
        
        tables.append({
            "name": name,
            "path": str(file_path),
            "size_bytes": stats.st_size,
            "modified": stats.st_mtime,
            "block_range": block_range,
            "is_latest": "latest" in str(file_path)
        })
    
    return tables

def extract_dataset_from_sql(sql_query: str) -> Optional[str]:
    """
    Try to extract the dataset name from an SQL query.
    
    This is a simple heuristic that looks for FROM clauses in the query.
    
    Args:
        sql_query: The SQL query to parse
        
    Returns:
        The extracted dataset name or None if it couldn't be determined
    """
    # Simple regex to find table names after FROM or JOIN
    # This won't handle all SQL syntax but works for basic queries
    matches = re.findall(r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql_query, re.IGNORECASE)
    
    if matches:
        # Return the first match that isn't a common SQL keyword
        for match in matches:
            if match.lower() not in ('where', 'select', 'group', 'order', 'having', 'limit', 'offset'):
                return match
    
    return None

def execute_sql_query(
    query: str,
    files: Optional[List[str]] = None,
    include_schema: bool = True
) -> Dict[str, Any]:
    """
    Execute a SQL query against specified parquet files.
    
    Args:
        query: SQL query to execute
        files: List of parquet file paths to query. If None, will use all files in the data directory.
        include_schema: Whether to include schema information in the result
        
    Returns:
        Dictionary with query results and metadata
    """
    data_dir = get_data_directory()
    conn = create_connection()
    
    try:
        # Determine which parquet files to use
        parquet_files = []
        if files:
            for file_path in files:
                path = Path(file_path)
                if path.exists() and path.suffix == '.parquet':
                    parquet_files.append(path)
                else:
                    print(f"Warning: File not found or not a parquet file: {file_path}")
        else:
            # If no files provided, use all parquet files in the data directory
            parquet_files = list(data_dir.glob("**/*.parquet"))
        
        if not parquet_files:
            return {
                "success": False,
                "error": "No parquet files available. Download data first with query_dataset."
            }
        
        # Register temporary views for datasets if needed
        has_registered_views = False
        try:
            # Check if the query might be using direct table references without read_parquet()
            potential_tables = extract_tables_from_sql(query)
            
            # Create views for potential table names that aren't using read_parquet
            for table_name in potential_tables:
                if not ("read_parquet" in query.lower() and table_name.lower() in query.lower()):
                    # Match files to table name more precisely
                    # First, look for exact dataset name match (e.g., "blocks" in ethereum__blocks_*.parquet)
                    dataset_pattern = f"__{table_name.lower()}__"
                    exact_matches = [f for f in parquet_files if dataset_pattern in str(f).lower()]
                    
                    # If no exact matches, try looser matching
                    if not exact_matches:
                        # Try matching at word boundaries to avoid partial matches
                        matching_files = []
                        for f in parquet_files:
                            file_lower = str(f).lower()
                            # Match dataset name patterns like ethereum__blocks_* or *_blocks_*
                            if f"__{table_name.lower()}__" in file_lower or f"_{table_name.lower()}_" in file_lower:
                                matching_files.append(f)
                            # Also match if it's just the table name at the start of the filename
                            elif f"/{table_name.lower()}_" in file_lower or f"/{table_name.lower()}." in file_lower:
                                matching_files.append(f)
                    else:
                        matching_files = exact_matches
                    
                    if matching_files:
                        # Create a combined view from all matching files
                        conn.execute(f"DROP VIEW IF EXISTS {table_name}")
                        
                        if len(matching_files) == 1:
                            # If only one file, create a simple view
                            conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM '{matching_files[0]}'")
                            print(f"Registered view '{table_name}' for file: {matching_files[0]}")
                        else:
                            # If multiple files, create a UNION ALL view to join all files
                            union_query = " UNION ALL ".join([f"SELECT * FROM '{file}'" for file in matching_files])
                            conn.execute(f"CREATE VIEW {table_name} AS {union_query}")
                            print(f"Registered view '{table_name}' for {len(matching_files)} files using UNION ALL")
                        
                        has_registered_views = True
            
            # Execute the query
            print(f"Executing SQL query: {query}")
            result = conn.execute(query).fetchdf()
            
            # Convert to records format for easier JSON serialization
            records = result.to_dict(orient="records")
            
            # Get schema information if requested
            schema_info = None
            if include_schema and not result.empty:
                schema_info = {
                    "columns": list(result.columns),
                    "dtypes": {col: str(dtype) for col, dtype in result.dtypes.items()}
                }
            
            # Track how the files were used
            file_usage = {}
            if has_registered_views:
                for table_name in extract_tables_from_sql(query):
                    # Use the same matching logic as above
                    dataset_pattern = f"__{table_name.lower()}__"
                    exact_matches = [f for f in parquet_files if dataset_pattern in str(f).lower()]
                    
                    if not exact_matches:
                        matching_files = []
                        for f in parquet_files:
                            file_lower = str(f).lower()
                            if f"__{table_name.lower()}__" in file_lower or f"_{table_name.lower()}_" in file_lower:
                                matching_files.append(f)
                            elif f"/{table_name.lower()}_" in file_lower or f"/{table_name.lower()}." in file_lower:
                                matching_files.append(f)
                    else:
                        matching_files = exact_matches
                    if matching_files:
                        file_usage[table_name] = {
                            "files": [str(f) for f in matching_files],
                            "combined": len(matching_files) > 1
                        }
            
            return {
                "success": True,
                "result": records,
                "row_count": len(records),
                "schema": schema_info,
                "files_used": [str(f) for f in parquet_files],
                "used_direct_references": has_registered_views,
                "table_mappings": file_usage if file_usage else None
            }
        except Exception as e:
            # Handle query-specific errors
            error_msg = str(e)
            print(f"SQL query error: {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "files_available": [str(f) for f in parquet_files]
            }
    except Exception as e:
        # Handle connection and setup errors
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        # Clean up any registered views
        if has_registered_views:
            for table_name in extract_tables_from_sql(query):
                try:
                    conn.execute(f"DROP VIEW IF EXISTS {table_name}")
                except:
                    pass
        conn.close()

def extract_tables_from_sql(sql_query: str) -> List[str]:
    """Extract table names from an SQL query that aren't using read_parquet."""
    # This extends our extract_dataset_from_sql function for more general use
    import re
    
    # Find potential table names after FROM or JOIN
    matches = re.findall(r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql_query, re.IGNORECASE)
    
    # Filter out common SQL keywords
    sql_keywords = ('where', 'select', 'group', 'order', 'having', 'limit', 'offset')
    return [match for match in matches if match.lower() not in sql_keywords]

def get_table_schema(file_path: str) -> Dict[str, Any]:
    """
    Get schema information for a parquet file.
    
    Args:
        file_path: Path to the parquet file
        
    Returns:
        Dictionary with schema information
    """
    conn = create_connection()
    
    try:
        path = Path(file_path)
        if not path.exists() or path.suffix != '.parquet':
            return {
                "success": False,
                "error": f"File not found or not a parquet file: {file_path}"
            }
        
        # Register a temporary view for the file
        conn.execute(f"CREATE VIEW temp_view AS SELECT * FROM '{file_path}'")
        
        # Get schema info
        schema_result = conn.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='temp_view'").fetchdf()
        
        # Get sample data
        sample_data = conn.execute("SELECT * FROM temp_view LIMIT 5").fetchdf()
        
        # Get row count (might be expensive for large files)
        row_count = conn.execute("SELECT COUNT(*) as count FROM temp_view").fetchone()[0]
        
        return {
            "success": True,
            "file_path": file_path,
            "columns": schema_result.to_dict(orient="records"),
            "sample_data": sample_data.to_dict(orient="records"),
            "row_count": row_count
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        conn.close()