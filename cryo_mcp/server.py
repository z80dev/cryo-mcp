# cryo_mcp/server.py
"""
Cryo MCP - A Model Completion Protocol server for the Cryo blockchain data extraction tool.

This module provides a server that exposes Cryo's functionality through the MCP protocol,
allowing blockchain data querying through an API interface geared at usage by LLMs.
"""
import json
import os
import subprocess
import requests
import argparse
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
from mcp.server.fastmcp import FastMCP

# Get the default RPC URL from environment or use fallback
DEFAULT_RPC_URL = "http://localhost:8545"

# Default data directory for storing output
DEFAULT_DATA_DIR = str(Path.home() / ".cryo-mcp" / "data")

# Create an MCP server
mcp = FastMCP("Cryo Data Server")

def get_latest_block_number() -> Optional[int]:
    """Get the latest block number from the Ethereum node"""
    rpc_url = os.environ.get("ETH_RPC_URL", DEFAULT_RPC_URL)
    
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    }
    
    try:
        response = requests.post(rpc_url, json=payload)
        response_data = response.json()
        
        if 'result' in response_data:
            # Convert hex to int
            latest_block = int(response_data['result'], 16)
            print(f"Latest block number: {latest_block}")
            return latest_block
        else:
            print(f"Error fetching latest block: {response_data.get('error', 'Unknown error')}")
            return None
    except Exception as e:
        print(f"Exception when fetching latest block: {e}")
        return None

@mcp.tool()
def list_datasets() -> List[str]:
    """Return a list of all available cryo datasets"""
    # Ensure we have the RPC URL
    rpc_url = os.environ.get("ETH_RPC_URL", DEFAULT_RPC_URL)
    
    result = subprocess.run(
        ["cryo", "help", "datasets", "-r", rpc_url],
        capture_output=True,
        text=True
    )

    # Parse the output to extract dataset names
    lines = result.stdout.split('\n')
    datasets = []

    for line in lines:
        if line.startswith('- ') and not line.startswith('- blocks_and_transactions:'):
            # Extract dataset name, removing any aliases
            dataset = line[2:].split(' (alias')[0].strip()
            datasets.append(dataset)
        if line == 'dataset group names':
            break

    return datasets

@mcp.tool()
def query_dataset(
    dataset: str,
    blocks: Optional[str] = None,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    use_latest: bool = False,
    blocks_from_latest: Optional[int] = None,
    contract: Optional[str] = None,
    output_format: str = "json",
    include_columns: Optional[List[str]] = None,
    exclude_columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Download blockchain data and return the file paths where the data is stored.
    
    IMPORTANT WORKFLOW NOTE: When running SQL queries, use this function first to download
    data, then use the returned file paths with query_sql() to execute SQL on those files.
    
    Example workflow for SQL:
    1. First download data: result = query_dataset('transactions', blocks='1000:1010', output_format='parquet')
    2. Get file paths: files = result.get('files', [])
    3. Run SQL query: query_sql("SELECT * FROM read_parquet('/path/to/file.parquet')", files=files)

    DATASET-SPECIFIC PARAMETERS:
    For datasets that require specific address parameters (like 'balances', 'erc20_transfers', etc.),
    ALWAYS use the 'contract' parameter to pass ANY Ethereum address. For example:
    
    - For 'balances' dataset: Use contract parameter for the address you want balances for
      query_dataset('balances', blocks='1000:1010', contract='0x123...')
    
    - For 'logs' or 'erc20_transfers': Use contract parameter for contract address
      query_dataset('logs', blocks='1000:1010', contract='0x123...')
    
    To check what parameters a dataset requires, always use lookup_dataset() first:
    lookup_dataset('balances')  # Will show required parameters

    Args:
        dataset: The name of the dataset to query (e.g., 'logs', 'transactions', 'balances')
        blocks: Block range specification as a string (e.g., '1000:1010')
        start_block: Start block number as integer (alternative to blocks)
        end_block: End block number as integer (alternative to blocks)
        use_latest: If True, query the latest block
        blocks_from_latest: Number of blocks before the latest to include (e.g., 10 = latest-10 to latest)
        contract: Contract address to filter by - IMPORTANT: Use this parameter for ALL address-based filtering
          regardless of the parameter name in the native cryo command (address, contract, etc.)
        output_format: Output format (json, csv, parquet) - use 'parquet' for SQL queries
        include_columns: Columns to include alongside the defaults
        exclude_columns: Columns to exclude from the defaults

    Returns:
        Dictionary containing file paths where the downloaded data is stored
    """
    # Ensure we have the RPC URL
    rpc_url = os.environ.get("ETH_RPC_URL", DEFAULT_RPC_URL)
    
    # Build the cryo command
    cmd = ["cryo", dataset, "-r", rpc_url]

    # Handle block range (priority: blocks > use_latest > start/end_block > default)
    if blocks:
        # Use specified block range string directly
        cmd.extend(["-b", blocks])
    elif use_latest or blocks_from_latest is not None:
        # Get the latest block number
        latest_block = get_latest_block_number()
        
        if latest_block is None:
            return {"error": "Failed to get the latest block number from the RPC endpoint"}
        
        if blocks_from_latest is not None:
            # Use a range of blocks up to the latest
            start = latest_block - blocks_from_latest
            block_range = f"{start}:{latest_block+1}"  # +1 to make it inclusive
        else:
            # Just the latest block
            block_range = f"{latest_block}:{latest_block+1}"  # +1 to make it inclusive
        
        print(f"Using latest block range: {block_range}")
        cmd.extend(["-b", block_range])
    elif start_block is not None:
        # Convert integer block numbers to string range
        if end_block is not None:
            # Note: cryo uses [start:end) range (inclusive start, exclusive end)
            # Add 1 to end_block to include it in the range
            block_range = f"{start_block}:{end_block+1}"
        else:
            # If only start_block is provided, get 10 blocks starting from there
            block_range = f"{start_block}:{start_block+10}"
        
        print(f"Using block range: {block_range}")
        cmd.extend(["-b", block_range])
    else:
        # Default to a reasonable block range if none specified
        cmd.extend(["-b", "1000:1010"])

    # Handle dataset-specific address parameters
    # For all address-based filters, we use the contract parameter
    # but map it to the correct flag based on the dataset
    if contract:
        # Check if this is a dataset that requires a different parameter name
        if dataset == 'balances':
            # For balances dataset, contract parameter maps to --address
            cmd.extend(["--address", contract])
        else:
            # For other datasets like logs, transactions, etc. use --contract
            cmd.extend(["--contract", contract])

    if output_format == "json":
        cmd.append("--json")
    elif output_format == "csv":
        cmd.append("--csv")

    if include_columns:
        cmd.append("--include-columns")
        cmd.extend(include_columns)

    if exclude_columns:
        cmd.append("--exclude-columns")
        cmd.extend(exclude_columns)

    # Get the base data directory
    data_dir = Path(os.environ.get("CRYO_DATA_DIR", DEFAULT_DATA_DIR))
    
    # Choose output directory based on whether we're querying latest blocks
    if use_latest or blocks_from_latest is not None:
        output_dir = data_dir / "latest"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Clean up the latest directory before new query
        print("Cleaning latest directory for current block query")
        existing_files = list(output_dir.glob(f"*{dataset}*.*"))
        for file in existing_files:
            try:
                file.unlink()
                print(f"Removed existing file: {file}")
            except Exception as e:
                print(f"Warning: Could not remove file {file}: {e}")
    else:
        # For historical queries, use the main data directory
        output_dir = data_dir
        output_dir.mkdir(parents=True, exist_ok=True)

    cmd.extend(["-o", str(output_dir)])

    # Print the command for debugging
    print(f"Running query command: {' '.join(cmd)}")
    
    # Execute the command
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        return {
            "error": result.stderr,
            "stdout": result.stdout,
            "command": " ".join(cmd)
        }

    # Try to find the report file which contains info about generated files
    report_dir = output_dir / ".cryo" / "reports"
    if report_dir.exists():
        # Get the most recent report file (should be the one we just created)
        report_files = sorted(report_dir.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
        if report_files:
            with open(report_files[0], 'r') as f:
                report_data = json.load(f)
                # Get the list of completed files from the report
                if "results" in report_data and "completed_paths" in report_data["results"]:
                    completed_files = report_data["results"]["completed_paths"]
                    print(f"Found {len(completed_files)} files in Cryo report: {completed_files}")
                    
                    # Return the list of files and their count
                    return {
                        "files": completed_files,
                        "count": len(completed_files),
                        "format": output_format
                    }
    
    # Fallback to glob search if report file not found or doesn't contain the expected data
    output_files = list(output_dir.glob(f"*{dataset}*.{output_format}"))
    print(f"Output files found via glob: {output_files}")

    if not output_files:
        return {"error": "No output files generated", "command": " ".join(cmd)}

    # Convert Path objects to strings for JSON serialization
    file_paths = [str(file_path) for file_path in output_files]
    
    return {
        "files": file_paths,
        "count": len(file_paths),
        "format": output_format
    }

@mcp.resource("dataset://{name}")
def get_dataset_info(name: str) -> Dict[str, Any]:
    """Get information about a specific dataset"""
    # Ensure we have the RPC URL
    rpc_url = os.environ.get("ETH_RPC_URL", DEFAULT_RPC_URL)
    
    result = subprocess.run(
        ["cryo", "help", name, "-r", rpc_url],
        capture_output=True,
        text=True
    )

    # Get the latest block number for examples
    latest_block = get_latest_block_number()
    latest_example = ""
    
    if latest_block:
        latest_example = f"query_dataset('{name}', blocks_from_latest=10)  # Gets latest-10 to latest blocks"
    
    # Add special examples for datasets requiring address parameters
    address_example = ""
    if "address" in result.stdout.lower() and "required parameters: address" in result.stdout.lower():
        address_example = f"query_dataset('{name}', blocks='1000:1010', contract='0x123...')  # Use contract parameter for address"
    
    return {
        "name": name,
        "description": result.stdout,
        "example_queries": [
            f"query_dataset('{name}', blocks='1000:1010')",
            f"query_dataset('{name}', start_block=1000, end_block=1009)",
            f"query_dataset('{name}', use_latest=True)  # Gets just the latest block",
            latest_example,
            address_example
        ] if address_example else [
            f"query_dataset('{name}', blocks='1000:1010')",
            f"query_dataset('{name}', start_block=1000, end_block=1009)",
            f"query_dataset('{name}', use_latest=True)  # Gets just the latest block",
            latest_example
        ],
        "notes": [
            "Block ranges are inclusive for start_block and end_block when using integer parameters.",
            "Use 'use_latest=True' to query only the latest block.",
            "Use 'blocks_from_latest=N' to query the latest N blocks.",
            "IMPORTANT: For datasets requiring an 'address' parameter (like 'balances'), use the 'contract' parameter.",
            "Always check the required parameters in the dataset description and use lookup_dataset() first."
        ]
    }

@mcp.tool()
def lookup_dataset(
    name: str,
    sample_start_block: Optional[int] = None,
    sample_end_block: Optional[int] = None,
    use_latest_sample: bool = False,
    sample_blocks_from_latest: Optional[int] = None
) -> Dict[str, Any]:
    """
    Look up a specific dataset and return detailed information about it. IMPORTANT: Always use this
    function before querying a new dataset to understand its required parameters and schema.
    
    The returned information includes:
    1. Required parameters for the dataset (IMPORTANT for datasets like 'balances' that need an address)
    2. Schema details showing available columns and data types
    3. Example queries for the dataset
    
    When the dataset requires specific parameters like 'address' (for 'balances'),
    ALWAYS use the 'contract' parameter in query_dataset() to pass these values.
    
    Example:
    For 'balances' dataset, lookup_dataset('balances') will show it requires an 'address' parameter.
    You should then query it using:
    query_dataset('balances', blocks='1000:1010', contract='0x1234...')
    
    Args:
        name: The name of the dataset to look up
        sample_start_block: Optional start block for sample data (integer)
        sample_end_block: Optional end block for sample data (integer)
        use_latest_sample: If True, use the latest block for sample data
        sample_blocks_from_latest: Number of blocks before the latest to include in sample
        
    Returns:
        Detailed information about the dataset including schema and available fields
    """
    # Get basic dataset info
    info = get_dataset_info(name)
    
    # Ensure we have the RPC URL
    rpc_url = os.environ.get("ETH_RPC_URL", DEFAULT_RPC_URL)
    
    # Get schema information by running the dataset with --dry-run
    schema_result = subprocess.run(
        ["cryo", name, "--dry-run", "-r", rpc_url],
        capture_output=True,
        text=True
    )
    
    if schema_result.returncode == 0:
        info["schema"] = schema_result.stdout
    else:
        info["schema_error"] = schema_result.stderr
    
    # Try to get a sample of the dataset (first 5 records)
    try:
        data_dir = Path(os.environ.get("CRYO_DATA_DIR", DEFAULT_DATA_DIR))
        
        # Determine block range for sample (priority: latest > specified blocks > default)
        if use_latest_sample or sample_blocks_from_latest is not None:
            # Get the latest block number
            latest_block = get_latest_block_number()
            
            if latest_block is None:
                info["sample_error"] = "Failed to get the latest block number from the RPC endpoint"
                return info
            
            if sample_blocks_from_latest is not None:
                # Use a range of blocks from latest-n to latest
                block_range = f"{latest_block - sample_blocks_from_latest}:{latest_block+1}"
            else:
                # Just the latest 5 blocks
                block_range = f"{latest_block-4}:{latest_block+1}"
            
            info["sample_block_range"] = block_range
            
            # Use the latest directory for latest block samples
            sample_dir = data_dir / "latest"
            sample_dir.mkdir(parents=True, exist_ok=True)
            
            # Clean up the latest directory before new query
            print("Cleaning latest directory for current sample")
            existing_files = list(sample_dir.glob(f"*{name}*.*"))
            for file in existing_files:
                try:
                    file.unlink()
                    print(f"Removed existing sample file: {file}")
                except Exception as e:
                    print(f"Warning: Could not remove sample file {file}: {e}")
        else:
            # For historical blocks, get the start block and end block
            if sample_start_block is not None:
                if sample_end_block is not None:
                    # Note: cryo uses [start:end) range (inclusive start, exclusive end)
                    # Add 1 to end_block to include it in the range
                    block_range = f"{sample_start_block}:{sample_end_block+1}"
                else:
                    # Use start block and get 5 blocks
                    block_range = f"{sample_start_block}:{sample_start_block+5}"
            else:
                # Default to a known good block range
                block_range = "1000:1005"
            
            # For historical samples, use the main data directory
            sample_dir = data_dir
            sample_dir.mkdir(parents=True, exist_ok=True)
                
        # Use the block range for the sample
        sample_cmd = [
            "cryo", name, 
            "-b", block_range,
            "-r", rpc_url,
            "--json", 
            "-o", str(sample_dir)
        ]
        
        print(f"Running sample command: {' '.join(sample_cmd)}")
        sample_result = subprocess.run(
            sample_cmd,
            capture_output=True,
            text=True,
            timeout=30  # Add timeout to prevent hanging
        )
        
        if sample_result.returncode == 0:
            # Try to find the report file which contains info about generated files
            report_dir = sample_dir / ".cryo" / "reports"
            if report_dir.exists():
                # Get the most recent report file
                report_files = sorted(report_dir.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
                if report_files:
                    with open(report_files[0], 'r') as f:
                        report_data = json.load(f)
                        # Get the list of completed files from the report
                        if "results" in report_data and "completed_paths" in report_data["results"]:
                            completed_files = report_data["results"]["completed_paths"]
                            print(f"Found {len(completed_files)} files in Cryo report: {completed_files}")
                            info["sample_files"] = completed_files
                            return info
            
            # Fallback to glob search if report file not found
            output_files = list(sample_dir.glob(f"*{name}*.json"))
            print(f"Output files found via glob: {output_files}")
            
            if output_files:
                # Convert Path objects to strings for JSON serialization
                file_paths = [str(file_path) for file_path in output_files]
                info["sample_files"] = file_paths
            else:
                info["sample_error"] = "No output files generated"
        else:
            info["sample_error"] = sample_result.stderr
            info["sample_stdout"] = sample_result.stdout  # Include stdout for debugging
    except (subprocess.TimeoutExpired, Exception) as e:
        info["sample_error"] = str(e)
    
    return info

@mcp.tool()
def get_transaction_by_hash(
    tx_hash: str
) -> Dict[str, Any]:
    """
    Get detailed information about a transaction by its hash
    
    Args:
        tx_hash: The transaction hash to look up
        
    Returns:
        Detailed information about the transaction
    """
    # Ensure we have the RPC URL
    rpc_url = os.environ.get("ETH_RPC_URL", DEFAULT_RPC_URL)
    
    # Use RPC directly to get the transaction
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getTransactionByHash",
        "params": [tx_hash],
        "id": 1
    }
    
    try:
        response = requests.post(rpc_url, json=payload)
        response_data = response.json()
        
        if 'result' in response_data and response_data['result']:
            tx_data = response_data['result']
            
            # Get the receipt as well for additional information (gas used, status)
            receipt_payload = {
                "jsonrpc": "2.0",
                "method": "eth_getTransactionReceipt",
                "params": [tx_hash],
                "id": 2
            }
            
            receipt_response = requests.post(rpc_url, json=receipt_payload)
            receipt_data = receipt_response.json()
            
            if 'result' in receipt_data and receipt_data['result']:
                receipt = receipt_data['result']
                
                # Combine transaction and receipt data
                result = {
                    "transaction_hash": tx_hash,
                    "block_number": int(tx_data.get("blockNumber", "0x0"), 16),
                    "block_hash": tx_data.get("blockHash"),
                    "from_address": tx_data.get("from"),
                    "to_address": tx_data.get("to"),
                    "value": tx_data.get("value"),
                    "value_decimal": int(tx_data.get("value", "0x0"), 16),
                    "gas_limit": int(tx_data.get("gas", "0x0"), 16),
                    "gas_price": int(tx_data.get("gasPrice", "0x0"), 16),
                    "nonce": int(tx_data.get("nonce", "0x0"), 16),
                    "input": tx_data.get("input"),
                    "transaction_index": int(tx_data.get("transactionIndex", "0x0"), 16),
                    "gas_used": int(receipt.get("gasUsed", "0x0"), 16),
                    "status": int(receipt.get("status", "0x0"), 16),
                    "logs_count": len(receipt.get("logs", [])),
                    "contract_address": receipt.get("contractAddress")
                }
                
                # Handle EIP-1559 transactions
                if "maxFeePerGas" in tx_data:
                    result["max_fee_per_gas"] = int(tx_data.get("maxFeePerGas", "0x0"), 16)
                    result["max_priority_fee_per_gas"] = int(tx_data.get("maxPriorityFeePerGas", "0x0"), 16)
                    result["transaction_type"] = int(tx_data.get("type", "0x0"), 16)
                
                return result
            else:
                # Return just the transaction data if receipt is not available
                return {
                    "transaction_hash": tx_hash,
                    "block_number": int(tx_data.get("blockNumber", "0x0"), 16),
                    "block_hash": tx_data.get("blockHash"),
                    "from_address": tx_data.get("from"),
                    "to_address": tx_data.get("to"),
                    "value": tx_data.get("value"),
                    "value_decimal": int(tx_data.get("value", "0x0"), 16),
                    "gas_limit": int(tx_data.get("gas", "0x0"), 16),
                    "gas_price": int(tx_data.get("gasPrice", "0x0"), 16),
                    "nonce": int(tx_data.get("nonce", "0x0"), 16),
                    "input": tx_data.get("input"),
                    "transaction_index": int(tx_data.get("transactionIndex", "0x0"), 16),
                    "error": "Failed to retrieve transaction receipt"
                }
        else:
            return {"error": f"Transaction not found: {tx_hash}"}
    except Exception as e:
        return {"error": f"Exception when fetching transaction: {e}"}

@mcp.tool()
def get_latest_ethereum_block() -> Dict[str, Any]:
    """
    Get information about the latest Ethereum block
    
    Returns:
        Information about the latest block including block number
    """
    latest_block = get_latest_block_number()
    
    if latest_block is None:
        return {"error": "Failed to get the latest block number from the RPC endpoint"}
    
    # Get block data using cryo
    rpc_url = os.environ.get("ETH_RPC_URL", DEFAULT_RPC_URL)
    block_range = f"{latest_block}:{latest_block+1}"  # +1 to make it inclusive
    
    data_dir = Path(os.environ.get("CRYO_DATA_DIR", DEFAULT_DATA_DIR))
    latest_dir = data_dir / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    
    # Always clean up the latest directory for latest block
    print("Cleaning latest directory for current block")
    existing_files = list(latest_dir.glob("*blocks*.*"))
    for file in existing_files:
        try:
            file.unlink()
            print(f"Removed existing file: {file}")
        except Exception as e:
            print(f"Warning: Could not remove file {file}: {e}")
    
    cmd = [
        "cryo", "blocks", 
        "-b", block_range,
        "-r", rpc_url,
        "--json", 
        "-o", str(latest_dir)
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        return {
            "block_number": latest_block,
            "error": "Failed to get detailed block data",
            "stderr": result.stderr
        }
    
    # Try to find the report file which contains info about generated files
    report_dir = latest_dir / ".cryo" / "reports"
    if report_dir.exists():
        # Get the most recent report file
        report_files = sorted(report_dir.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
        if report_files:
            with open(report_files[0], 'r') as f:
                report_data = json.load(f)
                # Get the list of completed files from the report
                if "results" in report_data and "completed_paths" in report_data["results"]:
                    completed_files = report_data["results"]["completed_paths"]
                    print(f"Found {len(completed_files)} files in Cryo report: {completed_files}")
                    
                    return {
                        "block_number": latest_block,
                        "files": completed_files,
                        "count": len(completed_files)
                    }
    
    # Fallback to glob search if report file not found
    output_files = list(latest_dir.glob("*blocks*.json"))
    
    if not output_files:
        return {
            "block_number": latest_block,
            "error": "No output files generated"
        }
    
    # Convert Path objects to strings for JSON serialization
    file_paths = [str(file_path) for file_path in output_files]
    
    return {
        "block_number": latest_block,
        "files": file_paths,
        "count": len(file_paths)
    }

@mcp.tool()
def query_sql(
    query: str,
    files: Optional[List[str]] = None,
    include_schema: bool = True
) -> Dict[str, Any]:
    """
    Run a SQL query against downloaded blockchain data files
    
    IMPORTANT WORKFLOW: This function should be used after calling query_dataset
    to download data. Use the file paths returned by query_dataset as input to this function.
    
    Workflow steps:
    1. Download data: result = query_dataset('transactions', blocks='1000:1010', output_format='parquet')
    2. Get file paths: files = result.get('files', [])
    3. Execute SQL using either:
       - Direct table references: query_sql("SELECT * FROM transactions", files=files)
       - Or read_parquet(): query_sql("SELECT * FROM read_parquet('/path/to/file.parquet')", files=files)
    
    To see the schema of a file, use get_sql_table_schema(file_path) before writing your query.
    
    DuckDB supports both approaches:
    1. Direct table references (simpler): "SELECT * FROM blocks"
    2. read_parquet function (explicit): "SELECT * FROM read_parquet('/path/to/file.parquet')"
    
    Args:
        query: SQL query to execute - can use simple table names or read_parquet()
        files: List of parquet file paths to query (typically from query_dataset results)
        include_schema: Whether to include schema information in the result
        
    Returns:
        Query results and metadata
    """
    from cryo_mcp.sql import execute_sql_query
    return execute_sql_query(query, files, include_schema)

@mcp.tool()
def list_available_sql_tables() -> List[Dict[str, Any]]:
    """
    List all available parquet files that can be queried with SQL
    
    USAGE NOTES:
    - This function lists parquet files that have already been downloaded
    - Each file can be queried using read_parquet('/path/to/file.parquet') in your SQL
    - For each file, this returns the file path, dataset type, and other metadata
    - Use these file paths in your SQL queries with query_sql()
    
    Returns:
        List of available files and their metadata
    """
    from cryo_mcp.sql import list_available_tables
    return list_available_tables()

@mcp.tool()
def get_sql_table_schema(file_path: str) -> Dict[str, Any]:
    """
    Get the schema and sample data for a specific parquet file
    
    WORKFLOW NOTE: Use this function to explore the structure of parquet files
    before writing SQL queries against them. This will show you:
    1. All available columns and their data types
    2. Sample data from the file
    3. Total row count
    
    Usage example:
    1. Get list of files: files = list_available_sql_tables()
    2. For a specific file: schema = get_sql_table_schema(files[0]['path'])
    3. Use columns in your SQL: query_sql("SELECT column1, column2 FROM read_parquet('/path/to/file.parquet')")
    
    Args:
        file_path: Path to the parquet file (from list_available_sql_tables or query_dataset)
        
    Returns:
        Table schema information including columns, data types, and sample data
    """
    from cryo_mcp.sql import get_table_schema
    return get_table_schema(file_path)

@mcp.tool()
def query_blockchain_sql(
    sql_query: str,
    dataset: Optional[str] = None,
    blocks: Optional[str] = None,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    use_latest: bool = False,
    blocks_from_latest: Optional[int] = None,
    contract: Optional[str] = None,
    force_refresh: bool = False,
    include_schema: bool = True
) -> Dict[str, Any]:
    """
    Download blockchain data and run SQL query in a single step
    
    CONVENIENCE FUNCTION: This combines query_dataset and query_sql into one call.
    
    You can write SQL queries using either approach:
    1. Simple table references: "SELECT * FROM blocks LIMIT 10"
    2. Explicit read_parquet: "SELECT * FROM read_parquet('/path/to/file.parquet') LIMIT 10"
    
    DATASET-SPECIFIC PARAMETERS:
    For datasets that require specific address parameters (like 'balances', 'erc20_transfers', etc.),
    ALWAYS use the 'contract' parameter to pass ANY Ethereum address. For example:
    
    - For 'balances' dataset: Use contract parameter for the address you want balances for
      query_blockchain_sql(
          sql_query="SELECT * FROM balances",
          dataset="balances",
          blocks='1000:1010',
          contract='0x123...'  # Address you want balances for
      )
    
    Examples:
    ```
    # Using simple table name
    query_blockchain_sql(
        sql_query="SELECT * FROM blocks LIMIT 10",
        dataset="blocks",
        blocks_from_latest=100
    )
    
    # Using read_parquet() (the path will be automatically replaced)
    query_blockchain_sql(
        sql_query="SELECT * FROM read_parquet('/any/path.parquet') LIMIT 10",
        dataset="blocks",
        blocks_from_latest=100
    )
    ```
    
    ALTERNATIVE WORKFLOW (more control):
    If you need more control, you can separate the steps:
    1. Download data: result = query_dataset('blocks', blocks_from_latest=100, output_format='parquet')
    2. Inspect schema: schema = get_sql_table_schema(result['files'][0])
    3. Run SQL query: query_sql("SELECT * FROM blocks", files=result['files'])
    
    Args:
        sql_query: SQL query to execute - using table names or read_parquet()
        dataset: The specific dataset to query (e.g., 'transactions', 'logs', 'balances')
                 If None, will be extracted from the SQL query
        blocks: Block range specification as a string (e.g., '1000:1010')
        start_block: Start block number (alternative to blocks)
        end_block: End block number (alternative to blocks)
        use_latest: If True, query the latest block
        blocks_from_latest: Number of blocks before the latest to include
        contract: Contract address to filter by - IMPORTANT: Use this parameter for ALL address-based filtering
          regardless of the parameter name in the native cryo command (address, contract, etc.)
        force_refresh: Force download of new data even if it exists
        include_schema: Include schema information in the result
        
    Returns:
        SQL query results and metadata
    """
    from cryo_mcp.sql import execute_sql_query, extract_dataset_from_sql
    
    # Try to determine dataset if not provided
    if dataset is None:
        dataset = extract_dataset_from_sql(sql_query)
        if dataset is None:
            return {
                "success": False,
                "error": "Could not determine dataset from SQL query. Please specify dataset parameter."
            }
    
    # First, ensure we have the data by running a query_dataset operation
    # This will download the data and return the file paths
    download_result = query_dataset(
        dataset=dataset,
        blocks=blocks,
        start_block=start_block,
        end_block=end_block,
        use_latest=use_latest,
        blocks_from_latest=blocks_from_latest,
        contract=contract,
        output_format="parquet"  # Use parquet for optimal SQL performance
    )
    
    if "error" in download_result:
        return {
            "success": False,
            "error": f"Failed to download data: {download_result['error']}",
            "download_details": download_result
        }
    
    # Get the file paths from the download result
    files = download_result.get("files", [])
    
    # Check if we have any files
    if not files:
        return {
            "success": False,
            "error": "No data files were generated from the download operation"
        }
    
    # Filter for parquet files only
    parquet_files = [f for f in files if f.endswith('.parquet')]
    if not parquet_files:
        return {
            "success": False,
            "error": "No parquet files were generated. Check output_format parameter."
        }
    
    # Now execute the SQL query directly against the downloaded parquet files
    sql_result = execute_sql_query(sql_query, parquet_files, include_schema)
    
    # Include download info in result
    sql_result["data_source"] = {
        "dataset": dataset,
        "files": files,
        "block_range": blocks or f"{start_block}:{end_block}" if start_block and end_block else "latest blocks" 
                        if use_latest or blocks_from_latest else "default range"
    }
    
    return sql_result

@mcp.tool()
def get_sql_examples() -> Dict[str, List[str]]:
    """
    Get example SQL queries for different blockchain datasets with DuckDB
    
    SQL WORKFLOW TIPS:
    1. First download data: result = query_dataset('dataset_name', blocks='...', output_format='parquet')
    2. Inspect schema: schema = get_sql_table_schema(result['files'][0])
    3. Run SQL: query_sql("SELECT * FROM read_parquet('/path/to/file.parquet')", files=result['files'])
    
    OR use the combined approach:
    - query_blockchain_sql(sql_query="SELECT * FROM read_parquet('...')", dataset='blocks', blocks='...')
    
    Returns:
        Dictionary of example queries categorized by dataset type and workflow patterns
    """
    return {
        "basic_usage": [
            "-- Option 1: Simple table names (recommended)",
            "SELECT * FROM blocks LIMIT 10",
            "SELECT * FROM transactions LIMIT 10",
            "SELECT * FROM logs LIMIT 10",
            
            "-- Option 2: Using read_parquet() with explicit file paths",
            "SELECT * FROM read_parquet('/path/to/blocks.parquet') LIMIT 10"
        ],
        "transactions": [
            "-- Option 1: Simple table reference",
            "SELECT * FROM transactions LIMIT 10",
            "SELECT block_number, COUNT(*) as tx_count FROM transactions GROUP BY block_number ORDER BY tx_count DESC LIMIT 10",
            
            "-- Option 2: Using read_parquet()",
            "SELECT from_address, COUNT(*) as sent_count FROM read_parquet('/path/to/transactions.parquet') GROUP BY from_address ORDER BY sent_count DESC LIMIT 10",
            "SELECT to_address, SUM(value) as total_eth FROM read_parquet('/path/to/transactions.parquet') GROUP BY to_address ORDER BY total_eth DESC LIMIT 10"
        ],
        "blocks": [
            "SELECT * FROM blocks LIMIT 10",
            "SELECT block_number, gas_used, transaction_count FROM blocks ORDER BY gas_used DESC LIMIT 10",
            "SELECT AVG(gas_used) as avg_gas, AVG(transaction_count) as avg_txs FROM blocks"
        ],
        "balances": [
            "-- IMPORTANT: When querying the balances dataset, use the 'contract' parameter to specify the address",
            "-- First download the data:",
            "# result = query_dataset('balances', blocks='15M:15.01M', contract='0x1234...', output_format='parquet')",
            "-- Then query the data:",
            "SELECT block_number, address, balance_f64 FROM balances ORDER BY block_number",
            "SELECT block_number, balance_f64, balance_f64/1e18 as balance_eth FROM balances ORDER BY block_number"
        ],
        "logs": [
            "SELECT * FROM logs LIMIT 10",
            "SELECT address, COUNT(*) as event_count FROM logs GROUP BY address ORDER BY event_count DESC LIMIT 10",
            "SELECT topic0, COUNT(*) as event_count FROM logs GROUP BY topic0 ORDER BY event_count DESC LIMIT 10"
        ],
        "joins": [
            "-- Join with simple table references",
            "SELECT t.block_number, COUNT(*) as tx_count, b.gas_used FROM transactions t JOIN blocks b ON t.block_number = b.block_number GROUP BY t.block_number, b.gas_used ORDER BY tx_count DESC LIMIT 10",
            
            "-- Join with read_parquet (useful for complex joins)",
            "SELECT l.block_number, l.address, COUNT(*) as log_count FROM read_parquet('/path/to/logs.parquet') l GROUP BY l.block_number, l.address ORDER BY log_count DESC LIMIT 10"
        ],
        "workflow_examples": [
            "-- Step 1: Download data with query_dataset",
            "# result = query_dataset(dataset='blocks', blocks='15000000:15000100', output_format='parquet')",
            "-- Step 2: Get schema info",
            "# schema = get_sql_table_schema(result['files'][0])",
            "-- Step 3: Run SQL query (simple table reference)",
            "# query_sql(query=\"SELECT * FROM blocks LIMIT 10\", files=result.get('files', []))",
            "",
            "-- Or use the combined function",
            "# query_blockchain_sql(sql_query=\"SELECT * FROM blocks LIMIT 10\", dataset='blocks', blocks='15000000:15000100')"
        ],
        "using_dataset_parameters": [
            "-- IMPORTANT: How to check required parameters for datasets",
            "-- Step 1: Look up the dataset to see required parameters",
            "# dataset_info = lookup_dataset('balances')",
            "# This will show: 'required parameters: address'",
            "",
            "-- Step 2: Use the contract parameter for ANY address parameter",
            "# For balances dataset, query_dataset('balances', blocks='1M:1.1M', contract='0x1234...')",
            "# For erc20_transfers, query_dataset('erc20_transfers', blocks='1M:1.1M', contract='0x1234...')",
            "",
            "-- Step 3: Always check the dataset description and schema before querying new datasets",
            "# This helps ensure you're passing the correct parameters"
        ]
    }

def parse_args(args=None):
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Cryo Data Server")
    parser.add_argument(
        "--rpc-url", 
        type=str, 
        help="Ethereum RPC URL to use for requests"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        help="Directory to store downloaded data, defaults to ~/.cryo-mcp/data/"
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Show version information and exit"
    )
    return parser.parse_args(args)

def main():
    """Main entry point for the command-line script"""
    args = parse_args()
    
    # Check if version was requested
    if args.version:
        from cryo_mcp import __version__
        print(f"cryo-mcp version {__version__}")
        return 0
    
    # Set RPC URL with priority: command line > environment variable > default
    if args.rpc_url:
        rpc_url = args.rpc_url
        os.environ["ETH_RPC_URL"] = rpc_url
        print(f"Using RPC URL from command line: {rpc_url}")
    elif os.environ.get("ETH_RPC_URL"):
        rpc_url = os.environ["ETH_RPC_URL"]
        print(f"Using RPC URL from environment: {rpc_url}")
    else:
        rpc_url = DEFAULT_RPC_URL
        os.environ["ETH_RPC_URL"] = rpc_url
        print(f"Using default RPC URL: {rpc_url}")
    
    # Set data directory with priority: command line > environment variable > default
    if args.data_dir:
        data_dir = args.data_dir
        os.environ["CRYO_DATA_DIR"] = data_dir
        print(f"Using data directory from command line: {data_dir}")
    elif os.environ.get("CRYO_DATA_DIR"):
        data_dir = os.environ["CRYO_DATA_DIR"]
        print(f"Using data directory from environment: {data_dir}")
    else:
        data_dir = DEFAULT_DATA_DIR
        os.environ["CRYO_DATA_DIR"] = data_dir
        print(f"Using default data directory: {data_dir}")
    
    # Ensure data directory exists
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    
    mcp.run()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
