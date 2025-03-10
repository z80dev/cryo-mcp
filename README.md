# Cryo MCP 🧊

A Model Completion Protocol (MCP) server for the [Cryo](https://github.com/paradigmxyz/cryo) blockchain data extraction tool. 

Cryo MCP allows you to access Cryo's powerful blockchain data extraction capabilities via an API server that implements the MCP protocol, making it easy to query blockchain data from any MCP-compatible client.

## For LLM Users: SQL Query Workflow Guide

When using this MCP server to run SQL queries on blockchain data, follow this workflow:

1. **Download data** with `query_dataset`:
   ```python
   result = query_dataset(
       dataset="blocks",  # or "transactions", "logs", etc.
       blocks="15000000:15001000",  # or use blocks_from_latest=100
       output_format="parquet"  # important: use parquet for SQL
   )
   files = result.get("files", [])  # Get the returned file paths
   ```

2. **Explore schema** with `get_sql_table_schema`:
   ```python
   # Check what columns are available in the file
   schema = get_sql_table_schema(files[0])
   # Now you can see all columns, data types, and sample data
   ```

3. **Run SQL** with `query_sql`:
   ```python
   # Option 1: Simple table reference (DuckDB will match the table name to file)
   sql_result = query_sql(
       query="SELECT block_number, timestamp, gas_used FROM blocks",
       files=files  # Pass the files from step 1
   )
   
   # Option 2: Using read_parquet() with explicit file path
   sql_result = query_sql(
       query=f"SELECT block_number, timestamp, gas_used FROM read_parquet('{files[0]}')",
       files=files  # Pass the files from step 1
   )
   ```

Alternatively, use the combined approach with `query_blockchain_sql`:
```python
# Option 1: Simple table reference
result = query_blockchain_sql(
    sql_query="SELECT * FROM blocks",
    dataset="blocks",
    blocks_from_latest=100
)

# Option 2: Using read_parquet()
result = query_blockchain_sql(
    sql_query="SELECT * FROM read_parquet('/path/to/file.parquet')",  # Path doesn't matter
    dataset="blocks",
    blocks_from_latest=100
)
```

For a complete working example, see [examples/sql_workflow_example.py](examples/sql_workflow_example.py).

## Features

- **Full Cryo Dataset Access**: Query any Cryo dataset through an API server
- **MCP Integration**: Works seamlessly with MCP clients
- **Flexible Query Options**: Support for all major Cryo filtering and output options
- **Block Range Options**: Query specific blocks, latest block, or relative ranges
- **Contract Filtering**: Filter data by contract address 
- **Latest Block Access**: Easy access to the latest Ethereum block data
- **Multiple Output Formats**: JSON, CSV, and Parquet support
- **Schema Information**: Get detailed dataset schemas and sample data
- **SQL Queries**: Run SQL queries directly against downloaded blockchain data

## Installation (Optional)

This is not required if you will run the tool with `uvx` directly.

```bash
# install with UV (recommended)
uv tool install cryo-mcp
```

## Requirements

- Python 3.8+
- uv
- A working installation of [Cryo](https://github.com/paradigmxyz/cryo)
- Access to an Ethereum RPC endpoint
- DuckDB (for SQL query functionality)

## Quick Start

### Usage with Claude Code

1. Run `claude mcp add` for an interactive prompt.
2. Enter `uvx` as the command to run.
3. Enter `cryo-mcp --rpc-url <ETH_RPC_URL> [--data-dir <DATA_DIR>]` as the args
4. Alternatively, provide `ETH_RPC_URL` and `CRYO_DATA_DIR` as environment variables instead.

New instances of `claude` will now have access to cryo as configured to hit your RPC endpoint and store data in the specified directory.

## Available Tools

Cryo MCP exposes the following MCP tools:

### `list_datasets()`

Returns a list of all available Cryo datasets.

Example:
```python
client.list_datasets()
```

### `query_dataset()`

Query a Cryo dataset with various filtering options.

Parameters:
- `dataset` (str): The name of the dataset to query (e.g., 'blocks', 'transactions', 'logs')
- `blocks` (str, optional): Block range specification (e.g., '1000:1010')
- `start_block` (int, optional): Start block number (alternative to blocks)
- `end_block` (int, optional): End block number (alternative to blocks)
- `use_latest` (bool, optional): If True, query the latest block
- `blocks_from_latest` (int, optional): Number of blocks from latest to include
- `contract` (str, optional): Contract address to filter by
- `output_format` (str, optional): Output format ('json', 'csv', 'parquet')
- `include_columns` (list, optional): Columns to include alongside defaults
- `exclude_columns` (list, optional): Columns to exclude from defaults

Example:
```python
# Get transactions from blocks 15M to 15.01M
client.query_dataset('transactions', blocks='15M:15.01M')

# Get logs for a specific contract from the latest 100 blocks
client.query_dataset('logs', blocks_from_latest=100, contract='0x1234...')

# Get just the latest block
client.query_dataset('blocks', use_latest=True)
```

### `lookup_dataset()`

Get detailed information about a specific dataset, including schema and sample data.

Parameters:
- `name` (str): The name of the dataset to look up
- `sample_start_block` (int, optional): Start block for sample data
- `sample_end_block` (int, optional): End block for sample data
- `use_latest_sample` (bool, optional): Use latest block for sample
- `sample_blocks_from_latest` (int, optional): Number of blocks from latest for sample

Example:
```python
client.lookup_dataset('logs')
```

### `get_latest_ethereum_block()`

Returns information about the latest Ethereum block.

Example:
```python
client.get_latest_ethereum_block()
```

### SQL Query Tools

Cryo MCP includes several tools for running SQL queries against blockchain data:

### `query_sql()`

Run a SQL query against downloaded blockchain data.

Parameters:
- `query` (str): SQL query to execute
- `files` (list, optional): List of parquet file paths to query. If None, will use all files in the data directory.
- `include_schema` (bool, optional): Whether to include schema information in the result

Example:
```python
# Run against all available files
client.query_sql("SELECT * FROM read_parquet('/path/to/blocks.parquet') LIMIT 10")

# Run against specific files
client.query_sql(
    "SELECT * FROM read_parquet('/path/to/blocks.parquet') LIMIT 10",
    files=['/path/to/blocks.parquet']
)
```

### `query_blockchain_sql()`

Query blockchain data using SQL, automatically downloading any required data.

Parameters:
- `sql_query` (str): SQL query to execute
- `dataset` (str, optional): The dataset to query (e.g., 'blocks', 'transactions')
- `blocks` (str, optional): Block range specification
- `start_block` (int, optional): Start block number
- `end_block` (int, optional): End block number
- `use_latest` (bool, optional): If True, query the latest block
- `blocks_from_latest` (int, optional): Number of blocks before the latest to include
- `contract` (str, optional): Contract address to filter by
- `force_refresh` (bool, optional): Force download of new data even if it exists
- `include_schema` (bool, optional): Include schema information in the result

Example:
```python
# Automatically downloads blocks data if needed, then runs the SQL query
client.query_blockchain_sql(
    sql_query="SELECT block_number, gas_used, timestamp FROM blocks ORDER BY gas_used DESC LIMIT 10",
    dataset="blocks",
    blocks_from_latest=100
)
```

### `list_available_sql_tables()`

List all available tables that can be queried with SQL.

Example:
```python
client.list_available_sql_tables()
```

### `get_sql_table_schema()`

Get the schema for a specific parquet file.

Parameters:
- `file_path` (str): Path to the parquet file

Example:
```python
client.get_sql_table_schema("/path/to/blocks.parquet")
```

### `get_sql_examples()`

Get example SQL queries for different blockchain datasets.

Example:
```python
client.get_sql_examples()
```

## Configuration Options

When starting the Cryo MCP server, you can use these command-line options:

- `--rpc-url URL`: Ethereum RPC URL (overrides ETH_RPC_URL environment variable)
- `--data-dir PATH`: Directory to store downloaded data (overrides CRYO_DATA_DIR environment variable, defaults to ~/.cryo-mcp/data/)

## Environment Variables

- `ETH_RPC_URL`: Default Ethereum RPC URL to use when not specified via command line
- `CRYO_DATA_DIR`: Default directory to store downloaded data when not specified via command line

## Advanced Usage

### SQL Queries Against Blockchain Data

Cryo MCP allows you to run powerful SQL queries against blockchain data, combining the flexibility of SQL with Cryo's data extraction capabilities:

#### Two-Step SQL Query Flow

You can split data extraction and querying into two separate steps:

```python
# Step 1: Download data and get file paths
download_result = client.query_dataset(
    dataset="transactions",
    blocks_from_latest=1000,
    output_format="parquet"
)

# Step 2: Use the file paths to run SQL queries
file_paths = download_result.get("files", [])
client.query_sql(
    query=f"""
    SELECT 
        to_address as contract_address, 
        COUNT(*) as tx_count,
        SUM(gas_used) as total_gas,
        AVG(gas_used) as avg_gas
    FROM read_parquet('{file_paths[0]}')
    WHERE to_address IS NOT NULL
    GROUP BY to_address
    ORDER BY total_gas DESC
    LIMIT 20
    """,
    files=file_paths
)
```

#### Combined SQL Query Flow

For convenience, you can also use the combined function that handles both steps:

```python
# Get top gas-consuming contracts
client.query_blockchain_sql(
    sql_query="""
    SELECT 
        to_address as contract_address, 
        COUNT(*) as tx_count,
        SUM(gas_used) as total_gas,
        AVG(gas_used) as avg_gas
    FROM read_parquet('/path/to/transactions.parquet')
    WHERE to_address IS NOT NULL
    GROUP BY to_address
    ORDER BY total_gas DESC
    LIMIT 20
    """,
    dataset="transactions",
    blocks_from_latest=1000
)

# Find blocks with the most transactions
client.query_blockchain_sql(
    sql_query="""
    SELECT 
        block_number, 
        COUNT(*) as tx_count
    FROM read_parquet('/path/to/transactions.parquet')
    GROUP BY block_number
    ORDER BY tx_count DESC
    LIMIT 10
    """,
    dataset="transactions",
    blocks="15M:16M"
)

# Analyze event logs by topic
client.query_blockchain_sql(
    sql_query="""
    SELECT 
        topic0, 
        COUNT(*) as event_count
    FROM read_parquet('/path/to/logs.parquet')
    GROUP BY topic0
    ORDER BY event_count DESC
    LIMIT 20
    """,
    dataset="logs",
    blocks_from_latest=100
)
```

**Note**: For SQL queries, always use `output_format="parquet"` when downloading data to ensure optimal performance with DuckDB. When using `query_blockchain_sql`, you should refer to the file paths directly in your SQL using the `read_parquet()` function.

### Querying with Block Ranges

Cryo MCP supports the full range of Cryo's block specification syntax:

```python
# Using block numbers
client.query_dataset('transactions', blocks='15000000:15001000')

# Using K/M notation
client.query_dataset('logs', blocks='15M:15.01M')

# Using offsets from latest 
client.query_dataset('blocks', blocks_from_latest=100)
```

### Contract Filtering

Filter logs and other data by contract address:

```python
# Get all logs for USDC contract
client.query_dataset('logs', 
                    blocks='16M:16.1M', 
                    contract='0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')
```

### Column Selection

Include only the columns you need:

```python
# Get just block numbers and timestamps
client.query_dataset('blocks', 
                    blocks='16M:16.1M', 
                    include_columns=['number', 'timestamp'])
```

## Development

### Project Structure

```
cryo-mcp/
├── cryo_mcp/           # Main package directory
│   ├── __init__.py     # Package initialization
│   ├── server.py       # Main MCP server implementation
│   ├── sql.py          # SQL query functionality
├── tests/              # Test directory
│   ├── test_*.py       # Test files
├── pyproject.toml      # Project configuration
├── README.md           # Project documentation
```

### Run Tests

`uv run pytest`

## License

MIT

## Credits

- Built on top of the amazing [Cryo](https://github.com/paradigmxyz/cryo) tool by Paradigm
- Uses the [MCP protocol](https://github.com/mcp-team/mcp) for API communication
