# Cryo MCP 🧊

A Model Completion Protocol (MCP) server for the [Cryo](https://github.com/paradigmxyz/cryo) blockchain data extraction tool. 

Cryo MCP allows you to access Cryo's powerful blockchain data extraction capabilities via an API server that implements the MCP protocol, making it easy to query blockchain data from any MCP-compatible client.

## Features

- **Full Cryo Dataset Access**: Query any Cryo dataset through an API server
- **MCP Integration**: Works seamlessly with MCP clients
- **Flexible Query Options**: Support for all major Cryo filtering and output options
- **Block Range Options**: Query specific blocks, latest block, or relative ranges
- **Contract Filtering**: Filter data by contract address 
- **Latest Block Access**: Easy access to the latest Ethereum block data
- **Multiple Output Formats**: JSON and CSV support
- **Schema Information**: Get detailed dataset schemas and sample data

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
- `output_format` (str, optional): Output format ('json', 'csv')
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

## Configuration Options

When starting the Cryo MCP server, you can use these command-line options:

- `--rpc-url URL`: Ethereum RPC URL (overrides ETH_RPC_URL environment variable)
- `--data-dir PATH`: Directory to store downloaded data (overrides CRYO_DATA_DIR environment variable, defaults to ~/.cryo-mcp/data/)

## Environment Variables

- `ETH_RPC_URL`: Default Ethereum RPC URL to use when not specified via command line
- `CRYO_DATA_DIR`: Default directory to store downloaded data when not specified via command line

## Advanced Usage

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
