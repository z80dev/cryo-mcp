#!/usr/bin/env python3
import os
import sys
from cryo_mcp.server import (
    get_latest_ethereum_block,
    list_datasets,
    query_dataset,
    lookup_dataset
)

# Set ETH_RPC_URL
os.environ["ETH_RPC_URL"] = "http://10.0.0.48:8545"
print(f"Using ETH_RPC_URL: {os.environ['ETH_RPC_URL']}")

def test_get_latest_block():
    """Test the get_latest_ethereum_block function"""
    print("\n=== Testing get_latest_ethereum_block ===")
    
    block_info = get_latest_ethereum_block()
    print(f"Latest block: {block_info}")
    
    if "error" in block_info:
        print(f"❌ Error getting latest block: {block_info['error']}")
        return False
    
    print(f"✅ Successfully got latest block: {block_info['block_number']}")
    return True

def test_list_datasets():
    """Test the list_datasets function"""
    print("\n=== Testing list_datasets ===")
    
    datasets = list_datasets()
    print(f"Found {len(datasets)} datasets: {', '.join(datasets[:5])}...")
    
    # Check that we have some common datasets
    required_datasets = ["blocks", "transactions", "logs", "balances"]
    missing = [ds for ds in required_datasets if ds not in datasets]
    
    if missing:
        print(f"❌ Missing required datasets: {', '.join(missing)}")
        return False
    
    print(f"✅ Successfully listed {len(datasets)} datasets")
    return True

def test_query_dataset():
    """Test the query_dataset function"""
    print("\n=== Testing query_dataset ===")
    
    # Test transactions with latest block
    result = query_dataset(
        dataset="transactions",
        use_latest=True,
        output_format="json"
    )
    
    if "error" in result:
        print(f"❌ Error querying transactions: {result['error']}")
        return False
    
    data = result.get("data", [])
    print(f"Got {len(data)} transactions from latest block")
    
    if not data:
        print("❌ No transactions returned")
        return False
    
    # Test transactions with block range and contract filter
    contract_address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # USDC
    result = query_dataset(
        dataset="transactions",
        blocks="22001067:22001068",
        contract=contract_address,
        output_format="json"
    )
    
    if "error" in result:
        print(f"❌ Error querying contract transactions: {result['error']}")
        return False
    
    data = result.get("data", [])
    print(f"Got {len(data)} USDC transactions from block 22001067")
    
    contract_txs = [tx for tx in data if tx.get("to_address") == contract_address]
    print(f"Found {len(contract_txs)} transactions to USDC")
    
    print(f"✅ Successfully queried dataset with different parameters")
    return True

def test_lookup_dataset():
    """Test the lookup_dataset function"""
    print("\n=== Testing lookup_dataset ===")
    
    # Look up transactions dataset
    result = lookup_dataset(
        name="transactions",
        use_latest_sample=True
    )
    
    if "schema_error" in result and "sample_error" not in result:
        print(f"❓ Schema error but sample OK: {result['schema_error']}")
    elif "sample_error" in result:
        print(f"❓ Sample error: {result['sample_error']}")
    
    print(f"Dataset info: {result['name']}")
    print(f"Example queries: {result['example_queries']}")
    
    # Check that we got some schema information
    if "schema" in result or "schema_error" in result:
        print("✅ Got schema information (or error)")
    else:
        print("❌ Missing schema information")
        return False
    
    print(f"✅ Successfully looked up dataset information")
    return True

def main():
    """Run all tests"""
    tests = [
        test_get_latest_block,
        test_list_datasets,
        test_query_dataset,
        test_lookup_dataset
    ]
    
    results = []
    for test in tests:
        results.append(test())
    
    print("\n=== Test Summary ===")
    print(f"Passed: {results.count(True)}/{len(results)}")
    
    return 0 if all(results) else 1

if __name__ == "__main__":
    sys.exit(main())