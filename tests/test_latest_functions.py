#!/usr/bin/env python3
import json
import os
import subprocess
import requests
from pathlib import Path

# Define the function directly in the test script
def get_latest_block_number():
    """Get the latest block number from the Ethereum node"""
    rpc_url = os.environ.get("ETH_RPC_URL", "http://10.0.0.48:8545")
    
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
            print(f"Error: {response_data.get('error', 'Unknown error')}")
            return None
    except Exception as e:
        print(f"Exception when fetching latest block: {e}")
        return None

def test_latest_block_functions():
    """Test the direct latest block functions"""
    
    print("=== Testing get_latest_block_number() ===")
    latest_block = get_latest_block_number()
    print(f"Latest block number: {latest_block}")
    
    print("\n=== Testing get_latest_ethereum_block with cryo ===")
    
    # Test getting the latest block using cryo directly
    if latest_block:
        rpc_url = os.environ.get("ETH_RPC_URL", "http://10.0.0.48:8545")
        block_range = f"{latest_block}:{latest_block+1}"
        
        temp_dir = Path("/tmp/cryo_latest_test")
        temp_dir.mkdir(exist_ok=True)
        
        cmd = ["cryo", "blocks", "-b", block_range, "-r", rpc_url, "--json", "-o", str(temp_dir)]
        
        print(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            return False
        
        # Find the output file
        output_files = list(temp_dir.glob("*blocks*.json"))
        
        if not output_files:
            print("No output files found")
            return False
        
        # Read the block data
        with open(output_files[0], 'r') as f:
            data = json.load(f)
            if data and len(data) > 0:
                print(f"Block data: {json.dumps(data[0], indent=2)}")
                return True
    
    return False

def test_query_latest_blocks():
    """Test querying the latest blocks using subprocess"""
    
    # Get the latest block number
    latest_block = get_latest_block_number()
    if latest_block is None:
        print("Failed to get latest block number")
        return False
    
    # Test getting a range of latest blocks
    start_block = latest_block - 5
    end_block = latest_block
    
    # Create a block range string
    block_range = f"{start_block}:{end_block+1}"  # Add 1 to make it inclusive
    
    # Use cryo directly
    rpc_url = os.environ.get("ETH_RPC_URL", "http://10.0.0.48:8545")
    temp_dir = Path("/tmp/cryo_test_latest")
    temp_dir.mkdir(exist_ok=True)
    
    cmd = [
        "cryo", "blocks", 
        "-b", block_range,
        "-r", rpc_url,
        "--json", 
        "-o", str(temp_dir)
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    
    # Find the output file
    output_files = list(temp_dir.glob("*blocks*.json"))
    
    if not output_files:
        print("No output files found")
        return False
    
    # Read the first file
    with open(output_files[0], 'r') as f:
        data = json.load(f)
        print(f"Found {len(data)} blocks")
        
        # Check if we got the range we expected
        block_numbers = [block["block_number"] for block in data]
        print(f"Block numbers: {block_numbers}")
        
        # Check the range covers what we requested (inclusive start to end)
        expected_blocks = list(range(start_block, end_block + 1))
        actual_blocks = sorted(block_numbers)
        
        print(f"Expected blocks: {expected_blocks}")
        print(f"Actual blocks: {actual_blocks}")
        
        return set(expected_blocks) == set(actual_blocks)

if __name__ == "__main__":
    print("Testing latest block functions")
    
    # Test direct functions
    functions_success = test_latest_block_functions()
    
    # Test querying latest blocks
    query_success = test_query_latest_blocks()
    
    if functions_success and query_success:
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Tests failed")
        if not functions_success:
            print("- Latest block functions test failed")
        if not query_success:
            print("- Query latest blocks test failed")