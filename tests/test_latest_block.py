#!/usr/bin/env python3
import json
import os
import subprocess
import requests
from pathlib import Path

# Set ETH_RPC_URL
RPC_URL = "http://10.0.0.48:8545"
os.environ["ETH_RPC_URL"] = RPC_URL
print(f"Using ETH_RPC_URL: {os.environ['ETH_RPC_URL']}")

def get_latest_block_number():
    """Get the latest block number from the Ethereum node"""
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    }
    
    try:
        response = requests.post(RPC_URL, json=payload)
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

def test_blocks_range():
    """Test querying a specific block range"""
    # Use fixed block range for testing
    start_block = 22005903
    end_block = 22005908
    
    block_range = f"{start_block}:{end_block}"
    
    cmd = ["cryo", "blocks", "-b", block_range, "-r", RPC_URL, "--json"]
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print(f"Return code: {result.returncode}")
    print(f"STDOUT: {result.stdout[:500]}...")
    
    if result.returncode != 0:
        print(f"STDERR: {result.stderr}")
        assert False, "Command failed"
    
    assert True

def test_latest_blocks():
    """Test getting the latest blocks"""
    latest_block = get_latest_block_number()
    
    if latest_block is None:
        print("Failed to get the latest block number")
        assert False, "Failed to get the latest block number"
    
    # Test getting the latest 5 blocks
    start_block = latest_block - 5
    print(f"Fetching blocks from {start_block} to {latest_block}")
    
    # Direct implementation rather than calling test_blocks_range
    block_range = f"{start_block}:{latest_block+1}"  # Add 1 to make it inclusive
    
    cmd = ["cryo", "blocks", "-b", block_range, "-r", RPC_URL, "--json"]
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print(f"Return code: {result.returncode}")
    print(f"STDOUT: {result.stdout[:500]}...")
    
    if result.returncode != 0:
        print(f"STDERR: {result.stderr}")
        assert False, "Failed to fetch the latest blocks"
    
    assert True

if __name__ == "__main__":
    test_latest_blocks()