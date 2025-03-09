#!/usr/bin/env python3
import json
import os
import subprocess
from pathlib import Path

# Set ETH_RPC_URL
os.environ["ETH_RPC_URL"] = "http://10.0.0.48:8545"
print(f"Using ETH_RPC_URL: {os.environ['ETH_RPC_URL']}")

def test_transactions():
    """Test fetching transactions for a specific block"""
    
    # Use a known block number
    block_num = 22001067  # You can replace this with any block number you want to test with
    block_range = f"{block_num}:{block_num+1}"
    
    # Create a temp directory for output
    temp_dir = Path("/tmp/cryo_tx_test")
    temp_dir.mkdir(exist_ok=True)
    
    cmd = ["cryo", "transactions", "-b", block_range, "-r", os.environ["ETH_RPC_URL"], "--json", "-o", str(temp_dir)]
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print(f"Return code: {result.returncode}")
    print(f"STDOUT: {result.stdout[:500]}...")
    
    # Find and read the output file
    output_files = list(temp_dir.glob("*transactions*.json"))
    print(f"Output files: {output_files}")
    
    if output_files:
        with open(output_files[0], 'r') as f:
            data = json.load(f)
            
            print(f"Number of transactions: {len(data)}")
            if data:
                print(f"First transaction hash: {data[0].get('transaction_hash')}")
                print(f"First transaction block number: {data[0].get('block_number')}")
                
                # Save the first transaction to a file for inspection
                print(f"Saving first transaction to ethereum__blocks_{block_num}_to_{block_num}.json")
                with open(f"ethereum__blocks_{block_num}_to_{block_num}.json", 'w') as outfile:
                    json.dump(data, outfile, indent=2)
            
            return data
    
    return None

if __name__ == "__main__":
    test_transactions()