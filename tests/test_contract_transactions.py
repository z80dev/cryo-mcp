#!/usr/bin/env python3
import json
import os
import subprocess
from pathlib import Path

# Set ETH_RPC_URL
os.environ["ETH_RPC_URL"] = "http://10.0.0.48:8545"
print(f"Using ETH_RPC_URL: {os.environ['ETH_RPC_URL']}")

def test_contract_transactions():
    """Test fetching transactions for a specific contract"""
    
    # Use a known block number
    block_num = 22001067  # You can replace this with any block number you want to test with
    block_range = f"{block_num}:{block_num+1}"
    
    # Use a known contract address (USDC for example)
    contract_address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    
    # Create a temp directory for output
    temp_dir = Path("/tmp/cryo_contract_tx_test")
    temp_dir.mkdir(exist_ok=True)
    
    cmd = [
        "cryo", "transactions", 
        "-b", block_range, 
        "--contract", contract_address,
        "-r", os.environ["ETH_RPC_URL"], 
        "--json", 
        "-o", str(temp_dir)
    ]
    
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
            
            print(f"Number of contract transactions: {len(data)}")
            if data:
                print(f"First transaction hash: {data[0].get('transaction_hash')}")
                print(f"First transaction block number: {data[0].get('block_number')}")
                
                # Verify contract interactions
                for tx in data:
                    if tx.get("to_address") == contract_address:
                        print(f"Found transaction to contract: {tx.get('transaction_hash')}")
                    elif tx.get("from_address") == contract_address:
                        print(f"Found transaction from contract: {tx.get('transaction_hash')}")
            
            return data
    
    return None

if __name__ == "__main__":
    test_contract_transactions()