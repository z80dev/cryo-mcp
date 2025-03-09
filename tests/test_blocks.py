#!/usr/bin/env python3
import json
import os
import subprocess
from pathlib import Path

# Set ETH_RPC_URL
os.environ["ETH_RPC_URL"] = "http://10.0.0.48:8545"
print(f"Using ETH_RPC_URL: {os.environ['ETH_RPC_URL']}")

def test_integer_blocks():
    """Test using integer blocks with cryo directly"""
    
    # Convert integer block range to string
    start_block = 1000
    end_block = 1005
    block_range = f"{start_block}:{end_block}"
    
    cmd = ["cryo", "blocks", "-b", block_range, "-r", "http://10.0.0.48:8545", "--json"]
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print(f"Return code: {result.returncode}")
    print(f"STDOUT: {result.stdout[:500]}...")
    
    # Now run the equivalent using our server's string conversion logic
    temp_dir = Path("/tmp/cryo_int_test")
    temp_dir.mkdir(exist_ok=True)
    
    cmd = ["cryo", "blocks", "-b", block_range, "-r", "http://10.0.0.48:8545", "--json", "-o", str(temp_dir)]
    
    print(f"\nRunning output command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print(f"Return code: {result.returncode}")
    print(f"STDOUT: {result.stdout[:500]}...")
    
    # Find and read the output file
    output_files = list(temp_dir.glob("*blocks*.json"))
    print(f"Output files: {output_files}")
    
    if output_files:
        with open(output_files[0], 'r') as f:
            data = json.load(f)
            print(f"Number of records: {len(data)}")
            print(f"First block number: {data[0]['block_number']}")
            print(f"Last block number: {data[-1]['block_number']}")
            
            # Verify that we got the block range we asked for
            # Note: cryo seems to use start:end as [start, end) (inclusive start, exclusive end)
            expected_blocks = list(range(start_block, end_block))
            actual_blocks = [block["block_number"] for block in data]
            
            print(f"Expected blocks: {expected_blocks}")
            print(f"Actual blocks: {actual_blocks}")
            
            if sorted(actual_blocks) == sorted(expected_blocks):
                print("✅ Block ranges match!")
            else:
                print("❌ Block ranges do not match!")
    
if __name__ == "__main__":
    test_integer_blocks()