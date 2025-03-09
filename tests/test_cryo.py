#!/usr/bin/env python3
import json
import os
import subprocess
from pathlib import Path

# Set ETH_RPC_URL
os.environ["ETH_RPC_URL"] = "http://10.0.0.48:8545"
print(f"Using ETH_RPC_URL: {os.environ['ETH_RPC_URL']}")

def test_cryo_cli():
    """Test direct CLI command to verify it works"""
    dataset = "blocks"
    cmd = ["cryo", dataset, "-b", "1000:1005", "-r", "http://10.0.0.48:8545", "--json"]
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print(f"Return code: {result.returncode}")
    print(f"STDOUT: {result.stdout[:500]}...")
    print(f"STDERR: {result.stderr[:500]}...")
    
    if result.returncode != 0:
        print("CLI command failed")
        return False
    
    return True

def test_cryo_with_output():
    """Test with output directory as we do in the server"""
    dataset = "blocks"
    temp_dir = Path("/tmp/cryo_test")
    temp_dir.mkdir(exist_ok=True)
    
    cmd = ["cryo", dataset, "-b", "1000:1005", "-r", "http://10.0.0.48:8545", "--json", "-o", str(temp_dir)]
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print(f"Return code: {result.returncode}")
    print(f"STDOUT: {result.stdout[:500]}...")
    print(f"STDERR: {result.stderr[:500]}...")
    
    if result.returncode != 0:
        print("CLI command with output failed")
        return False
    
    # Find the output file
    output_files = list(temp_dir.glob(f"*{dataset}*.json"))
    print(f"Output files: {output_files}")
    
    if not output_files:
        print("No output files found")
        return False
    
    # Read the first file
    with open(output_files[0], 'r') as f:
        data = json.load(f)
        print(f"Data sample: {json.dumps(data[:2], indent=2)}")
    
    return True

if __name__ == "__main__":
    print("=== Testing direct CLI command ===")
    cli_result = test_cryo_cli()
    
    print("\n=== Testing CLI command with output directory ===")
    output_result = test_cryo_with_output()
    
    if cli_result and output_result:
        print("\n✅ All tests passed")
    else:
        print("\n❌ Tests failed")