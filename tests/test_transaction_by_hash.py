#!/usr/bin/env python3
import os
import sys
from cryo_mcp.server import get_transaction_by_hash

# Set ETH_RPC_URL
os.environ["ETH_RPC_URL"] = "http://10.0.0.48:8545"
print(f"Using ETH_RPC_URL: {os.environ['ETH_RPC_URL']}")

def test_get_transaction_by_hash():
    """Test getting transaction details by hash"""
    
    # Test with a known transaction hash from our previous tests
    # You can replace this with any valid transaction hash you want to test with
    tx_hash = "0xbee5a5c9024d9d6dde31c180c71b21aba1ebb7a726cf148a4b2781cf0ca7b7e6"
    
    print(f"Looking up transaction: {tx_hash}")
    tx_info = get_transaction_by_hash(tx_hash)
    
    if "error" in tx_info:
        print(f"❌ Error: {tx_info['error']}")
        return False
    
    # Print transaction details
    print("\nTransaction Details:")
    for key, value in tx_info.items():
        # Skip printing the full input data which can be very long
        if key == "input" and value and len(value) > 100:
            print(f"  {key}: {value[:50]}...{value[-50:]}")
        else:
            print(f"  {key}: {value}")
    
    # Test with an invalid transaction hash
    invalid_hash = "0x1234567890123456789012345678901234567890123456789012345678901234"
    
    print(f"\nLooking up invalid transaction: {invalid_hash}")
    invalid_tx = get_transaction_by_hash(invalid_hash)
    
    if "error" in invalid_tx:
        print(f"✅ Expected error for invalid hash: {invalid_tx['error']}")
    else:
        print(f"❌ Unexpected success for invalid hash: {invalid_tx}")
        return False
    
    return True

if __name__ == "__main__":
    test_get_transaction_by_hash()