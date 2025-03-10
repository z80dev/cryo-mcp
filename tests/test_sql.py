#!/usr/bin/env python3
import os
import json
import tempfile
from pathlib import Path
import unittest
import subprocess
from unittest.mock import patch, MagicMock
import shutil

# Add parent directory to path to import modules
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cryo_mcp.sql import execute_sql_query, list_available_tables, create_connection, extract_dataset_from_sql
from cryo_mcp.server import query_blockchain_sql, query_dataset

# Constants for test data
TEST_DATA_DIR = Path(__file__).parent / "data"
TEST_BLOCK_RANGE = "1000:1005"  # Use a small block range for testing

class TestSQL(unittest.TestCase):
    """Test cases for SQL functionality"""
    
    @classmethod
    def setUpClass(cls):
        """Setup for all tests - download real blockchain data once"""
        # Create test data directory if it doesn't exist
        TEST_DATA_DIR.mkdir(exist_ok=True)
        
        # Setup ETH_RPC_URL environment variable if not set
        if not os.environ.get("ETH_RPC_URL"):
            os.environ["ETH_RPC_URL"] = "http://localhost:8545"
        
        # We don't need to download data here anymore as we've manually downloaded it 
        # via direct shell command
    
    def setUp(self):
        """Setup for each test"""
        # Create a temporary directory for test data
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)
        
        # Set environment variable for data directory
        os.environ["CRYO_DATA_DIR"] = str(self.data_dir)
        
        # Create latest directory
        self.latest_dir = self.data_dir / "latest"
        self.latest_dir.mkdir(exist_ok=True)
        
        # Copy real parquet files from TEST_DATA_DIR to temp dir if they exist
        self.has_real_data = False
        parquet_files = list(TEST_DATA_DIR.glob("*.parquet"))
        if parquet_files:
            for file in parquet_files:
                shutil.copy(file, self.data_dir)
            self.has_real_data = True
            print(f"Using real blockchain data for tests: {[f.name for f in parquet_files]}")
    
    def tearDown(self):
        """Clean up temporary directory"""
        self.temp_dir.cleanup()

    def create_mock_parquet_file(self, dataset_name, is_latest=False):
        """Create a mock parquet file for testing"""
        # If we already have real data, don't need to create mock data
        if self.has_real_data and dataset_name == "blocks":
            return next(self.data_dir.glob("*blocks*.parquet"))
        
        # Determine the directory based on whether it's a latest file
        directory = self.latest_dir if is_latest else self.data_dir
        
        # Create a mock parquet file (doesn't need to be a real parquet file for our tests)
        file_path = directory / f"{dataset_name}__00001000_to_00001010.parquet"
        with open(file_path, 'w') as f:
            f.write("mock parquet data")
        
        return file_path

    @patch('cryo_mcp.sql.duckdb.connect')
    def test_extract_dataset_from_sql(self, mock_connect):
        """Test extracting dataset names from SQL queries"""
        test_cases = [
            {"query": "SELECT * FROM blocks LIMIT 10", "expected": "blocks"},
            {"query": "SELECT block_number FROM transactions WHERE value > 0", "expected": "transactions"},
            {"query": "SELECT logs.address FROM logs", "expected": "logs"},
            {"query": "SELECT t.hash FROM transactions t JOIN blocks b", "expected": "transactions"},
            {"query": "SELECT * FROM WHERE x = 1", "expected": None},  # Invalid SQL
            {"query": "SELECT * FROM", "expected": None},  # Invalid SQL
        ]
        
        for case in test_cases:
            result = extract_dataset_from_sql(case["query"])
            self.assertEqual(result, case["expected"], f"Failed for query: {case['query']}")

    def test_list_available_tables(self):
        """Test listing available tables with real data"""
        # If we don't have real data, we need to create mock files
        if not self.has_real_data:
            self.create_mock_parquet_file("blocks")
            self.create_mock_parquet_file("transactions", is_latest=True)
        else:
            # With real data, we should already have a blocks table
            pass
            
        # Get tables
        tables = list_available_tables()
        
        # Check that we have at least one table
        self.assertTrue(len(tables) > 0, "Should find at least one table")
        
        # With real data, verify that our known table is found
        if self.has_real_data:
            # There should be at least one table with 'ethereum' in the name
            ethereum_tables = [table for table in tables if 'ethereum' in table["path"]]
            self.assertTrue(len(ethereum_tables) > 0, "Should find ethereum tables")

    @patch('cryo_mcp.server.query_dataset')
    @patch('cryo_mcp.sql.execute_sql_query')
    def test_query_blockchain_sql(self, mock_execute_sql, mock_query_dataset):
        """Test the combined blockchain SQL query function"""
        # Mock query_dataset to return a successful result
        mock_query_dataset.return_value = {
            "files": ["/path/to/blocks__1000_to_1010.parquet"],
            "count": 1,
            "format": "parquet"
        }
        
        # Mock execute_sql_query to return a successful result
        mock_execute_sql.return_value = {
            "success": True,
            "result": [{"block_number": 1000, "gas_used": 1000000}],
            "row_count": 1,
            "schema": {"columns": ["block_number", "gas_used"]},
            "files_used": ["/path/to/blocks__1000_to_1010.parquet"]
        }
        
        # Call query_blockchain_sql
        result = query_blockchain_sql(
            sql_query="SELECT block_number, gas_used FROM '/path/to/blocks__1000_to_1010.parquet' LIMIT 1",
            dataset="blocks",
            blocks="1000:1010"
        )
        
        # Check results
        self.assertTrue(result["success"], "Query should succeed")
        
        # Verify that query_dataset was called with correct parameters
        mock_query_dataset.assert_called_once_with(
            dataset="blocks",
            blocks="1000:1010",
            start_block=None,
            end_block=None,
            use_latest=False,
            blocks_from_latest=None,
            contract=None,
            output_format="parquet"
        )
        
        # Verify that execute_sql_query was called with correct parameters
        mock_execute_sql.assert_called_once_with(
            "SELECT block_number, gas_used FROM '/path/to/blocks__1000_to_1010.parquet' LIMIT 1",
            ["/path/to/blocks__1000_to_1010.parquet"],  # files parameter
            True  # include_schema parameter
        )

    def test_execute_sql_query_with_nonexistent_file(self):
        """Test executing SQL query with a nonexistent file"""
        # Call execute_sql_query with a file that doesn't exist
        result = execute_sql_query(
            "SELECT * FROM '/nonexistent/file.parquet' LIMIT 1", 
            files=['/nonexistent/file.parquet']
        )
        
        # Print debug info
        print("Nonexistent file query result:", result)
        
        # Check results
        self.assertFalse(result["success"], "Query should fail with nonexistent file")
        self.assertIn("error", result, "Should return an error message")

    def test_execute_sql_query_with_real_data(self):
        """Test executing SQL query with real blockchain data"""
        # Skip this test if we don't have real data
        if not self.has_real_data:
            self.skipTest("No real blockchain data available")
        
        # Find parquet files to use for testing
        parquet_files = list(self.data_dir.glob("*.parquet"))
        if not parquet_files:
            self.skipTest("No parquet files found for testing")
        
        # Get file paths as strings for the test
        file_paths = [str(f) for f in parquet_files]
        
        # Part 1: Test direct file reference
        result = execute_sql_query(
            f"SELECT * FROM '{file_paths[0]}' LIMIT 3",
            files=file_paths
        )
        
        # Print some debug info to see what's happening
        print("Result:", result)
        
        # Check if we have an error
        if not result.get("success", False) and "error" in result:
            print("SQL error:", result["error"])
            
            # Inspect the parquet file to make sure it's valid
            for file in parquet_files:
                print(f"Parquet file details: {file}")
                print(f"File size: {file.stat().st_size} bytes")
                
            try:
                # Try to read the parquet file directly
                from cryo_mcp.sql import create_connection
                conn = create_connection()
                conn.execute(f"SELECT * FROM '{file_paths[0]}' LIMIT 1")
                print("Direct parquet read test succeeded")
            except Exception as e:
                print(f"Direct parquet read test failed: {e}")
        
        # Check results
        self.assertTrue(result.get("success", False), "Query should succeed")
        self.assertEqual(result["row_count"], 3, "Should return 3 rows")
        self.assertEqual(len(result["files_used"]), len(file_paths), "Should track all files")
        
        # Verify that we got real data with expected columns
        self.assertIn("schema", result, "Should include schema")
        self.assertIn("columns", result["schema"], "Should include columns in schema")
        
        # Verify we can run a more complex query directly on the file
        complex_result = execute_sql_query(
            f"""
            SELECT 
                MIN(block_number) as min_block,
                MAX(block_number) as max_block,
                AVG(gas_used) as avg_gas
            FROM '{file_paths[0]}'
            """,
            files=file_paths
        )

        print("Complex result:", complex_result)
        self.assertTrue(complex_result["success"], "Complex query should succeed")
        self.assertEqual(complex_result["row_count"], 1, "Should return 1 summary row")
        self.assertIn("min_block", complex_result["result"][0], "Should have min_block column")
        self.assertIn("max_block", complex_result["result"][0], "Should have max_block column")
        
        # Part 2: Test table name with multiple files (if we have more than one file)
        if len(parquet_files) > 1:
            # Create a duplicate file to ensure we have multiple files
            duplicate_file = self.data_dir / f"{parquet_files[0].stem}_copy.parquet"
            shutil.copy(parquet_files[0], duplicate_file)
            
            # Update file paths list to include the duplicate
            file_paths.append(str(duplicate_file))
            
            # Extract dataset name from filename for table reference
            # Example: ethereum__blocks__00001000_to_00001004.parquet -> blocks
            dataset_name = None
            file_name = parquet_files[0].stem
            if "__" in file_name:
                parts = file_name.split("__")
                if len(parts) > 1:
                    dataset_name = parts[1]  # e.g., blocks, transactions
            
            if not dataset_name:
                # Fallback - just use a simple name
                dataset_name = "blocks"
            
            # Run a query using table name (should combine files)
            multi_file_result = execute_sql_query(
                f"SELECT COUNT(*) as total_rows FROM {dataset_name}",
                files=file_paths
            )
            
            print(f"Multi-file result for table '{dataset_name}':", multi_file_result)
            
            # Check that our query was successful
            self.assertTrue(multi_file_result["success"], "Multi-file query should succeed")
            
            # Verify table mappings show multiple files were used
            self.assertIsNotNone(multi_file_result.get("table_mappings"), "Should include table mappings")
            self.assertTrue(
                any(mapping["combined"] for mapping in multi_file_result.get("table_mappings", {}).values()),
                "Should indicate files were combined"
            )

    @patch('duckdb.DuckDBPyConnection')
    @patch('cryo_mcp.sql.duckdb.connect')
    def test_execute_sql_query_with_mock_data(self, mock_connect, mock_connection):
        """Test executing SQL query with mock data (fallback if real data unavailable)"""
        # Skip if we have real data (we'll use the real data test instead)
        if self.has_real_data:
            self.skipTest("Using real data test instead")
            
        # Create mock parquet file
        file_path = self.create_mock_parquet_file("blocks")
        
        # Setup mock connection and cursor
        mock_fetchdf = MagicMock()
        mock_fetchdf.to_dict.return_value = [{"block_number": 1000, "gas_used": 1000000}]
        mock_fetchdf.empty = False
        mock_fetchdf.columns = ["block_number", "gas_used"]
        mock_fetchdf.dtypes = {"block_number": "int64", "gas_used": "int64"}
        
        mock_cursor = MagicMock()
        mock_cursor.fetchdf.return_value = mock_fetchdf
        
        mock_connection_instance = mock_connect.return_value
        mock_connection_instance.execute.return_value = mock_cursor
        
        # Call execute_sql_query with direct file reference
        result = execute_sql_query(
            f"SELECT * FROM '{file_path}'",
            files=[str(file_path)]
        )
        
        # Check connection setup
        mock_connect.assert_called_once()
        
        # Check results
        self.assertTrue(result["success"], "Query should succeed")
        self.assertEqual(result["row_count"], 1, "Should return correct row count")
        self.assertEqual(len(result["files_used"]), 1, "Should track files used")
        self.assertIn(str(file_path), result["files_used"][0], "Should include file path used")


if __name__ == "__main__":
    unittest.main()
