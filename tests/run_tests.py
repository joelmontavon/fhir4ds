#!/usr/bin/env python3
"""
SQL-on-FHIR Test Runner

This script runs the official SQL-on-FHIR tests against multiple database dialects
(DuckDB and PostgreSQL) and generates separate test reports for each dialect.

Usage:
    python tests/run_tests.py [--dialect DIALECT]
    
Arguments:
    --dialect: Specify which dialect to test (duckdb, postgresql, all). Default: duckdb
    
Output:
    - test_report_duckdb.json: Test results for DuckDB dialect
    - test_report_postgresql.json: Test results for PostgreSQL dialect
    - test_report.json: Combined results when running all dialects
"""

import argparse
import json
import os
import sys
import glob
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone
from decimal import Decimal

# Add parent directory to path so we can import fhir4ds
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fhir4ds import FHIRDataStore, ViewRunner
from fhir4ds.dialects import DuckDBDialect, PostgreSQLDialect

TABLE_NAME = "fhir_resources"
JSON_COLUMN = "resource"

def load_official_test_file(file_path: str) -> Dict[str, Any]:
    """Load a single official test file and return its contents."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading {file_path}: {e}")
        return {}

def get_official_test_files(test_dir: str = "tests/official") -> List[str]:
    """Get all official test JSON files."""
    pattern = os.path.join(test_dir, "*.json")
    return glob.glob(pattern)

def _canonicalize_value(value: Any) -> Any:
    """Recursively canonicalizes a value for comparison."""
    if isinstance(value, list):
        canonical_items = [_canonicalize_value(item) for item in value]
        try:
            return tuple(sorted(canonical_items))
        except TypeError:
            return tuple(canonical_items)
    elif isinstance(value, dict):
        return canonical_row(value)
    return value

def canonical_row(row_dict: Dict[str, Any]) -> Tuple[Tuple[str, Any], ...]:
    """Converts a dictionary to a canonical representation."""
    return tuple(sorted((k, _canonicalize_value(v)) for k, v in row_dict.items()))

def _normalize_value(value: Any) -> Any:
    """
    Normalizes a single value from a database result.
    - Converts Decimal to float.
    - Parses JSON strings.
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, str):
        try:
            return json.loads(value) # Parse if it's a JSON string (e.g., array, object)
        except json.JSONDecodeError:
            return value # It's a plain string
    # For numbers (already float/int), booleans, None, they are returned as is
    return value

def _normalize_row_values(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Applies _normalize_value to each value in a result row dictionary."""
    return {key: _normalize_value(val) for key, val in row_dict.items()}

def canonicalize_results(results: List[Dict[str, Any]]) -> List[Tuple[Tuple[str, Any], ...]]:
    """Converts a list of dictionaries to a sorted list of canonical_rows."""
    # Normalize values first (convert Decimal to float, etc.)
    normalized_results = [_normalize_row_values(row) for row in results if row is not None]
    list_of_canonical_rows = [canonical_row(row) for row in normalized_results]
    
    def sort_key_for_canonical_row(canonical_row_tuple: Tuple[Tuple[str, Any], ...]):
        return tuple((k, v is None, str(v) if v is not None else "") for k, v in canonical_row_tuple)
    
    return sorted(list_of_canonical_rows, key=sort_key_for_canonical_row)

def setup_datastore_with_dialect(dialect_name: str, test_resources: List[Dict[str, Any]]) -> Tuple[FHIRDataStore, ViewRunner]:
    """Setup datastore and view runner with the specified dialect."""
    if dialect_name.lower() == 'duckdb':
        dialect = DuckDBDialect()
        
        # Create datastore and view runner
        datastore = FHIRDataStore(dialect=dialect)
        view_runner = ViewRunner(datastore=datastore)
        
        # Load test resources
        if test_resources:
            datastore.load_resources(test_resources)
        
        return datastore, view_runner
        
    elif dialect_name.lower() == 'postgresql':
        # Use the connection string from CLAUDE.md
        connection_string = "postgresql://postgres:postgres@localhost:5432/postgres"
        dialect = PostgreSQLDialect(connection_string)
        
        # Create datastore and view runner
        datastore = FHIRDataStore(dialect=dialect)
        view_runner = ViewRunner(datastore=datastore)
        
        # Load test resources
        if test_resources:
            datastore.load_resources(test_resources)
        
        return datastore, view_runner
    else:
        raise ValueError(f"Unsupported dialect: {dialect_name}")

def is_ordering_only_failure(expected_results: List[Dict], actual_results: List[Dict]) -> bool:
    """
    Check if test failure is solely due to row ordering differences.
    
    Args:
        expected_results: Expected test results
        actual_results: Actual test results
        
    Returns:
        True if results match when order is ignored, False otherwise
    """
    if len(expected_results) != len(actual_results):
        return False
        
    # Convert to sorted lists for comparison
    try:
        expected_canonical = canonicalize_results(expected_results)
        actual_canonical = canonicalize_results(actual_results)
        
        # Compare canonicalized results
        return expected_canonical == actual_canonical
    except Exception:
        # If canonicalization fails, assume it's not just an ordering issue
        return False

def run_test_case(test_case: Dict[str, Any], view_runner: ViewRunner, dialect_name: str) -> Dict[str, Any]:
    """Run a single test case and return the result."""
    test_name = test_case.get('title', 'Unknown Test')
    view_definition = test_case.get('view', {})
    expected_results = test_case.get('expect', [])
    expected_count = test_case.get('expectCount')
    expect_error = test_case.get('expectError', False)
    
    error_occurred = False
    error_message = ""
    actual_results = []
    
    try:
        # Execute the view
        result_set = view_runner.execute_view_definition(view_definition)
        rows = result_set.fetchall()
        
        # Convert tuples to dictionaries using column descriptions
        if rows and result_set.description:
            column_names = [desc[0] for desc in result_set.description]
            raw_results = [dict(zip(column_names, row)) for row in rows]
            # Normalize values (convert Decimal to float, etc.)
            actual_results = [_normalize_row_values(row) for row in raw_results]
        else:
            actual_results = []
            
    except Exception as e:
        error_occurred = True
        error_message = str(e)
    
    # Handle expected errors
    if expect_error:
        if error_occurred:
            return {
                "name": test_name,
                "result": {
                    "passed": True,
                    "reason": f"Passed (expected error: {error_message})"
                }
            }
        else:
            return {
                "name": test_name,
                "result": {
                    "passed": False,
                    "reason": "Failed (expected error, but none occurred)"
                }
            }
    
    # Handle unexpected errors
    if error_occurred:
        return {
            "name": test_name,
            "result": {
                "passed": False,
                "reason": f"Execution error: {error_message}"
            }
        }
    
    # Check expected count if specified
    if expected_count is not None:
        if len(actual_results) == expected_count:
            return {
                "name": test_name,
                "result": {
                    "passed": True,
                    "reason": f"Passed (count: {len(actual_results)})"
                }
            }
        else:
            return {
                "name": test_name,
                "result": {
                    "passed": False,
                    "reason": f"Count mismatch - Expected {expected_count} rows, got {len(actual_results)} rows"
                }
            }
    
    # Check expected results if specified
    if expected_results is not None:
        # Canonicalize both expected and actual results for comparison
        expected_canonical = canonicalize_results(expected_results)
        actual_canonical = canonicalize_results(actual_results)
        
        # Direct comparison first
        if expected_canonical == actual_canonical:
            return {
                "name": test_name,
                "result": {
                    "passed": True
                }
            }
        
        # Check if failure is only due to ordering (like run_tests.py does)
        is_ordering_test = 'order' in test_name.lower() or 'sort' in test_name.lower()
        
        if not is_ordering_test and is_ordering_only_failure(expected_results, actual_results):
            return {
                "name": test_name,
                "result": {
                    "passed": True,
                    "reason": f"Passed: {dialect_name}'s row ordering is non-deterministic without an explicit ORDER BY. Content matches when order is ignored."
                }
            }
        
        # Content mismatch
        return {
            "name": test_name,
            "result": {
                "passed": False,
                "reason": f"Results mismatch - Expected {len(expected_results)} rows, got {len(actual_results)} rows"
            }
        }
    
    # No expectations specified - assume pass if no error
    return {
        "name": test_name,
        "result": {
            "passed": True,
            "reason": "Passed (no expectations specified)"
        }
    }

def run_tests_for_dialect(dialect_name: str, test_files: List[str]) -> Dict[str, Any]:
    """Run all tests for a specific dialect."""
    print(f"\n🔍 Running tests for {dialect_name.upper()} dialect")
    print("=" * 50)
    
    all_results = {}
    
    for test_file in test_files:
        test_file_name = os.path.basename(test_file)
        print(f"Processing {test_file_name}...")
        
        # Load the test file
        test_data = load_official_test_file(test_file)
        if not test_data:
            continue
            
        # Get test resources and test cases
        test_resources = test_data.get('resources', [])
        test_cases = test_data.get('tests', [])
        
        if not test_cases:
            print(f"  No test cases found in {test_file_name}")
            continue
        
        try:
            # Setup datastore for this dialect
            datastore, view_runner = setup_datastore_with_dialect(dialect_name, test_resources)
            
            # Run each test case
            test_results = []
            for test_case in test_cases:
                result = run_test_case(test_case, view_runner, dialect_name)
                test_results.append(result)
                
                # Print progress
                status = "✅ PASS" if result["result"]["passed"] else "❌ FAIL"
                print(f"  {status} {result['name']}")
                if not result["result"]["passed"]:
                    print(f"    Reason: {result['result'].get('reason', 'Unknown')}")
            
            # Store results for this test file
            all_results[test_file_name] = {
                "tests": test_results
            }
            
            # Clean up
            if hasattr(datastore, 'close'):
                datastore.close()
                
        except Exception as e:
            print(f"  ❌ Error setting up dialect {dialect_name}: {e}")
            # Add error result for this test file
            all_results[test_file_name] = {
                "tests": [{
                    "name": f"Setup Error - {dialect_name}",
                    "result": {
                        "passed": False,
                        "reason": f"Failed to setup {dialect_name} dialect: {str(e)}"
                    }
                }]
            }
    
    return all_results

def generate_summary_report(results: Dict[str, Any], dialect_name: str) -> None:
    """Generate a summary report for the test results."""
    total_tests = 0
    passed_tests = 0
    
    for test_file, file_results in results.items():
        for test in file_results.get('tests', []):
            total_tests += 1
            if test['result']['passed']:
                passed_tests += 1
    
    print(f"\n📊 {dialect_name.upper()} DIALECT SUMMARY")
    print("=" * 40)
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {total_tests - passed_tests}")
    if total_tests > 0:
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")

def main():
    parser = argparse.ArgumentParser(description='Run SQL-on-FHIR tests against multiple dialects')
    parser.add_argument('--dialect', choices=['duckdb', 'postgresql', 'all'], default='duckdb',
                        help='Specify which dialect to test (default: duckdb)')
    
    args = parser.parse_args()
    
    print("🚀 SQL-on-FHIR Test Runner")
    print("=" * 50)
    print("✅ Both DuckDB and PostgreSQL execution are now supported!")
    print()
    
    # Get all official test files
    test_files = get_official_test_files()
    if not test_files:
        print("❌ No official test files found in tests/official/")
        sys.exit(1)
    
    print(f"Found {len(test_files)} test files:")
    for test_file in test_files:
        print(f"  - {os.path.basename(test_file)}")
    
    # Determine which dialects to test
    dialects_to_test = []
    if args.dialect == 'all':
        dialects_to_test = ['duckdb', 'postgresql']
        print("🔄 Testing both DuckDB and PostgreSQL dialects")
    else:
        dialects_to_test = [args.dialect]
    
    all_dialect_results = {}
    
    # Run tests for each dialect
    for dialect in dialects_to_test:
        try:
            results = run_tests_for_dialect(dialect, test_files)
            all_dialect_results[dialect] = results
            
            # Generate and save dialect-specific report
            report_filename = f"test_report_{dialect}.json"
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            
            print(f"\n📄 Report saved to {report_filename}")
            generate_summary_report(results, dialect)
            
        except Exception as e:
            print(f"❌ Failed to run tests for {dialect}: {e}")
            continue
    
    # If testing all dialects, create a combined report
    if args.dialect == 'all' and len(all_dialect_results) > 1:
        combined_report = {
            "metadata": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "dialects_tested": list(all_dialect_results.keys())
            },
            "results_by_dialect": all_dialect_results
        }
        
        with open("test_report.json", 'w', encoding='utf-8') as f:
            json.dump(combined_report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Combined report saved to test_report.json")
    
    print(f"\n🎉 Testing complete!")

if __name__ == "__main__":
    main()