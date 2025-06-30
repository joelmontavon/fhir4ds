#!/usr/bin/env python3

import os
import sys
import json

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, project_root)

from fhir4ds import FHIRDataStore, ViewRunner

def test_logical_operations():
    # Create test data with various patients
    test_data = [
        {
            "resourceType": "Patient",
            "id": "m0",
            "gender": "male",
            "deceasedBoolean": False
        },
        {
            "resourceType": "Patient", 
            "id": "f0",
            "gender": "female",
            "deceasedBoolean": False
        },
        {
            "resourceType": "Patient",
            "id": "m1", 
            "gender": "male",
            "deceasedBoolean": True
        },
        {
            "resourceType": "Patient",
            "id": "f1",
            "gender": "female"
        }
    ]

    # Setup modern datastore
    datastore = FHIRDataStore.with_duckdb()
    
    # Load test data
    for resource in test_data:
        datastore.load_resource(resource)

    # Create runner
    runner = datastore.view_runner()

    print("=== Testing Logical Operations ===\n")

    # Test 1: AND operation - Use actual test from logic.json
    print("1. Testing AND operation:")
    view_def_and = {
        "name": "test_and",
        "resource": "Patient",
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id"
                    }
                ]
            }
        ],
        "where": [
            {
                "path": "gender = 'male' and deceased.ofType(boolean) = false"
            }
        ]
    }
    
    try:
        result_and = runner.execute_view_definition(view_def_and)
        rows_and = result_and.fetchall()
        print(f"   Expected: [('m0',)] (male and not deceased)")
        print(f"   Actual: {rows_and}")
        print(f"   Result: {'✅ PASS' if rows_and == [('m0',)] else '❌ FAIL'}")
    except Exception as e:
        print(f"   ❌ FAIL: {e}")
    print()

    # Test 2: OR operation - Use actual test from logic.json
    print("2. Testing OR operation:")
    view_def_or = {
        "name": "test_or",
        "resource": "Patient",
        "select": [
            {
                "column": [
                    {
                        "name": "id", 
                        "path": "id"
                    }
                ]
            }
        ],
        "where": [
            {
                "path": "gender = 'male' or deceased.ofType(boolean) = false"
            }
        ]
    }
    
    try:
        result_or = runner.execute_view_definition(view_def_or)
        rows_or = result_or.fetchall()
        print(f"   Expected: [('m0',), ('f0',), ('m1',)] (male or not deceased)")
        print(f"   Actual: {rows_or}")
        expected_or = [('m0',), ('f0',), ('m1',)]
        actual_set = set(rows_or)
        expected_set = set(expected_or)
        print(f"   Result: {'✅ PASS' if actual_set == expected_set else '❌ FAIL'}")
    except Exception as e:
        print(f"   ❌ FAIL: {e}")
    print()

    # Test 3: NOT operation - Use actual test from logic.json
    print("3. Testing NOT operation:")
    view_def_not = {
        "name": "test_not",
        "resource": "Patient", 
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id"
                    }
                ]
            }
        ],
        "where": [
            {
                "path": "(gender = 'male').not()"
            }
        ]
    }
    
    try:
        result_not = runner.execute_view_definition(view_def_not)
        rows_not = result_not.fetchall()
        print(f"   Expected: [('f0',), ('f1',)] (not male, i.e., female)")
        print(f"   Actual: {rows_not}")
        expected_not = [('f0',), ('f1',)]
        # Handle quoted strings
        actual_clean = [(row[0].strip('"'),) if isinstance(row[0], str) and row[0].startswith('"') else row for row in rows_not]
        print(f"   Result: {'✅ PASS' if set(actual_clean) == set(expected_not) else '❌ FAIL'}")
    except Exception as e:
        print(f"   ❌ FAIL: {e}")
    print()

if __name__ == "__main__":
    test_logical_operations()