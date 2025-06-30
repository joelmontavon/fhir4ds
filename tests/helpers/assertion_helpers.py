"""
Custom Assertion Helpers

Utilities for making assertions specific to FHIR4DS testing.
"""

import json
import pandas as pd
from typing import Any, Dict, List, Optional, Union
from decimal import Decimal


def assert_valid_viewdefinition(view_def: Dict[str, Any]):
    """
    Assert that a dictionary is a valid ViewDefinition.
    
    Args:
        view_def: Dictionary to validate
        
    Raises:
        AssertionError: If not a valid ViewDefinition
    """
    assert isinstance(view_def, dict), "ViewDefinition must be a dictionary"
    assert view_def.get("resourceType") == "ViewDefinition", "Must have resourceType 'ViewDefinition'"
    assert "resource" in view_def, "ViewDefinition must specify a resource"
    assert "select" in view_def, "ViewDefinition must have select clause"
    assert isinstance(view_def["select"], list), "Select must be a list"
    assert len(view_def["select"]) > 0, "Select must not be empty"
    
    # Validate select structure
    for i, select_item in enumerate(view_def["select"]):
        assert isinstance(select_item, dict), f"Select item {i} must be a dictionary"
        
        # Handle union operations
        if "unionAll" in select_item:
            assert isinstance(select_item["unionAll"], list), f"Select item {i} unionAll must be a list"
            assert len(select_item["unionAll"]) > 0, f"Select item {i} unionAll must not be empty"
            # Validate each union member has columns
            for j, union_member in enumerate(select_item["unionAll"]):
                assert isinstance(union_member, dict), f"Union member {j} must be a dictionary"
                assert "column" in union_member, f"Union member {j} must have columns"
                assert isinstance(union_member["column"], list), f"Union member {j} columns must be a list"
                assert len(union_member["column"]) > 0, f"Union member {j} must have at least one column"
        else:
            # Regular select item
            assert "column" in select_item, f"Select item {i} must have columns"
            assert isinstance(select_item["column"], list), f"Select item {i} columns must be a list"
            assert len(select_item["column"]) > 0, f"Select item {i} must have at least one column"


def assert_valid_query_result(result, expected_min_rows: int = 0):
    """
    Assert that a query result is valid.
    
    Args:
        result: Query result object
        expected_min_rows: Minimum expected number of rows
        
    Raises:
        AssertionError: If result is invalid
    """
    assert result is not None, "Query result cannot be None"
    
    # Try to fetch rows
    try:
        rows = result.fetchall()
        assert isinstance(rows, list), "Result rows must be a list"
        assert len(rows) >= expected_min_rows, f"Expected at least {expected_min_rows} rows, got {len(rows)}"
    except Exception as e:
        raise AssertionError(f"Failed to fetch result rows: {e}")


def assert_valid_dataframe(df: pd.DataFrame, expected_min_rows: int = 0, expected_columns: Optional[List[str]] = None):
    """
    Assert that a DataFrame is valid.
    
    Args:
        df: Pandas DataFrame to validate
        expected_min_rows: Minimum expected number of rows
        expected_columns: Expected column names (optional)
        
    Raises:
        AssertionError: If DataFrame is invalid
    """
    assert isinstance(df, pd.DataFrame), "Must be a pandas DataFrame"
    assert len(df) >= expected_min_rows, f"Expected at least {expected_min_rows} rows, got {len(df)}"
    
    if expected_columns:
        for col in expected_columns:
            assert col in df.columns, f"Expected column '{col}' not found in DataFrame"


def assert_valid_json_export(json_str: str, expected_min_records: int = 0):
    """
    Assert that a JSON export string is valid.
    
    Args:
        json_str: JSON string to validate
        expected_min_records: Minimum expected number of records
        
    Raises:
        AssertionError: If JSON is invalid
    """
    assert isinstance(json_str, str), "JSON export must be a string"
    assert len(json_str) > 0, "JSON export cannot be empty"
    
    # Parse JSON
    try:
        data = json.loads(json_str)
        assert isinstance(data, dict), "JSON export must be a dictionary"
        assert "data" in data, "JSON export must have 'data' key"
        assert isinstance(data["data"], list), "JSON export data must be a list"
        assert len(data["data"]) >= expected_min_records, f"Expected at least {expected_min_records} records"
        assert "row_count" in data, "JSON export must have 'row_count'"
        assert data["row_count"] == len(data["data"]), "Row count must match data length"
    except json.JSONDecodeError as e:
        raise AssertionError(f"Invalid JSON: {e}")


def assert_file_exists_and_not_empty(file_path: str):
    """
    Assert that a file exists and is not empty.
    
    Args:
        file_path: Path to the file
        
    Raises:
        AssertionError: If file doesn't exist or is empty
    """
    import os
    assert os.path.exists(file_path), f"File does not exist: {file_path}"
    assert os.path.getsize(file_path) > 0, f"File is empty: {file_path}"


def assert_performance_metrics_valid(metrics):
    """
    Assert that performance metrics are valid.
    
    Args:
        metrics: Performance metrics object
        
    Raises:
        AssertionError: If metrics are invalid
    """
    assert hasattr(metrics, 'execution_time'), "Metrics must have execution_time"
    assert hasattr(metrics, 'success'), "Metrics must have success"
    assert hasattr(metrics, 'row_count'), "Metrics must have row_count"
    assert hasattr(metrics, 'efficiency_score'), "Metrics must have efficiency_score"
    
    assert isinstance(metrics.execution_time, (int, float)), "Execution time must be numeric"
    assert metrics.execution_time >= 0, "Execution time must be non-negative"
    assert isinstance(metrics.success, bool), "Success must be boolean"
    assert isinstance(metrics.row_count, int), "Row count must be integer"
    assert metrics.row_count >= 0, "Row count must be non-negative"
    assert isinstance(metrics.efficiency_score, (int, float)), "Efficiency score must be numeric"
    assert 0 <= metrics.efficiency_score <= 100, "Efficiency score must be between 0 and 100"


def assert_batch_results_valid(results: List, expected_count: int):
    """
    Assert that batch processing results are valid.
    
    Args:
        results: List of batch results
        expected_count: Expected number of results
        
    Raises:
        AssertionError: If results are invalid
    """
    assert isinstance(results, list), "Batch results must be a list"
    assert len(results) == expected_count, f"Expected {expected_count} results, got {len(results)}"
    
    for i, result in enumerate(results):
        assert hasattr(result, 'success'), f"Result {i} must have success attribute"
        assert hasattr(result, 'execution_time'), f"Result {i} must have execution_time"
        assert isinstance(result.success, bool), f"Result {i} success must be boolean"
        assert isinstance(result.execution_time, (int, float)), f"Result {i} execution_time must be numeric"


def assert_template_metadata_valid(metadata):
    """
    Assert that template metadata is valid.
    
    Args:
        metadata: Template metadata object
        
    Raises:
        AssertionError: If metadata is invalid
    """
    assert hasattr(metadata, 'name'), "Metadata must have name"
    assert hasattr(metadata, 'title'), "Metadata must have title"
    assert hasattr(metadata, 'description'), "Metadata must have description"
    assert hasattr(metadata, 'resource_type'), "Metadata must have resource_type"
    assert hasattr(metadata, 'category'), "Metadata must have category"
    assert hasattr(metadata, 'tags'), "Metadata must have tags"
    
    assert isinstance(metadata.name, str), "Name must be string"
    assert len(metadata.name) > 0, "Name cannot be empty"
    assert isinstance(metadata.tags, list), "Tags must be a list"


def assert_optimization_suggestions_valid(suggestions):
    """
    Assert that optimization suggestions are valid.
    
    Args:
        suggestions: Optimization suggestions object
        
    Raises:
        AssertionError: If suggestions are invalid
    """
    assert hasattr(suggestions, 'recommendations'), "Must have recommendations"
    assert hasattr(suggestions, 'overall_score'), "Must have overall_score"
    assert isinstance(suggestions.recommendations, list), "Recommendations must be a list"
    assert isinstance(suggestions.overall_score, (int, float)), "Overall score must be numeric"
    assert 0 <= suggestions.overall_score <= 100, "Overall score must be between 0 and 100"
    
    for i, rec in enumerate(suggestions.recommendations):
        assert hasattr(rec, 'priority'), f"Recommendation {i} must have priority"
        assert hasattr(rec, 'description'), f"Recommendation {i} must have description"
        assert rec.priority in ['high', 'medium', 'low'], f"Recommendation {i} priority must be high/medium/low"
        assert isinstance(rec.description, str), f"Recommendation {i} description must be string"
        assert len(rec.description) > 0, f"Recommendation {i} description cannot be empty"


def assert_database_connection_valid(db):
    """
    Assert that a database connection is valid and working.
    
    Args:
        db: Database connection object
        
    Raises:
        AssertionError: If connection is invalid
    """
    assert db is not None, "Database connection cannot be None"
    assert hasattr(db, 'execute'), "Database must have execute method"
    assert hasattr(db, 'datastore'), "Database must have datastore"
    assert hasattr(db, 'get_resource_count'), "Database must have get_resource_count method"
    
    # Test basic functionality
    try:
        count = db.get_resource_count()
        assert isinstance(count, int), "Resource count must be integer"
        assert count >= 0, "Resource count must be non-negative"
    except Exception as e:
        raise AssertionError(f"Failed to get resource count: {e}")


def assert_roughly_equal(actual: float, expected: float, tolerance: float = 0.1):
    """
    Assert that two values are roughly equal within tolerance.
    
    Args:
        actual: Actual value
        expected: Expected value
        tolerance: Tolerance for comparison
        
    Raises:
        AssertionError: If values are not roughly equal
    """
    diff = abs(actual - expected)
    assert diff <= tolerance, f"Expected {expected} ± {tolerance}, got {actual} (diff: {diff})"


def assert_contains_substring(text: str, substring: str, case_sensitive: bool = False):
    """
    Assert that text contains a substring.
    
    Args:
        text: Text to search in
        substring: Substring to find
        case_sensitive: Whether search is case sensitive
        
    Raises:
        AssertionError: If substring not found
    """
    search_text = text if case_sensitive else text.lower()
    search_substring = substring if case_sensitive else substring.lower()
    
    assert search_substring in search_text, f"'{substring}' not found in '{text}'"


def assert_list_contains_items(actual_list: List, expected_items: List):
    """
    Assert that a list contains all expected items.
    
    Args:
        actual_list: List to check
        expected_items: Items that should be in the list
        
    Raises:
        AssertionError: If any expected items are missing
    """
    for item in expected_items:
        assert item in actual_list, f"Expected item '{item}' not found in list"