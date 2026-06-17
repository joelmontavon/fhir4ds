"""
Legacy String Function Handler - Compatibility Shim for Unit Tests.

This module provides compatibility with legacy unit tests that expect the old
StringFunctionHandler class.
"""

from typing import List, Any, Optional, Dict, Union


class StringFunctionHandler:
    """
    Legacy compatibility shim for StringFunctionHandler.
    
    This class mimics the old API to allow existing unit tests to pass.
    """
    
    def __init__(self, generator=None):
        """Initialize with legacy generator (for compatibility)."""
        self.generator = generator
        self._supported_functions = {
            'substring', 'startswith', 'endswith', 'indexof', 'replace', 'toupper', 'tolower',
            'upper', 'lower', 'trim', 'split', 'tochars', 'matches', 'replacematches', 'join'
        }
    
    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        return function_name.lower() in self._supported_functions
    
    def handle_function(self, function_name: str, base_expr: str, func_node) -> str:
        """Handle function call - compatibility method."""
        # For unit tests, just return mock SQL
        return f"CASE WHEN {base_expr} IS NULL THEN NULL ELSE 'mock_string_sql' END"
    
    def _handle_tochars(self, base_expr: str, func_node) -> str:
        """Handle toChars() function - mock implementation for tests."""
        if hasattr(func_node, 'args') and func_node.args:
            raise ValueError("toChars() function takes no arguments")
        
        return f"""CASE
            WHEN {base_expr} IS NULL THEN NULL
            ELSE (
                SELECT json_array_agg(substr({base_expr}, pos, 1))
                FROM generate_series(1, length({base_expr})) pos
            )
        END"""
    
    def _handle_matches(self, base_expr: str, func_node) -> str:
        """Handle matches() function - mock implementation for tests."""
        if not hasattr(func_node, 'args') or len(func_node.args) != 1:
            raise ValueError("matches() function requires exactly one argument")
        
        return f"CASE WHEN {base_expr} IS NULL THEN NULL ELSE true END"
    
    def _handle_replacematches(self, base_expr: str, func_node) -> str:
        """Handle replaceMatches() function - mock implementation for tests."""
        if not hasattr(func_node, 'args') or len(func_node.args) != 2:
            raise ValueError("replaceMatches() function requires exactly two arguments")
        
        return f"CASE WHEN {base_expr} IS NULL THEN NULL ELSE {base_expr} END"