"""
Literal handling for FHIRPath expressions.

This module handles the conversion of literal nodes (strings, numbers, booleans)
from FHIRPath AST into their SQL representation.
"""

from typing import Any


class LiteralHandler:
    """Handles literal node processing for FHIRPath to SQL conversion."""
    
    def __init__(self):
        """Initialize the literal handler."""
        pass
    
    def visit_literal(self, node) -> str:
        """
        Visit a literal node and convert to SQL representation.
        
        Args:
            node: AST literal node with type and value attributes
            
        Returns:
            SQL string representation of the literal
            
        Supported types:
            - string: Wrapped in single quotes
            - integer/decimal: Raw numeric value
            - boolean: Lowercase true/false
            - other: String representation
        """
        if node.type == 'string':
            return f"'{node.value}'"
        elif node.type in ['integer', 'decimal']:
            return str(node.value)
        elif node.type == 'boolean':
            return str(node.value).lower()
        else:
            return str(node.value)
    
    def visit_variable(self, node) -> str:
        """
        Visit a variable node ($index, $total, etc.).
        
        Args:
            node: AST variable node with name attribute
            
        Returns:
            SQL expression for the variable
            
        Supported variables:
            - $index: 0-based index in collection iteration
            - $total: Total count in collection iteration
        """
        # Phase 7 Week 16: Context Variables Implementation
        
        if node.name == 'index':
            # $index variable: represents the 0-based index in collection iteration
            # For now, return a placeholder that can be filled in by collection operations
            # In a real implementation, this would need context from the containing iteration
            return "ROW_NUMBER() OVER () - 1"  # 0-based index
            
        elif node.name == 'total':
            # $total variable: represents the total count in collection iteration
            # For now, return a placeholder that can be filled in by collection operations
            # In a real implementation, this would need context from the containing iteration
            return "COUNT(*) OVER ()"  # Total count
            
        else:
            raise ValueError(f"Unknown context variable: ${node.name}")