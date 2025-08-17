"""
Operator handling for FHIRPath expressions.

This module handles binary and unary operations, including arithmetic,
logical, comparison, and other operators from FHIRPath AST into SQL.
"""

from typing import Tuple, Any


class OperatorHandler:
    """Handles operator node processing for FHIRPath to SQL conversion."""
    
    def __init__(self, sql_operators_dict, ast_nodes, dialect):
        """
        Initialize the operator handler.
        
        Args:
            sql_operators_dict: Dictionary mapping FHIRPath operators to SQL operators
            ast_nodes: AST node classes for type checking
            dialect: Database dialect for specific SQL generation
        """
        self.SQL_OPERATORS = sql_operators_dict
        self.LiteralNode = ast_nodes['LiteralNode']
        self.IdentifierNode = ast_nodes['IdentifierNode']  
        self.PathNode = ast_nodes['PathNode']
        self.FunctionCallNode = ast_nodes['FunctionCallNode']
        self.dialect = dialect
    
    def visit_binary_op(self, node, visit_func, determine_comparison_casts_func, generate_union_sql_func) -> str:
        """
        Visit a binary operation node with proper type casting.
        
        Args:
            node: Binary operation AST node
            visit_func: Function to visit child nodes
            determine_comparison_casts_func: Function to determine comparison casting
            generate_union_sql_func: Function to generate union SQL
            
        Returns:
            SQL expression for the binary operation
        """
        left = visit_func(node.left)
        right = visit_func(node.right)
        
        sql_op = self.SQL_OPERATORS.get(node.operator.lower(), node.operator)
        
        # Handle string concatenation vs arithmetic for + operator
        if sql_op == '+':
            # Check if this should be string concatenation
            if self._is_string_concatenation(node.left, node.right):
                # Use dialect-specific string concatenation
                return self.dialect.string_concat(left, right)
            else:
                # Treat as arithmetic - cast to numeric types
                if not (isinstance(node.left, self.LiteralNode) and node.left.type in ['integer', 'decimal']):
                    left = f"CAST({left} AS DOUBLE)"
                if not (isinstance(node.right, self.LiteralNode) and node.right.type in ['integer', 'decimal']):
                    right = f"CAST({right} AS DOUBLE)"
                return f"({left} {sql_op} {right})"
        
        # Handle other arithmetic operations with potential JSON operands
        elif sql_op in ['-', '*', '/']:
            # If left is not a number literal, cast it (it might be from json_extract)
            if not (isinstance(node.left, self.LiteralNode) and node.left.type in ['integer', 'decimal']):
                left = f"CAST({left} AS DOUBLE)"
            # If right is not a number literal, cast it
            if not (isinstance(node.right, self.LiteralNode) and node.right.type in ['integer', 'decimal']):
                right = f"CAST({right} AS DOUBLE)"
            return f"({left} {sql_op} {right})"
        
        # Handle integer division and modulo operations
        elif node.operator == 'div':
            # Integer division - cast to integers and use integer division
            if not (isinstance(node.left, self.LiteralNode) and node.left.type in ['integer', 'decimal']):
                left = f"CAST({left} AS INTEGER)"
            if not (isinstance(node.right, self.LiteralNode) and node.right.type in ['integer', 'decimal']):
                right = f"CAST({right} AS INTEGER)"
            # Use floor division to ensure integer result, with division by zero protection
            return f"CASE WHEN {right} = 0 THEN NULL ELSE CAST(floor(CAST({left} AS DOUBLE) / CAST({right} AS DOUBLE)) AS INTEGER) END"
        
        elif node.operator == 'mod':
            # Modulo operation - cast to integers and use modulo
            if not (isinstance(node.left, self.LiteralNode) and node.left.type in ['integer', 'decimal']):
                left = f"CAST({left} AS INTEGER)"
            if not (isinstance(node.right, self.LiteralNode) and node.right.type in ['integer', 'decimal']):
                right = f"CAST({right} AS INTEGER)"
            # Use modulo function with division by zero protection
            return f"CASE WHEN {right} = 0 THEN NULL ELSE ({left} % {right}) END"

        # Handle logical operations (AND, OR) with proper boolean casting
        elif sql_op in ['AND', 'OR']:
            # Cast known boolean fields to proper boolean types for logical operations
            left_cast = self._ensure_boolean_casting(node.left, left)
            right_cast = self._ensure_boolean_casting(node.right, right)
            return f"({left_cast} {sql_op} {right_cast})"
        
        # Handle XOR operation (exclusive OR) - Phase 6 Week 15
        elif node.operator == 'xor':
            # XOR: true when operands differ, false when they're the same
            left_cast = self._ensure_boolean_casting(node.left, left)
            right_cast = self._ensure_boolean_casting(node.right, right)
            return f"(({left_cast} AND NOT {right_cast}) OR (NOT {left_cast} AND {right_cast}))"
        
        # Handle IMPLIES operation (logical implication) - Phase 6 Week 15  
        elif node.operator == 'implies':
            # IMPLIES: equivalent to (NOT left OR right), or false only when left is true and right is false
            left_cast = self._ensure_boolean_casting(node.left, left)
            right_cast = self._ensure_boolean_casting(node.right, right)
            return f"(NOT {left_cast} OR {right_cast})"

        # Handle JSON value comparisons with proper type casting
        elif node.operator in ['=', '!=', '>', '<', '>=', '<=', '~', '!~']:
            left_cast, right_cast = determine_comparison_casts_func(node.left, node.right, left, right)
            comparison_sql = f"({left_cast} {sql_op} {right_cast})"
            # Apply PostgreSQL JSON operator fixes inline during generation
            if hasattr(self.dialect, 'fix_json_operators_inline'):
                comparison_sql = self.dialect.fix_json_operators_inline(comparison_sql)
            return comparison_sql
        
        # Handle collection union operations
        elif node.operator == '|':
            return generate_union_sql_func(left, right)
        
        return f"({left} {sql_op} {right})"
    
    def visit_unary_op(self, node, visit_func) -> str:
        """
        Visit a unary operation node.
        
        Args:
            node: Unary operation AST node
            visit_func: Function to visit child nodes
            
        Returns:
            SQL expression for the unary operation
        """
        operand = visit_func(node.operand)
        
        if node.operator.lower() == 'not':
            return f"NOT ({operand})"
        elif node.operator == '+': # Unary plus
            return f"+({operand})" 
        elif node.operator == '-': # Unary minus
            return f"-({operand})"
        else:
            return f"{node.operator}({operand})"
    
    def _ensure_boolean_casting(self, node, sql_expr: str) -> str:
        """Ensure proper boolean casting for logical operations"""
        # Check if this is a known boolean field that needs casting
        if self._is_boolean_field(node):
            return f"CAST({sql_expr} AS BOOLEAN)"
        # Check if this is already a boolean comparison that doesn't need additional casting
        elif sql_expr.strip().startswith("CAST(") and "AS BOOLEAN" in sql_expr:
            return sql_expr
        # Check if this is a CTE subquery that might contain boolean values
        elif (sql_expr.strip().startswith("(SELECT") and 
              sql_expr.strip().endswith(")") and 
              ("extracted_value" in sql_expr or "array_extract_result" in sql_expr)):
            return f"CAST({sql_expr} AS BOOLEAN)"
        # Check if this is a simple json_extract that might be a boolean field
        elif "json_extract" in sql_expr and self._contains_boolean_field_reference(sql_expr):
            return f"CAST({sql_expr} AS BOOLEAN)"
        else:
            return sql_expr
    
    def _is_boolean_field(self, node) -> bool:
        """Check if a node represents a known boolean field"""
        # Import KNOWN_BOOLEAN_FIELDS from constants
        try:
            from ..constants import KNOWN_BOOLEAN_FIELDS
        except ImportError:
            KNOWN_BOOLEAN_FIELDS = ['active', 'deceasedBoolean']
            
        if isinstance(node, self.IdentifierNode):
            return node.name in KNOWN_BOOLEAN_FIELDS
        elif isinstance(node, self.PathNode) and node.segments:
            # Check if the last segment is a boolean field
            last_segment = node.segments[-1]
            if isinstance(last_segment, self.IdentifierNode):
                return last_segment.name in KNOWN_BOOLEAN_FIELDS
        return False
    
    def _contains_boolean_field_reference(self, sql_expr: str) -> bool:
        """Check if SQL expression contains references to known boolean fields"""
        try:
            from ..constants import KNOWN_BOOLEAN_FIELDS
        except ImportError:
            KNOWN_BOOLEAN_FIELDS = ['active', 'deceasedBoolean']
        return any(f"'{field}'" in sql_expr or f'$.{field}' in sql_expr for field in KNOWN_BOOLEAN_FIELDS)
    
    def _is_string_concatenation(self, left_node, right_node) -> bool:
        """Determine if a + operation should be string concatenation vs arithmetic"""
        # If either operand is a string literal, treat as string concatenation
        if (isinstance(left_node, self.LiteralNode) and left_node.type == 'string') or \
           (isinstance(right_node, self.LiteralNode) and right_node.type == 'string'):
            return True
        
        # If dealing with paths that likely return strings (name, family, given, etc.)
        if self._is_likely_string_path(left_node) or self._is_likely_string_path(right_node):
            return True
        
        # If either operand is a function that likely returns a string
        if self._is_likely_string_function(left_node) or self._is_likely_string_function(right_node):
            return True
            
        return False
    
    def _is_likely_string_path(self, node) -> bool:
        """Check if a node likely represents a string value"""
        if isinstance(node, self.PathNode):
            # Check for common string field patterns
            path_str = self._path_to_string(node)
            string_patterns = ['name', 'family', 'given', 'system', 'value', 'display', 'text', 'code', 'status', 'version', 'title', 'description']
            return any(pattern in path_str.lower() for pattern in string_patterns)
        elif isinstance(node, self.IdentifierNode):
            string_fields = ['name', 'family', 'given', 'system', 'value', 'display', 'text', 'code', 'status', 'version', 'title', 'description', 'id', 'resourceType']
            return node.name.lower() in string_fields
        return False
    
    def _is_likely_string_function(self, node) -> bool:
        """Check if a function call likely returns a string value"""
        if isinstance(node, self.FunctionCallNode):
            func_name = node.name.lower()
            string_functions = ['tostring', 'substring', 'first', 'last', 'join', 'replace', 'toupper', 'tolower', 'upper', 'lower', 'trim']
            return func_name in string_functions
        return False
    
    def _path_to_string(self, node) -> str:
        """Convert a path node to string representation for pattern matching"""
        if isinstance(node, self.PathNode):
            segments = []
            for segment in node.segments:
                if isinstance(segment, self.IdentifierNode):
                    segments.append(segment.name)
            return '.'.join(segments)
        return ""