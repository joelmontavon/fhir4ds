"""
CQL Nullological Functions

Implements CQL-specific nullological operators including Coalesce, IsNull,
IsTrue, IsFalse, and other null-handling functions that support CQL's
three-valued logic system.
"""

import logging
from typing import Any, List, Dict, Union
from ...fhirpath.parser.ast_nodes import LiteralNode

logger = logging.getLogger(__name__)


class CQLNullologicalFunctionHandler:
    """
    CQL nullological function handler implementing three-valued logic.
    
    Provides null-handling functions specific to CQL that may differ from
    standard SQL or FHIRPath null semantics.
    """
    
    def __init__(self, dialect: str = "duckdb"):
        """Initialize CQL nullological function handler."""
        self.dialect = dialect
        
        # Register nullological functions
        self.function_map = {
            'coalesce': self.coalesce,
            'isnull': self.is_null,
            'isnotnull': self.is_not_null,
            'istrue': self.is_true,
            'isfalse': self.is_false,
            'ifnull': self.if_null,
            'nullif': self.null_if,
        }
    
    def coalesce(self, *args: Any) -> LiteralNode:
        """
        CQL Coalesce() function - return first non-null value.
        
        CQL Coalesce differs from SQL COALESCE in that it follows
        CQL's three-valued logic and type system rules.
        
        Args:
            *args: Variable number of expressions to coalesce
            
        Returns:
            LiteralNode containing SQL expression for coalesce operation
        """
        logger.debug(f"Generating CQL Coalesce() with {len(args)} arguments")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        if not args:
            sql = "NULL"
        elif len(args) == 1:
            sql = str(extract_value(args[0]))
        else:
            # Use SQL COALESCE for basic functionality
            # Could be enhanced for CQL-specific type conversion rules
            args_sql = [str(extract_value(arg)) for arg in args]
            sql = f"COALESCE({', '.join(args_sql)})"
        
        return LiteralNode(value=sql, type='sql')
    
    def is_null(self, expression: Any) -> LiteralNode:
        """
        CQL IsNull() function - test if expression is null.
        
        Args:
            expression: Expression to test for null
            
        Returns:
            LiteralNode containing SQL expression that returns true if expression is null
        """
        logger.debug("Generating CQL IsNull() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        sql = f"({expr_val} IS NULL)"
        return LiteralNode(value=sql, type='sql')
    
    def is_not_null(self, expression: Any) -> LiteralNode:
        """
        CQL IsNotNull() function - test if expression is not null.
        
        Args:
            expression: Expression to test for not null
            
        Returns:
            LiteralNode containing SQL expression that returns true if expression is not null
        """
        logger.debug("Generating CQL IsNotNull() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        sql = f"({expr_val} IS NOT NULL)"
        return LiteralNode(value=sql, type='sql')
    
    def is_true(self, expression: Any) -> LiteralNode:
        """
        CQL IsTrue() function - test if expression is true (not null or false).
        
        In CQL's three-valued logic:
        - IsTrue(true) = true
        - IsTrue(false) = false  
        - IsTrue(null) = false
        
        Args:
            expression: Boolean expression to test
            
        Returns:
            LiteralNode containing SQL expression that returns true only if expression is true
        """
        logger.debug("Generating CQL IsTrue() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        # Handle three-valued logic: true if expression is true, false otherwise
        sql = f"({expr_val} IS TRUE)"
        return LiteralNode(value=sql, type='sql')
    
    def is_false(self, expression: Any) -> LiteralNode:
        """
        CQL IsFalse() function - test if expression is false (not null or true).
        
        In CQL's three-valued logic:
        - IsFalse(true) = false
        - IsFalse(false) = true
        - IsFalse(null) = false
        
        Args:
            expression: Boolean expression to test
            
        Returns:
            LiteralNode containing SQL expression that returns true only if expression is false
        """
        logger.debug("Generating CQL IsFalse() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        # Handle three-valued logic: true if expression is false, false otherwise
        sql = f"({expr_val} IS FALSE)"
        return LiteralNode(value=sql, type='sql')
    
    def if_null(self, test_expression: Any, null_value: Any) -> LiteralNode:
        """
        CQL IfNull() function - return alternative value if expression is null.
        
        Similar to Coalesce but with only two arguments.
        
        Args:
            test_expression: Expression to test for null
            null_value: Value to return if test_expression is null
            
        Returns:
            LiteralNode containing SQL expression for conditional null replacement
        """
        logger.debug("Generating CQL IfNull() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        test_val = extract_value(test_expression)
        null_val = extract_value(null_value)
        sql = f"COALESCE({test_val}, {null_val})"
        return LiteralNode(value=sql, type='sql')
    
    def null_if(self, expression: Any, compare_value: Any) -> LiteralNode:
        """
        CQL NullIf() function - return null if expression equals compare_value.
        
        Args:
            expression: Expression to evaluate
            compare_value: Value to compare against
            
        Returns:
            LiteralNode containing SQL expression that returns null if values are equal
        """
        logger.debug("Generating CQL NullIf() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        comp_val = extract_value(compare_value)
        sql = f"NULLIF({expr_val}, {comp_val})"
        return LiteralNode(value=sql, type='sql')
    
    def to_boolean(self, expression: Any) -> LiteralNode:
        """
        CQL ToBoolean() function - convert expression to boolean.
        
        Handles CQL's specific boolean conversion rules:
        - 'true', 't', '1' -> true
        - 'false', 'f', '0' -> false  
        - null/empty -> null
        - Other strings -> null
        
        Args:
            expression: Expression to convert to boolean
            
        Returns:
            LiteralNode containing SQL expression for boolean conversion
        """
        logger.debug("Generating CQL ToBoolean() function")
        
        # Extract value from AST node if needed
        def extract_value(arg):
            if hasattr(arg, 'value'):
                return arg.value
            else:
                return str(arg)
        
        expr_val = extract_value(expression)
        
        if self.dialect == "duckdb":
            sql = f"""
            CASE 
                WHEN LOWER(TRIM({expr_val})) IN ('true', 't', '1') THEN true
                WHEN LOWER(TRIM({expr_val})) IN ('false', 'f', '0') THEN false
                WHEN {expr_val} IS NULL OR TRIM({expr_val}) = '' THEN NULL
                ELSE NULL
            END
            """.strip()
        elif self.dialect == "postgresql":
            sql = f"""
            CASE 
                WHEN LOWER(TRIM({expr_val})) IN ('true', 't', '1') THEN true
                WHEN LOWER(TRIM({expr_val})) IN ('false', 'f', '0') THEN false
                WHEN {expr_val} IS NULL OR TRIM({expr_val}) = '' THEN NULL
                ELSE NULL
            END
            """.strip()
        else:
            # Generic SQL fallback
            sql = f"""
            CASE 
                WHEN LOWER(TRIM({expr_val})) IN ('true', 't', '1') THEN true
                WHEN LOWER(TRIM({expr_val})) IN ('false', 'f', '0') THEN false
                WHEN {expr_val} IS NULL OR TRIM({expr_val}) = '' THEN NULL
                ELSE NULL
            END
            """.strip()
        
        return LiteralNode(value=sql, type='sql')
    
    def handle_three_valued_logic(self, operator: str, left: Any, right: Any) -> str:
        """
        Handle CQL three-valued logic for comparison operators.
        
        In CQL three-valued logic:
        - true AND null = null
        - false AND null = false
        - null AND null = null
        - true OR null = true
        - false OR null = null
        - null OR null = null
        
        Args:
            operator: Logical operator (AND, OR, NOT)
            left: Left operand
            right: Right operand (not used for NOT)
            
        Returns:
            SQL expression implementing three-valued logic
        """
        logger.debug(f"Generating three-valued logic for {operator}")
        
        operator = operator.upper()
        
        if operator == "AND":
            # CQL AND: null if either operand is null and the other is true
            return f"""
            CASE
                WHEN ({left}) IS NULL AND ({right}) IS NULL THEN NULL
                WHEN ({left}) IS NULL AND ({right}) = false THEN false
                WHEN ({left}) IS NULL AND ({right}) = true THEN NULL
                WHEN ({left}) = false AND ({right}) IS NULL THEN false
                WHEN ({left}) = true AND ({right}) IS NULL THEN NULL
                ELSE ({left}) AND ({right})
            END
            """.strip()
        
        elif operator == "OR":
            # CQL OR: null if either operand is null and the other is false
            return f"""
            CASE
                WHEN ({left}) IS NULL AND ({right}) IS NULL THEN NULL
                WHEN ({left}) IS NULL AND ({right}) = true THEN true
                WHEN ({left}) IS NULL AND ({right}) = false THEN NULL
                WHEN ({left}) = true AND ({right}) IS NULL THEN true
                WHEN ({left}) = false AND ({right}) IS NULL THEN NULL
                ELSE ({left}) OR ({right})
            END
            """.strip()
        
        elif operator == "NOT":
            # CQL NOT: null remains null
            return f"""
            CASE
                WHEN ({left}) IS NULL THEN NULL
                ELSE NOT ({left})
            END
            """.strip()
        
        else:
            logger.warning(f"Unknown three-valued logic operator: {operator}")
            return f"({left} {operator} {right})"
    
    def get_supported_functions(self) -> List[str]:
        """Get list of all supported nullological functions."""
        return list(self.function_map.keys()) + ['toboolean']
    
    def generate_nullological_function_sql(self, function_name: str, args: List[Any],
                                         dialect: str = None) -> str:
        """
        Generate SQL for CQL nullological function call.
        
        Args:
            function_name: Name of the function to call
            args: Function arguments
            dialect: Database dialect (overrides instance dialect)
            
        Returns:
            SQL expression for function call
        """
        if dialect:
            old_dialect = self.dialect
            self.dialect = dialect
        
        try:
            function_name_lower = function_name.lower()
            
            if function_name_lower == 'toboolean':
                return self.to_boolean(args[0]) if args else "NULL"
            
            if function_name_lower in self.function_map:
                handler = self.function_map[function_name_lower]
                
                if function_name_lower == 'coalesce':
                    return handler(*args)
                elif len(args) == 1:
                    return handler(args[0])
                elif len(args) == 2:
                    return handler(args[0], args[1])
                else:
                    logger.warning(f"Invalid number of arguments for {function_name}: {len(args)}")
                    return "NULL"
            else:
                logger.warning(f"Unknown nullological function: {function_name}")
                return f"-- Unknown function: {function_name}({', '.join(map(str, args))})"
                
        finally:
            if dialect:
                self.dialect = old_dialect
    
    def get_function_info(self) -> Dict[str, Dict[str, Any]]:
        """Get information about supported nullological functions."""
        return {
            'coalesce': {
                'description': 'Return first non-null value from arguments',
                'args': 'variable',
                'returns': 'any',
                'example': 'Coalesce(null, 5, 10) returns 5'
            },
            'isnull': {
                'description': 'Test if expression is null',
                'args': 1,
                'returns': 'boolean',
                'example': 'IsNull(someValue)'
            },
            'isnotnull': {
                'description': 'Test if expression is not null',
                'args': 1,
                'returns': 'boolean',
                'example': 'IsNotNull(someValue)'
            },
            'istrue': {
                'description': 'Test if expression is true (three-valued logic)',
                'args': 1,
                'returns': 'boolean',
                'example': 'IsTrue(condition)'
            },
            'isfalse': {
                'description': 'Test if expression is false (three-valued logic)',
                'args': 1,
                'returns': 'boolean',
                'example': 'IsFalse(condition)'
            },
            'ifnull': {
                'description': 'Return alternative if expression is null',
                'args': 2,
                'returns': 'any',
                'example': 'IfNull(value, 0)'
            },
            'nullif': {
                'description': 'Return null if expression equals compare value',
                'args': 2,
                'returns': 'any',
                'example': 'NullIf(value, -1)'
            },
            'toboolean': {
                'description': 'Convert expression to boolean using CQL rules',
                'args': 1,
                'returns': 'boolean',
                'example': 'ToBoolean("true")'
            }
        }