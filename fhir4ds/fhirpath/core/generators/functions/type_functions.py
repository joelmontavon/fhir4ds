"""
Type conversion function handlers for FHIRPath expressions.

This module handles type conversion operations like toBoolean, toString, toInteger,
toDecimal, toDate, toDateTime, toTime, convertsToBoolean, convertsToDecimal,
convertsToInteger, convertsToDate, convertsToDateTime, convertsToTime.
"""

from typing import List, Any, Optional
from ..base_handler import BaseFunctionHandler


class TypeFunctionHandler(BaseFunctionHandler):
    """Handles type conversion function processing for FHIRPath to SQL conversion."""
    
    def __init__(self, generator, cte_builder=None):
        """
        Initialize the type function handler.
        
        Args:
            generator: Reference to main SQLGenerator for complex operations
            cte_builder: Optional CTEBuilder instance for CTE management
        """
        super().__init__(generator, cte_builder)
        self.generator = generator
        self.dialect = generator.dialect
    
    def _generate_boolean_conversion_logic(self, value_expr: str, is_conversion: bool = True) -> str:
        """
        Generate shared boolean conversion logic.
        
        Args:
            value_expr: SQL expression for the value to convert/test
            is_conversion: If True, return boolean values; if False, return test results
            
        Returns:
            SQL CASE statement for boolean conversion/testing
        """
        if is_conversion:
            # For toBoolean() - convert to actual boolean values
            return f"""
            CASE 
                -- String 'true'/'false' case
                WHEN LOWER(CAST({value_expr} AS VARCHAR)) = 'true' THEN true
                WHEN LOWER(CAST({value_expr} AS VARCHAR)) = 'false' THEN false
                -- Numeric 0/1 case 
                WHEN CAST({value_expr} AS DOUBLE) = 1.0 THEN true
                WHEN CAST({value_expr} AS DOUBLE) = 0.0 THEN false
                ELSE NULL
            END
            """
        else:
            # For convertsToBoolean() - test if value can be converted
            return f"""
            CASE 
                -- String 'true'/'false' case
                WHEN LOWER(CAST({value_expr} AS VARCHAR)) IN ('true', 'false') THEN true
                -- Numeric 0/1 case 
                WHEN CAST({value_expr} AS DOUBLE) IN (1.0, 0.0) THEN true
                ELSE false
            END
            """
    
    def _generate_boolean_array_logic(self, base_expr: str, is_conversion: bool = True) -> str:
        """
        Generate boolean logic for array values.
        
        Args:
            base_expr: SQL expression for the array
            is_conversion: If True, return boolean values; if False, return test results
            
        Returns:
            SQL expression for handling boolean arrays
        """
        empty_return = "NULL" if is_conversion else "false"
        
        return f"""
        CASE 
            WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN {empty_return}
            ELSE (
                SELECT 
                    {self._generate_boolean_conversion_logic('value', is_conversion)}
                FROM {self.generator.iterate_json_array(base_expr, '$')}
                LIMIT 1
            )
        END
        """
    
    def _generate_boolean_single_logic(self, base_expr: str, is_conversion: bool = True) -> str:
        """
        Generate boolean logic for single values.
        
        Args:
            base_expr: SQL expression for the single value
            is_conversion: If True, return boolean values; if False, return test results
            
        Returns:
            SQL expression for handling single boolean values
        """
        return self._generate_boolean_conversion_logic(base_expr, is_conversion)
        
    def get_supported_functions(self) -> List[str]:
        """Return list of type function names this handler supports."""
        return [
            'toboolean', 'tostring', 'tointeger', 'todecimal', 'todate', 
            'todatetime', 'totime', 'toquantity', 'convertstoboolean', 'convertstodecimal',
            'convertstointeger', 'convertstodate', 'convertstodatetime', 'convertstotime',
            'as', 'is', 'oftype'
        ]

    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        type_functions = {
            'toboolean', 'tostring', 'tointeger', 'todecimal', 'todate', 
            'todatetime', 'totime', 'toquantity', 'convertstoboolean', 'convertstodecimal',
            'convertstointeger', 'convertstodate', 'convertstodatetime', 'convertstotime',
            'convertsto'
        }
        return function_name.lower() in type_functions
    
    def handle_function(self, func_name: str, base_expr: str, func_node) -> str:
        """
        Handle type conversion function and return SQL expression.
        
        Args:
            func_name: Name of the function to handle
            base_expr: Base SQL expression to apply function to
            func_node: Function AST node with arguments
            
        Returns:
            SQL expression for the function result
        """
        func_name = func_name.lower()
        
        if func_name == 'toboolean':
            return self._handle_toboolean(base_expr, func_node)
        elif func_name == 'tostring':
            return self._handle_tostring(base_expr, func_node)
        elif func_name == 'tointeger':
            return self._handle_tointeger(base_expr, func_node)
        elif func_name == 'todecimal':
            return self._handle_todecimal(base_expr, func_node)
        elif func_name == 'todate':
            return self._handle_todate(base_expr, func_node)
        elif func_name == 'todatetime':
            return self._handle_todatetime(base_expr, func_node)
        elif func_name == 'totime':
            return self._handle_totime(base_expr, func_node)
        elif func_name == 'toquantity':
            return self._handle_toquantity(base_expr, func_node)
        elif func_name == 'convertstoboolean':
            return self._handle_convertstoboolean(base_expr, func_node)
        elif func_name == 'convertstodecimal':
            return self._handle_convertstodecimal(base_expr, func_node)
        elif func_name == 'convertstointeger':
            return self._handle_convertstointeger(base_expr, func_node)
        elif func_name == 'convertstodate':
            return self._handle_convertstodate(base_expr, func_node)
        elif func_name == 'convertstodatetime':
            return self._handle_convertstodatetime(base_expr, func_node)
        elif func_name == 'convertstotime':
            return self._handle_convertstotime(base_expr, func_node)
        elif func_name == 'convertsto':
            return self._handle_convertsto(base_expr, func_node)
        else:
            raise ValueError(f"Unsupported type conversion function: {func_name}")
    
    def _handle_toboolean(self, base_expr: str, func_node) -> str:
        """Handle toBoolean() function."""
        # toBoolean() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"toBoolean() requires no arguments, got {len(func_node.args)}")

        # Phase 3 Week 8: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'toboolean'):
            try:
                return self.generator._generate_toboolean_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for toBoolean(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # toBoolean() - converts the input collection to boolean values
        # FHIRPath specification: toBoolean() - converts strings, numbers to boolean
        return f"""
        CASE 
            -- Check if collection is null or empty
            WHEN {base_expr} IS NULL THEN NULL
            -- Handle array/collection case
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                {self._generate_boolean_array_logic(base_expr, is_conversion=True)}
            -- Single value case (not array)
            ELSE 
                {self._generate_boolean_single_logic(base_expr, is_conversion=True)}
        END
        """
    
    def _handle_tostring(self, base_expr: str, func_node) -> str:
        """Handle toString() function."""
        # toString() function - convert value to string
        if len(func_node.args) != 0:
            raise ValueError("toString() function takes no arguments")
        
        # PHASE 5M: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'tostring'):
            try:
                return self.generator._generate_tostring_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for tostring(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # toString() should truncate decimal values for integer-like results  
        # For numeric results from arithmetic operations, always truncate to integer
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN
                CAST(CAST({base_expr} AS INTEGER) AS VARCHAR)
            ELSE NULL
        END
        """
    
    def _handle_tointeger(self, base_expr: str, func_node) -> str:
        """Handle toInteger() function."""
        # toInteger() function - convert string to integer
        if len(func_node.args) != 0:
            raise ValueError("toInteger() function takes no arguments")
        
        # PHASE 5M: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'tointeger'):
            try:
                return self.generator._generate_tointeger_with_cte(base_expr)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for tointeger(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # Handle the case where base_expr is already extracted
        if 'json_extract' in base_expr or 'SUBSTRING' in base_expr:
            # If already a string value, cast directly
            base_value = base_expr
        else:
            # Extract as string first
            base_value = self.generator.extract_json_field(base_expr, '$')
        
        return f"""
        CASE 
            WHEN {base_expr} IS NOT NULL THEN CAST({base_value} AS INTEGER)
            ELSE NULL
        END
        """
    
    def _handle_todecimal(self, base_expr: str, func_node) -> str:
        """Handle toDecimal() function."""
        # Phase 3 Week 8: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'todecimal'):
            try:
                return self.generator._generate_todecimal_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for toDecimal(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # toDecimal() - converts the input collection to decimal values
        # FHIRPath specification: toDecimal() - converts strings, numbers to decimal
        
        # toDecimal() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"toDecimal() requires no arguments, got {len(func_node.args)}")
        
        return f"""
        CASE 
            -- Check if collection is null or empty
            WHEN {base_expr} IS NULL THEN NULL
            -- Handle array/collection case
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT 
                            CASE 
                                -- String numeric values - parse as decimal
                                WHEN CAST(value AS VARCHAR) REGEXP '^-?[0-9]+\\.?[0-9]*$' THEN 
                                    CAST(CAST(value AS VARCHAR) AS DOUBLE)
                                -- Already numeric values
                                WHEN {self.generator.get_json_type('value')} IN ('NUMBER', 'INTEGER', 'DOUBLE') THEN
                                    CAST(value AS DOUBLE)
                                ELSE NULL
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                END
            -- Single value case (not array)
            ELSE 
                CASE 
                    -- String numeric values - parse as decimal
                    WHEN CAST({base_expr} AS VARCHAR) REGEXP '^-?[0-9]+\\.?[0-9]*$' THEN 
                        CAST(CAST({base_expr} AS VARCHAR) AS DOUBLE)
                    -- Already numeric values
                    WHEN {self.generator.get_json_type(base_expr)} IN ('NUMBER', 'INTEGER', 'DOUBLE') THEN
                        CAST({base_expr} AS DOUBLE)
                    ELSE NULL
                END
        END
        """
    
    def _handle_todate(self, base_expr: str, func_node) -> str:
        """Handle toDate() function."""
        # Phase 3 Week 9: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'todate'):
            try:
                return self.generator._generate_todate_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for toDate(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # toDate() - converts the input collection to date values
        if len(func_node.args) != 0:
            raise ValueError(f"toDate() requires no arguments, got {len(func_node.args)}")
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT 
                            CASE 
                                WHEN CAST(value AS VARCHAR) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' THEN 
                                    CAST(value AS VARCHAR)
                                ELSE NULL
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                END
            ELSE 
                CASE 
                    WHEN CAST({base_expr} AS VARCHAR) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' THEN 
                        CAST({base_expr} AS VARCHAR)
                    ELSE NULL
                END
        END
        """
    
    def _handle_todatetime(self, base_expr: str, func_node) -> str:
        """Handle toDateTime() function."""
        # Phase 3 Week 9: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'todatetime'):
            try:
                return self.generator._generate_todatetime_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for toDateTime(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # toDateTime() - converts the input collection to datetime values
        if len(func_node.args) != 0:
            raise ValueError(f"toDateTime() requires no arguments, got {len(func_node.args)}")
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT 
                            CASE 
                                WHEN CAST(value AS VARCHAR) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' THEN 
                                    CAST(value AS VARCHAR)
                                ELSE NULL
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                END
            ELSE 
                CASE 
                    WHEN CAST({base_expr} AS VARCHAR) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' THEN 
                        CAST({base_expr} AS VARCHAR)
                    ELSE NULL
                END
        END
        """
    
    def _handle_totime(self, base_expr: str, func_node) -> str:
        """Handle toTime() function."""
        # Phase 3 Week 9: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'totime'):
            try:
                return self.generator._generate_totime_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for toTime(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # toTime() - converts the input collection to time values
        if len(func_node.args) != 0:
            raise ValueError(f"toTime() requires no arguments, got {len(func_node.args)}")
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN NULL
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT 
                            CASE 
                                WHEN CAST(value AS VARCHAR) REGEXP '^[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' THEN 
                                    CAST(value AS VARCHAR)
                                ELSE NULL
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                END
            ELSE 
                CASE 
                    WHEN CAST({base_expr} AS VARCHAR) REGEXP '^[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' THEN 
                        CAST({base_expr} AS VARCHAR)
                    ELSE NULL
                END
        END
        """
    
    def _handle_convertstoboolean(self, base_expr: str, func_node) -> str:
        """Handle convertsToBoolean() function."""
        # Phase 3 Week 8: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'convertstoboolean'):
            try:
                return self.generator._generate_convertstoboolean_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for convertsToBoolean(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # convertsToBoolean() - tests if the input collection can be converted to boolean
        # convertsToBoolean() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToBoolean() requires no arguments, got {len(func_node.args)}")
        
        return f"""
        CASE 
            -- Check if collection is null or empty
            WHEN {base_expr} IS NULL THEN false
            -- Handle array/collection case
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                {self._generate_boolean_array_logic(base_expr, is_conversion=False)}
            -- Single value case (not array)
            ELSE 
                {self._generate_boolean_single_logic(base_expr, is_conversion=False)}
        END
        """
    
    def _handle_convertstodecimal(self, base_expr: str, func_node) -> str:
        """Handle convertsToDecimal() function."""
        # Phase 3 Week 8: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'convertstodecimal'):
            try:
                return self.generator._generate_convertstodecimal_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for convertsToDecimal(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # convertsToDecimal() - tests if the input collection can be converted to decimal
        # convertsToDecimal() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToDecimal() requires no arguments, got {len(func_node.args)}")
        
        return f"""
        CASE 
            -- Check if collection is null or empty
            WHEN {base_expr} IS NULL THEN false
            -- Handle array/collection case
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN false
                    ELSE (
                        SELECT 
                            CASE 
                                -- String numeric values - check if can parse as decimal
                                WHEN CAST(value AS VARCHAR) REGEXP '^-?[0-9]+\\.?[0-9]*$' THEN true
                                -- Already numeric values
                                WHEN {self.generator.get_json_type('value')} IN ('NUMBER', 'INTEGER', 'DOUBLE') THEN true
                                ELSE false
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                END
            -- Single value case (not array)
            ELSE 
                CASE 
                    -- String numeric values - check if can parse as decimal
                    WHEN CAST({base_expr} AS VARCHAR) REGEXP '^-?[0-9]+\\.?[0-9]*$' THEN true
                    -- Already numeric values
                    WHEN {self.generator.get_json_type(base_expr)} IN ('NUMBER', 'INTEGER', 'DOUBLE') THEN true
                    ELSE false
                END
        END
        """
    
    def _handle_convertstointeger(self, base_expr: str, func_node) -> str:
        """Handle convertsToInteger() function."""
        # Phase 3 Week 8: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'convertstointeger'):
            try:
                return self.generator._generate_convertstointeger_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for convertsToInteger(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # convertsToInteger() - tests if the input collection can be converted to integer
        # convertsToInteger() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToInteger() requires no arguments, got {len(func_node.args)}")
        
        return f"""
        CASE 
            -- Check if collection is null or empty
            WHEN {base_expr} IS NULL THEN false
            -- Handle array/collection case
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN false
                    ELSE (
                        SELECT 
                            CASE 
                                -- String integer values - check if can parse as integer (no decimals)
                                WHEN CAST(value AS VARCHAR) REGEXP '^-?[0-9]+$' THEN true
                                -- Already integer values
                                WHEN {self.generator.get_json_type('value')} = 'INTEGER' THEN true
                                ELSE false
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                END
            -- Single value case (not array)
            ELSE 
                CASE 
                    -- String integer values - check if can parse as integer (no decimals)
                    WHEN CAST({base_expr} AS VARCHAR) REGEXP '^-?[0-9]+$' THEN true
                    -- Already integer values
                    WHEN {self.generator.get_json_type(base_expr)} = 'INTEGER' THEN true
                    ELSE false
                END
        END
        """
    
    def _handle_convertstodate(self, base_expr: str, func_node) -> str:
        """Handle convertsToDate() function."""
        # Phase 3 Week 9: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'convertstodate'):
            try:
                return self.generator._generate_convertstodate_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for convertsToDate(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # convertsToDate() - tests if the input collection can be converted to date values
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToDate() requires no arguments, got {len(func_node.args)}")
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN false
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN false
                    ELSE (
                        SELECT 
                            CASE 
                                WHEN CAST(value AS VARCHAR) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' THEN true
                                ELSE false
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                END
            ELSE 
                CASE 
                    WHEN CAST({base_expr} AS VARCHAR) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' THEN true
                    ELSE false
                END
        END
        """
    
    def _handle_convertstodatetime(self, base_expr: str, func_node) -> str:
        """Handle convertsToDateTime() function."""
        # Phase 3 Week 9: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'convertstodatetime'):
            try:
                return self.generator._generate_convertstodatetime_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for convertsToDateTime(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # convertsToDateTime() - tests if the input collection can be converted to datetime values
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToDateTime() requires no arguments, got {len(func_node.args)}")
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN false
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN false
                    ELSE (
                        SELECT 
                            CASE 
                                WHEN CAST(value AS VARCHAR) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' THEN true
                                ELSE false
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                END
            ELSE 
                CASE 
                    WHEN CAST({base_expr} AS VARCHAR) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' THEN true
                    ELSE false
                END
        END
        """
    
    def _handle_convertstotime(self, base_expr: str, func_node) -> str:
        """Handle convertsToTime() function."""
        # Phase 3 Week 9: CTE Implementation with Feature Flag
        if self.generator.enable_cte and self.generator._should_use_cte_unified(base_expr, 'convertstotime'):
            try:
                return self.generator._generate_convertstotime_with_cte(base_expr, func_node)
            except Exception as e:
                # Fallback to original implementation if CTE generation fails
                print(f"CTE generation failed for convertsToTime(), falling back to subqueries: {e}")
        
        # Original implementation (fallback)
        # convertsToTime() - tests if the input collection can be converted to time values
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToTime() requires no arguments, got {len(func_node.args)}")
        
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN false
            WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN false
                    ELSE (
                        SELECT 
                            CASE 
                                WHEN CAST(value AS VARCHAR) REGEXP '^[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' THEN true
                                ELSE false
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                END
            ELSE 
                CASE 
                    WHEN CAST({base_expr} AS VARCHAR) REGEXP '^[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}' THEN true
                    ELSE false
                END
        END
        """
    
    def _handle_toquantity(self, base_expr: str, func_node) -> str:
        """
        Handle toQuantity() function - converts input to FHIR Quantity type.
        
        Args:
            base_expr: SQL expression for the input value
            func_node: AST node for the function call
            
        Returns:
            SQL expression that converts input to Quantity
        """
        # Validate no arguments
        if hasattr(func_node, 'args') and func_node.args:
            raise ValueError("toQuantity() function takes no arguments")
        
        # Generate quantity conversion logic based on FHIR specification
        if self.dialect.name == "POSTGRESQL":
            return f"""
            CASE 
                -- Already a FHIR Quantity object (has both value and unit)
                WHEN {base_expr} IS NOT NULL AND 
                     jsonb_typeof({base_expr}) = 'object' AND
                     ({base_expr} ? 'value') AND 
                     ({base_expr} ? 'unit') THEN
                    {base_expr}
                
                -- Simple numeric value  
                WHEN {base_expr} IS NOT NULL AND jsonb_typeof({base_expr}) = 'number' THEN
                    jsonb_build_object(
                        'value', ({base_expr})::decimal,
                        'system', 'http://unitsofmeasure.org',
                        'code', '1',
                        'unit', '1'
                    )
                
                -- String with basic numeric parsing
                WHEN {base_expr} IS NOT NULL AND 
                     jsonb_typeof({base_expr}) = 'string' AND 
                     ({base_expr}#>>'{{}}')::text ~ '^[0-9]+\\.?[0-9]*$' THEN
                    jsonb_build_object(
                        'value', (({base_expr}#>>'{{}}')::text)::decimal,
                        'system', 'http://unitsofmeasure.org', 
                        'code', '1',
                        'unit', '1'
                    )
                
                ELSE NULL
            END
            """
        else:  # DuckDB
            return f"""
            CASE 
                -- Already a FHIR Quantity object (has both value and unit)
                WHEN {base_expr} IS NOT NULL AND 
                     json_type({base_expr}) = 'OBJECT' AND
                     json_extract({base_expr}, '$.value') IS NOT NULL AND 
                     json_extract({base_expr}, '$.unit') IS NOT NULL THEN
                    {base_expr}
                
                -- Simple numeric value
                WHEN {base_expr} IS NOT NULL AND json_type({base_expr}) = 'NUMBER' THEN
                    json_object(
                        'value', CAST({base_expr} AS DECIMAL),
                        'system', 'http://unitsofmeasure.org',
                        'code', '1', 
                        'unit', '1'
                    )
                
                -- String with numeric parsing
                WHEN {base_expr} IS NOT NULL AND 
                     json_type({base_expr}) = 'STRING' AND 
                     regexp_matches(json_extract_string({base_expr}, '$'), '^[0-9]+\\.?[0-9]*$') THEN
                    json_object(
                        'value', CAST(json_extract_string({base_expr}, '$') AS DECIMAL),
                        'system', 'http://unitsofmeasure.org',
                        'code', '1',
                        'unit', '1'
                    )
                
                ELSE NULL
            END
            """
    
    def _handle_convertsto(self, base_expr: str, func_node) -> str:
        """
        Handle convertsTo() function - generic type conversion checker.
        
        Args:
            base_expr: SQL expression for the input value
            func_node: AST node for the function call
            
        Returns:
            SQL expression that tests if input can be converted to specified type
        """
        # convertsTo() takes exactly one argument (the type name)
        if len(func_node.args) != 1:
            raise ValueError(f"convertsTo() requires exactly one argument, got {len(func_node.args)}")
        
        # Get the type argument
        type_arg = func_node.args[0]
        
        # The type should be a literal string
        if hasattr(type_arg, 'value'):
            target_type = type_arg.value.lower()
        else:
            raise ValueError("convertsTo() type argument must be a literal string")
        
        # Create a mock func_node with no args for specific convertsTo* functions
        class MockFuncNode:
            args = []
        
        mock_func_node = MockFuncNode()
        
        # Delegate to specific convertsTo* functions based on type
        if target_type == 'boolean':
            return self._handle_convertstoboolean(base_expr, mock_func_node)
        elif target_type == 'integer':
            return self._handle_convertstointeger(base_expr, mock_func_node)
        elif target_type == 'decimal':
            return self._handle_convertstodecimal(base_expr, mock_func_node)
        elif target_type == 'date':
            return self._handle_convertstodate(base_expr, mock_func_node)
        elif target_type == 'datetime':
            return self._handle_convertstodatetime(base_expr, mock_func_node)
        elif target_type == 'time':
            return self._handle_convertstotime(base_expr, mock_func_node)
        elif target_type == 'string':
            # Everything can be converted to string
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN false
                ELSE true
            END
            """
        elif target_type == 'quantity':
            # Basic quantity conversion check
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN false
                WHEN {self.generator.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN false
                        ELSE (
                            SELECT 
                                CASE 
                                    WHEN {self.generator.get_json_type('value')} = 'NUMBER' THEN true
                                    WHEN {self.generator.get_json_type('value')} = 'STRING' AND 
                                         CAST(value AS VARCHAR) REGEXP '^[0-9]+\\.?[0-9]*$' THEN true
                                    ELSE false
                                END
                            FROM {self.generator.iterate_json_array(base_expr, '$')}
                            LIMIT 1
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {self.generator.get_json_type(base_expr)} = 'NUMBER' THEN true
                        WHEN {self.generator.get_json_type(base_expr)} = 'STRING' AND 
                             CAST({base_expr} AS VARCHAR) REGEXP '^[0-9]+\\.?[0-9]*$' THEN true
                        ELSE false
                    END
            END
            """
        else:
            # For unknown types, return false
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN false
                ELSE false
            END
            """