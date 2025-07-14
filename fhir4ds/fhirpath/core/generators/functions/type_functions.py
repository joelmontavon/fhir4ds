"""
Type conversion function handlers for FHIRPath expressions.

This module handles type conversion operations like toBoolean, toString, toInteger,
toDecimal, toDate, toDateTime, toTime, convertsToBoolean, convertsToDecimal,
convertsToInteger, convertsToDate, convertsToDateTime, convertsToTime.
"""

from typing import List, Any, Optional


class TypeFunctionHandler:
    """Handles type conversion function processing for FHIRPath to SQL conversion."""
    
    def __init__(self, generator):
        """
        Initialize the type function handler.
        
        Args:
            generator: Reference to main SQLGenerator for complex operations
        """
        self.generator = generator
        self.dialect = generator.dialect
        
    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        type_functions = {
            'toboolean', 'tostring', 'tointeger', 'todecimal', 'todate', 
            'todatetime', 'totime', 'convertstoboolean', 'convertstodecimal',
            'convertstointeger', 'convertstodate', 'convertstodatetime', 'convertstotime'
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
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN NULL
                    ELSE (
                        SELECT 
                            CASE 
                                -- String 'true'/'false' case
                                WHEN LOWER(CAST(value AS VARCHAR)) = 'true' THEN true
                                WHEN LOWER(CAST(value AS VARCHAR)) = 'false' THEN false
                                -- Numeric 0/1 case 
                                WHEN CAST(value AS DOUBLE) = 1.0 THEN true
                                WHEN CAST(value AS DOUBLE) = 0.0 THEN false
                                ELSE NULL
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                END
            -- Single value case (not array)
            ELSE 
                CASE 
                    -- String 'true'/'false' case
                    WHEN LOWER(CAST({base_expr} AS VARCHAR)) = 'true' THEN true
                    WHEN LOWER(CAST({base_expr} AS VARCHAR)) = 'false' THEN false
                    -- Numeric 0/1 case
                    WHEN CAST({base_expr} AS DOUBLE) = 1.0 THEN true
                    WHEN CAST({base_expr} AS DOUBLE) = 0.0 THEN false
                    ELSE NULL
                END
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
                CASE 
                    WHEN {self.generator.get_json_array_length(base_expr)} = 0 THEN false
                    ELSE (
                        SELECT 
                            CASE 
                                -- String 'true'/'false' case
                                WHEN LOWER(CAST(value AS VARCHAR)) IN ('true', 'false') THEN true
                                -- Numeric 0/1 case 
                                WHEN CAST(value AS DOUBLE) IN (1.0, 0.0) THEN true
                                ELSE false
                            END
                        FROM {self.generator.iterate_json_array(base_expr, '$')}
                        LIMIT 1
                    )
                END
            -- Single value case (not array)
            ELSE 
                CASE 
                    -- String 'true'/'false' case
                    WHEN LOWER(CAST({base_expr} AS VARCHAR)) IN ('true', 'false') THEN true
                    -- Numeric 0/1 case
                    WHEN CAST({base_expr} AS DOUBLE) IN (1.0, 0.0) THEN true
                    ELSE false
                END
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