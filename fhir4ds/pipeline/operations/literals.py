"""
Literal value operations for FHIRPath pipelines.

These operations handle literal values like strings, numbers, and booleans
that appear in FHIRPath expressions.
"""

from typing import Any, Optional, List
import json
import logging
from ..core.base import PipelineOperation, SQLState, ExecutionContext, ContextMode

logger = logging.getLogger(__name__)

class LiteralOperation(PipelineOperation[SQLState]):
    """
    Operation for literal values in FHIRPath expressions.
    
    This handles literals like:
    - 'hello world' (string)
    - 42 (integer)
    - 3.14 (decimal)
    - true (boolean)
    - null (null value)
    - @2023-01-01 (date)
    - @T14:30:00 (time)
    - @2023-01-01T14:30:00 (datetime)
    
    This replaces the literal handling from SQLGenerator.visit_literal().
    """
    
    def __init__(self, value: Any, value_type: str):
        """
        Initialize literal operation.
        
        Args:
            value: The literal value
            value_type: Type of the literal ('string', 'integer', 'decimal', 'boolean', 'null', 'date', 'time', 'datetime')
        """
        self.value = value
        self.value_type = value_type
        self._validate_literal()
    
    def _validate_literal(self) -> None:
        """Validate literal value and type."""
        valid_types = {'string', 'integer', 'decimal', 'boolean', 'null', 'date', 'time', 'datetime'}
        if self.value_type not in valid_types:
            raise ValueError(f"Invalid literal type: {self.value_type}. Must be one of {valid_types}")
        
        # Type-specific validation
        if self.value_type == 'integer':
            if not isinstance(self.value, (int, str)) or (isinstance(self.value, str) and not self.value.lstrip('-').isdigit()):
                raise ValueError(f"Invalid integer literal: {self.value}")
        elif self.value_type == 'decimal':
            if not isinstance(self.value, (int, float, str)):
                raise ValueError(f"Invalid decimal literal: {self.value}")
            try:
                float(self.value)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid decimal literal: {self.value}")
        elif self.value_type == 'boolean':
            if not isinstance(self.value, (bool, str)) or (isinstance(self.value, str) and self.value.lower() not in ['true', 'false']):
                raise ValueError(f"Invalid boolean literal: {self.value}")
        elif self.value_type == 'null':
            if self.value is not None and self.value != 'null':
                raise ValueError("Null literal must have None value or 'null' string")
        elif self.value_type in ['date', 'time', 'datetime']:
            if not isinstance(self.value, str):
                raise ValueError(f"Date/time literal must be a string: {self.value}")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute literal value operation.
        
        Args:
            input_state: Current SQL state (not used for literals)
            context: Execution context with dialect
            
        Returns:
            New SQL state with literal SQL
        """
        sql_literal = self._generate_literal_sql(context)
        
        return input_state.evolve(
            sql_fragment=sql_literal,
            is_collection=False,  # Literals are always single values
            path_context="$",     # Literals reset path context
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _generate_literal_sql(self, context: ExecutionContext) -> str:
        """
        Generate SQL for literal value based on type and dialect.
        
        Args:
            context: Execution context with dialect
            
        Returns:
            SQL representation of the literal
        """
        dialect_name = getattr(context.dialect, 'name', 'unknown').upper()
        
        if self.value_type == 'string':
            # Escape single quotes and wrap in quotes
            escaped_value = str(self.value).replace("'", "''")
            return f"'{escaped_value}'"
        
        elif self.value_type == 'integer':
            return str(int(self.value))
        
        elif self.value_type == 'decimal':
            return str(float(self.value))
        
        elif self.value_type == 'boolean':
            bool_value = self.value
            if isinstance(self.value, str):
                bool_value = self.value.lower() == 'true'
            
            # Use dialect-specific boolean literals
            if dialect_name == 'POSTGRESQL':
                return 'true' if bool_value else 'false'
            else:
                return 'TRUE' if bool_value else 'FALSE'
        
        elif self.value_type == 'null':
            return 'NULL'
        
        elif self.value_type == 'date':
            # Generate date literal based on dialect
            date_str = str(self.value).strip('@')  # Remove @ prefix if present
            if dialect_name == 'POSTGRESQL':
                return f"DATE '{date_str}'"
            else:
                return f"'{date_str}'"
        
        elif self.value_type == 'time':
            # Generate time literal based on dialect
            time_str = str(self.value).strip('@T')  # Remove @T prefix if present
            if dialect_name == 'POSTGRESQL':
                return f"TIME '{time_str}'"
            else:
                return f"'{time_str}'"
        
        elif self.value_type == 'datetime':
            # Generate datetime literal based on dialect
            datetime_str = str(self.value).strip('@')  # Remove @ prefix if present
            if dialect_name == 'POSTGRESQL':
                return f"TIMESTAMP '{datetime_str}'"
            else:
                return f"'{datetime_str}'"
        
        else:
            raise ValueError(f"Unknown literal type: {self.value_type}")
    
    def optimize_for_dialect(self, dialect) -> 'LiteralOperation':
        """
        Optimize literal for specific dialect.
        
        Args:
            dialect: Target database dialect
            
        Returns:
            Potentially optimized literal operation (usually unchanged)
        """
        # Literals are generally dialect-agnostic
        # Dialect-specific optimizations would be minimal
        return self
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        if self.value_type == 'string':
            # Truncate long strings for readability
            display_value = str(self.value)
            if len(display_value) > 20:
                display_value = display_value[:17] + "..."
            return f"literal('{display_value}')"
        elif self.value_type in ['date', 'time', 'datetime']:
            return f"literal(@{self.value})"
        else:
            return f"literal({self.value})"
    
    def validate_preconditions(self, input_state: SQLState, context: ExecutionContext) -> None:
        """
        Validate literal preconditions.
        
        Literals have no preconditions since they're self-contained.
        
        Args:
            input_state: Input SQL state (unused)
            context: Execution context (unused)
        """
        pass  # Literals have no preconditions
    
    def estimate_complexity(self, input_state: SQLState, context: ExecutionContext) -> int:
        """
        Estimate complexity of literal operation.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            Complexity score (always 0 - literals are trivial)
        """
        return 0  # Literals are the simplest possible operation

class CollectionLiteralOperation(PipelineOperation[SQLState]):
    """
    Operation for collection literals like arrays.
    
    This handles collection literals such as:
    - [] (empty array)
    - [1, 2, 3] (array of integers)
    - ['a', 'b', 'c'] (array of strings)
    - [true, false] (array of booleans)
    """
    
    def __init__(self, elements: List[Any]):
        """
        Initialize collection literal operation.
        
        Args:
            elements: List of elements in the collection
        """
        self.elements = elements or []
        self._validate_elements()
    
    def _validate_elements(self) -> None:
        """Validate collection elements."""
        if not isinstance(self.elements, list):
            raise ValueError("Collection elements must be a list")
        
        # All elements should be of compatible types for SQL
        for i, element in enumerate(self.elements):
            if not isinstance(element, (str, int, float, bool, type(None), LiteralOperation)):
                raise ValueError(f"Invalid element type at index {i}: {type(element)}")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute collection literal operation.
        
        Args:
            input_state: Current SQL state
            context: Execution context with dialect
            
        Returns:
            New SQL state with collection literal SQL
        """
        sql_fragment = self._generate_collection_sql(context)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True,
            path_context="$",
            context_mode=ContextMode.COLLECTION
        )
    
    def _generate_collection_sql(self, context: ExecutionContext) -> str:
        """
        Generate SQL for collection literal based on dialect.
        
        Args:
            context: Execution context
            
        Returns:
            SQL expression for collection literal
        """
        dialect_name = getattr(context.dialect, 'name', 'unknown').upper()
        
        if not self.elements:
            # Empty collection
            if dialect_name == 'DUCKDB':
                return "json_array()"
            elif dialect_name == 'POSTGRESQL':
                return "'[]'::jsonb"
            else:
                return "json_array()"
        
        # Non-empty collection
        element_sqls = []
        for element in self.elements:
            if isinstance(element, LiteralOperation):
                element_sql = element._generate_literal_sql(context)
            elif isinstance(element, str):
                escaped = element.replace("'", "''")
                element_sql = f"'{escaped}'"
            elif isinstance(element, bool):
                if dialect_name == 'POSTGRESQL':
                    element_sql = 'true' if element else 'false'
                else:
                    element_sql = 'TRUE' if element else 'FALSE'
            elif element is None:
                element_sql = 'NULL'
            else:
                element_sql = str(element)
            
            element_sqls.append(element_sql)
        
        if dialect_name == 'DUCKDB':
            return f"json_array({', '.join(element_sqls)})"
        elif dialect_name == 'POSTGRESQL':
            return f"jsonb_build_array({', '.join(element_sqls)})"
        else:
            # Generic fallback
            return f"json_array({', '.join(element_sqls)})"
    
    def optimize_for_dialect(self, dialect) -> 'CollectionLiteralOperation':
        """Optimize collection literal for dialect."""
        return self
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        if not self.elements:
            return "collection_literal([])"
        elif len(self.elements) <= 3:
            # Show actual elements for small collections
            element_reprs = []
            for element in self.elements:
                if isinstance(element, str):
                    element_reprs.append(f"'{element}'")
                else:
                    element_reprs.append(str(element))
            return f"collection_literal([{', '.join(element_reprs)}])"
        else:
            return f"collection_literal([{len(self.elements)} elements])"
    
    def validate_preconditions(self, input_state: SQLState, context: ExecutionContext) -> None:
        """Validate collection literal preconditions."""
        pass  # Collection literals have no preconditions
    
    def estimate_complexity(self, input_state: SQLState, context: ExecutionContext) -> int:
        """Estimate complexity of collection literal."""
        return max(1, len(self.elements) // 10)  # Complexity based on element count

class QuantityLiteralOperation(PipelineOperation[SQLState]):
    """
    Operation for FHIR Quantity literals.
    
    This handles quantity literals such as:
    - 5.0 'mg' (quantity with unit)
    - 100 'kg' (integer quantity with unit)
    """
    
    def __init__(self, value: float, unit: str):
        """
        Initialize quantity literal operation.
        
        Args:
            value: Numeric value of the quantity
            unit: Unit of measurement (e.g., 'mg', 'kg', 'cm')
        """
        self.value = value
        self.unit = unit
        self._validate_quantity()
    
    def _validate_quantity(self) -> None:
        """Validate quantity components."""
        if not isinstance(self.value, (int, float)):
            raise ValueError("Quantity value must be numeric")
        
        if not isinstance(self.unit, str) or not self.unit.strip():
            raise ValueError("Quantity unit must be a non-empty string")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute quantity literal operation.
        
        Args:
            input_state: Current SQL state
            context: Execution context
            
        Returns:
            New SQL state with quantity literal SQL
        """
        sql_fragment = self._generate_quantity_sql(context)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            path_context="$",
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _generate_quantity_sql(self, context: ExecutionContext) -> str:
        """
        Generate SQL for quantity literal.
        
        Quantities are represented as JSON objects with value and unit fields.
        
        Args:
            context: Execution context
            
        Returns:
            SQL expression for quantity literal
        """
        dialect_name = getattr(context.dialect, 'name', 'unknown').upper()
        
        # Create JSON object for quantity
        quantity_obj = {
            "value": self.value,
            "unit": self.unit
        }
        
        if dialect_name == 'DUCKDB':
            return f"json('{json.dumps(quantity_obj)}')"
        elif dialect_name == 'POSTGRESQL':
            return f"'{json.dumps(quantity_obj)}'::jsonb"
        else:
            return f"'{json.dumps(quantity_obj)}'"
    
    def optimize_for_dialect(self, dialect) -> 'QuantityLiteralOperation':
        """Optimize quantity literal for dialect."""
        return self
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        return f"quantity_literal({self.value} '{self.unit}')"
    
    def validate_preconditions(self, input_state: SQLState, context: ExecutionContext) -> None:
        """Validate quantity literal preconditions."""
        pass  # Quantity literals have no preconditions
    
    def estimate_complexity(self, input_state: SQLState, context: ExecutionContext) -> int:
        """Estimate complexity of quantity literal."""
        return 1  # Slightly more complex than simple literals due to JSON creation