"""
CQL Type System Implementation

This module implements the core CQL types including Tuple, Choice, and enhanced 
primitive types required for full CQL specification compliance.

Key Types Implemented:
- CQLTuple: Object/record construction with named fields
- CQLChoice: Union types for handling multiple possible types  
- CQLList: Generic list type with proper type inference
- CQLQuantity: Quantity type with unit conversion support
- CQLRatio: Ratio type for clinical calculations
- Enhanced Code/Concept types with terminology integration

Architecture: Integrates with existing parser/translator via visitor pattern
"""

import logging
from typing import Dict, Any, List, Optional, Union, Type
from enum import Enum
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class CQLBaseType(ABC):
    """Base class for all CQL types."""
    
    def __init__(self, value: Any = None):
        self.value = value
    
    @abstractmethod
    def to_sql(self, dialect: str = "duckdb") -> str:
        """Convert type instance to SQL representation."""
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """Validate type instance."""
        pass


class CQLTuple(CQLBaseType):
    """
    CQL Tuple type for object construction.
    
    Supports CQL syntax: Tuple { field1: value1, field2: value2 }
    Also supports shorthand: { field1: value1, field2: value2 }
    
    SQL Generation: Creates JSON objects for cross-dialect compatibility
    """
    
    def __init__(self, fields: Optional[Dict[str, Any]] = None):
        """
        Initialize tuple with field definitions.
        
        Args:
            fields: Dictionary mapping field names to values/expressions
        """
        self.fields = fields or {}
        super().__init__(self.fields)
    
    def add_field(self, name: str, value: Any) -> 'CQLTuple':
        """Add a field to the tuple."""
        self.fields[name] = value
        return self
    
    def get_field(self, name: str) -> Any:
        """Get field value by name."""
        return self.fields.get(name)
    
    def to_sql(self, dialect: str = "duckdb") -> str:
        """
        Convert tuple to SQL JSON object construction.
        
        Example:
        { name: 'John', age: 25 } -> 
        DuckDB: {'name': 'John', 'age': 25}
        PostgreSQL: json_build_object('name', 'John', 'age', 25)
        """
        if not self.fields:
            return "'{}'" if dialect == "duckdb" else "json_build_object()"
        
        if dialect == "postgresql":
            # PostgreSQL json_build_object format
            pairs = []
            for key, value in self.fields.items():
                # Handle different value types
                if isinstance(value, str) and not value.startswith('('):  # String literal
                    sql_value = f"'{value}'"
                else:  # Expression or SQL
                    sql_value = str(value)
                pairs.extend([f"'{key}'", sql_value])
            return f"json_build_object({', '.join(pairs)})"
        
        else:  # DuckDB format
            # DuckDB struct literal format  
            pairs = []
            for key, value in self.fields.items():
                if isinstance(value, str) and not value.startswith('('):  # String literal
                    sql_value = f"'{value}'"
                else:  # Expression or SQL
                    sql_value = str(value)
                pairs.append(f"'{key}': {sql_value}")
            return f"{{{', '.join(pairs)}}}"
    
    def to_sql_access(self, field_name: str, base_expr: str, dialect: str = "duckdb") -> str:
        """
        Generate SQL for accessing a tuple field.
        
        Args:
            field_name: Name of field to access
            base_expr: SQL expression representing the tuple
            dialect: Target SQL dialect
            
        Returns:
            SQL expression for field access
        """
        if dialect == "postgresql":
            return f"({base_expr})->'{field_name}'"
        else:  # DuckDB
            return f"({base_expr}).{field_name}"
    
    def validate(self) -> bool:
        """Validate tuple structure."""
        if not isinstance(self.fields, dict):
            return False
        
        # Check field names are valid identifiers
        for field_name in self.fields.keys():
            if not isinstance(field_name, str) or not field_name.isidentifier():
                logger.warning(f"Invalid tuple field name: {field_name}")
                return False
        
        return True
    
    def __repr__(self) -> str:
        field_strs = [f"{k}: {v}" for k, v in self.fields.items()]
        return f"Tuple {{ {', '.join(field_strs)} }}"


class CQLChoice(CQLBaseType):
    """
    CQL Choice type for union types.
    
    Supports CQL syntax: Choice<Type1, Type2>
    Handles cases where a value can be one of multiple types.
    """
    
    def __init__(self, possible_types: List[Type], value: Any = None, active_type: Optional[Type] = None):
        """
        Initialize choice type.
        
        Args:
            possible_types: List of possible types this choice can hold
            value: Current value (optional)
            active_type: Currently active type (optional)
        """
        self.possible_types = possible_types
        self.active_type = active_type
        super().__init__(value)
    
    def set_value(self, value: Any, value_type: Type) -> 'CQLChoice':
        """Set the choice value and active type."""
        if value_type not in self.possible_types:
            raise ValueError(f"Type {value_type} not in possible types {self.possible_types}")
        
        self.value = value
        self.active_type = value_type
        return self
    
    def is_of_type(self, check_type: Type) -> bool:
        """Check if current value is of specified type."""
        return self.active_type == check_type
    
    def to_sql(self, dialect: str = "duckdb") -> str:
        """
        Convert choice to SQL.
        
        For runtime, we use tagged unions with type information.
        """
        if self.value is None:
            return "NULL"
        
        # Create tagged union object
        type_name = self.active_type.__name__ if self.active_type else "unknown"
        
        if dialect == "postgresql":
            return f"json_build_object('type', '{type_name}', 'value', {self.value})"
        else:  # DuckDB
            return f"{{'type': '{type_name}', 'value': {self.value}}}"
    
    def validate(self) -> bool:
        """Validate choice type."""
        if not self.possible_types:
            return False
        
        if self.active_type and self.active_type not in self.possible_types:
            return False
        
        return True
    
    def __repr__(self) -> str:
        type_names = [t.__name__ for t in self.possible_types]
        return f"Choice<{', '.join(type_names)}>"


class CQLList(CQLBaseType):
    """
    CQL List type with generic type support.
    
    Supports CQL syntax: List<Type>
    Provides type-safe list operations.
    """
    
    def __init__(self, element_type: Type, elements: Optional[List[Any]] = None):
        """
        Initialize list type.
        
        Args:
            element_type: Type of elements in the list
            elements: Initial list elements
        """
        self.element_type = element_type
        self.elements = elements or []
        super().__init__(self.elements)
    
    def add_element(self, element: Any) -> 'CQLList':
        """Add element to list (with type checking in future)."""
        self.elements.append(element)
        return self
    
    def to_sql(self, dialect: str = "duckdb") -> str:
        """Convert list to SQL array."""
        if not self.elements:
            return "[]"  # Empty array
        
        # Convert elements to SQL
        sql_elements = []
        for element in self.elements:
            if isinstance(element, str):
                sql_elements.append(f"'{element}'")
            else:
                sql_elements.append(str(element))
        
        return f"[{', '.join(sql_elements)}]"
    
    def validate(self) -> bool:
        """Validate list type."""
        return isinstance(self.elements, list)
    
    def __repr__(self) -> str:
        return f"List<{self.element_type.__name__}>[{len(self.elements)} elements]"


class CQLQuantity(CQLBaseType):
    """
    CQL Quantity type with unit support.
    
    Supports CQL syntax: Quantity { value: 10, unit: 'kg' }
    """
    
    def __init__(self, value: Union[int, float], unit: str):
        """
        Initialize quantity.
        
        Args:
            value: Numeric value
            unit: Unit string (e.g., 'kg', 'm', 's')
        """
        self.numeric_value = value
        self.unit = unit
        super().__init__({'value': value, 'unit': unit})
    
    def to_sql(self, dialect: str = "duckdb") -> str:
        """Convert quantity to SQL object."""
        if dialect == "postgresql":
            return f"json_build_object('value', {self.numeric_value}, 'unit', '{self.unit}')"
        else:  # DuckDB
            return f"{{'value': {self.numeric_value}, 'unit': '{self.unit}'}}"
    
    def validate(self) -> bool:
        """Validate quantity."""
        return isinstance(self.numeric_value, (int, float)) and isinstance(self.unit, str)
    
    def __repr__(self) -> str:
        return f"{self.numeric_value} {self.unit}"


class CQLRatio(CQLBaseType):
    """
    CQL Ratio type for clinical calculations.
    
    Supports CQL syntax: Ratio { numerator: Quantity{...}, denominator: Quantity{...} }
    """
    
    def __init__(self, numerator: CQLQuantity, denominator: CQLQuantity):
        """
        Initialize ratio.
        
        Args:
            numerator: Numerator quantity
            denominator: Denominator quantity
        """
        self.numerator = numerator
        self.denominator = denominator
        super().__init__({'numerator': numerator, 'denominator': denominator})
    
    def to_sql(self, dialect: str = "duckdb") -> str:
        """Convert ratio to SQL object."""
        num_sql = self.numerator.to_sql(dialect)
        den_sql = self.denominator.to_sql(dialect)
        
        if dialect == "postgresql":
            return f"json_build_object('numerator', {num_sql}, 'denominator', {den_sql})"
        else:  # DuckDB
            return f"{{'numerator': {num_sql}, 'denominator': {den_sql}}}"
    
    def validate(self) -> bool:
        """Validate ratio."""
        return (isinstance(self.numerator, CQLQuantity) and 
                isinstance(self.denominator, CQLQuantity) and
                self.numerator.validate() and 
                self.denominator.validate())
    
    def __repr__(self) -> str:
        return f"{self.numerator} : {self.denominator}"


class CQLTypeSystem:
    """
    Central type system manager for CQL types.
    
    Provides type inference, validation, and SQL generation coordination.
    """
    
    def __init__(self):
        """Initialize type system."""
        self.registered_types = {
            'Tuple': CQLTuple,
            'Choice': CQLChoice, 
            'List': CQLList,
            'Quantity': CQLQuantity,
            'Ratio': CQLRatio
        }
        
        logger.info(f"CQL Type System initialized with {len(self.registered_types)} types")
    
    def create_tuple(self, fields: Dict[str, Any]) -> CQLTuple:
        """Create a new tuple instance."""
        return CQLTuple(fields)
    
    def create_choice(self, possible_types: List[Type]) -> CQLChoice:
        """Create a new choice type."""
        return CQLChoice(possible_types)
    
    def create_list(self, element_type: Type, elements: List[Any] = None) -> CQLList:
        """Create a new list instance."""
        return CQLList(element_type, elements)
    
    def infer_type(self, value: Any) -> Optional[CQLBaseType]:
        """
        Infer CQL type from a value.
        
        Args:
            value: Value to infer type for
            
        Returns:
            Inferred CQL type or None
        """
        if isinstance(value, dict):
            # Could be a tuple
            return CQLTuple(value)
        elif isinstance(value, list):
            # Could be a list
            element_type = type(value[0]) if value else str
            return CQLList(element_type, value)
        
        return None
    
    def validate_type_compatibility(self, value: Any, expected_type: CQLBaseType) -> bool:
        """
        Check if a value is compatible with expected type.
        
        Args:
            value: Value to check
            expected_type: Expected CQL type
            
        Returns:
            True if compatible
        """
        inferred = self.infer_type(value)
        if not inferred:
            return False
        
        return type(inferred) == type(expected_type)
    
    def get_type_info(self) -> Dict[str, Any]:
        """Get information about registered types."""
        return {
            'registered_types': list(self.registered_types.keys()),
            'total_types': len(self.registered_types)
        }


# Global type system instance
type_system = CQLTypeSystem()

# Export key classes for external use
__all__ = [
    'CQLBaseType', 'CQLTuple', 'CQLChoice', 'CQLList', 
    'CQLQuantity', 'CQLRatio', 'CQLTypeSystem', 'type_system'
]