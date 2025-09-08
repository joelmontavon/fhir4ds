"""
FHIRPath Core Module

Contains the core FHIRPath processing components.
Legacy SQL generation has been replaced by the pipeline architecture.
"""

from .choice_types import fhir_choice_types
from .constants import FHIR_PRIMITIVE_TYPES_AS_STRING, SQL_OPERATORS
# Legacy components replaced by pipeline system
from .sql_builders import (
    QueryItem, Literal, Field, Func, Expr, SelectItem, 
    Table, Join, Select, FromItem, TableRef, Subquery, 
    Cte, Union
)

__all__ = [
    "fhir_choice_types", "FHIR_PRIMITIVE_TYPES_AS_STRING", "SQL_OPERATORS",
    # Builder classes (still used by some components)
    "QueryItem", "Literal", "Field", "Func", "Expr", "SelectItem", 
    "Table", "Join", "Select", "FromItem", "TableRef", "Subquery", 
    "Cte", "Union"
]