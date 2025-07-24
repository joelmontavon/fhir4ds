"""
FHIRPath Core Module

Contains the core FHIRPath processing components including SQL generation,
choice types, constants, and the main translator.
"""

from .choice_types import fhir_choice_types
from .constants import FHIR_PRIMITIVE_TYPES_AS_STRING, SQL_OPERATORS
from .generator import SQLGenerator
from .sql_builders import (
    QueryItem, Literal, Field, Func, Expr, SelectItem, 
    Table, Join, Select, FromItem, TableRef, Subquery, 
    Cte, Union
)
from .translator import FHIRPathToSQL

__all__ = [
    "fhir_choice_types", "FHIR_PRIMITIVE_TYPES_AS_STRING", "SQL_OPERATORS",
    "SQLGenerator", "FHIRPathToSQL",
    # Builder classes
    "QueryItem", "Literal", "Field", "Func", "Expr", "SelectItem", 
    "Table", "Join", "Select", "FromItem", "TableRef", "Subquery", 
    "Cte", "Union"
]