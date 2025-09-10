"""
Modular function handlers for FHIRPath operations.

This module breaks down the massive FunctionCallOperation god class into
focused, specialized function handlers following clean architecture principles.
"""

from .base import FunctionHandler
from .collection import CollectionFunctionHandler
from .string import StringFunctionHandler
from .math import MathFunctionHandler
from .datetime import DateTimeFunctionHandler
from .type_conversion import TypeConversionFunctionHandler
from .comparison import ComparisonFunctionHandler

# NOTE: FunctionCallOperation import removed to avoid circular imports
# It can be imported directly from the parent module when needed

__all__ = [
    'FunctionHandler',
    'CollectionFunctionHandler',
    'StringFunctionHandler',
    'MathFunctionHandler',
    'DateTimeFunctionHandler',
    'TypeConversionFunctionHandler',
    'ComparisonFunctionHandler',
]