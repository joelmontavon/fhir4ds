"""
Test utilities for FHIR4DS development and validation.

This package contains utilities for testing, validation, and comparison
during development of the FHIR4DS library.
"""

from .cte_comparison import (
    CTESystemComparison,
    ComparisonResult,
    compare_expression,
    batch_test_expressions,
    SAMPLE_EXPRESSIONS
)

__all__ = [
    'CTESystemComparison',
    'ComparisonResult', 
    'compare_expression',
    'batch_test_expressions',
    'SAMPLE_EXPRESSIONS'
]