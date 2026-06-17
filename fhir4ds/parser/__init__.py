# fhir4ds.parser

"""
FHIRPath Parser
---------------

This package contains the FHIRPath parser, which is responsible for
converting a stream of tokens from the lexer into an Abstract Syntax Tree (AST).

The parser is a key component of the FHIR4DS library, enabling the
interpretation and execution of FHIRPath expressions.

Modules:
- fhirpath_parser: The main parser implementation.
- exceptions: Custom exceptions for parsing errors.
- precedence: Operator precedence and associativity rules.
"""

from .fhirpath_parser import FHIRPathParser
from .exceptions import ParseError

__all__ = [
    "FHIRPathParser",
    "ParseError",
]
