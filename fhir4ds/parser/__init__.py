# fhir4ds.parser

"""
FHIRPath Parser
---------------

This package contains the complete FHIRPath parsing system, including
lexer for tokenization and parser for generating Abstract Syntax Trees (AST).

The parser is a key component of the FHIR4DS library, enabling the
interpretation and execution of FHIRPath expressions.

Modules:
- lexer: FHIRPath tokenization and lexical analysis.
- tokens: Token type definitions and data structures.
- fhirpath_parser: The main parser implementation.
- exceptions: Custom exceptions for parsing and lexing errors.
- precedence: Operator precedence and associativity rules.
"""

from .lexer import FHIRPathLexer
from .tokens import Token, TokenType, SourceLocation
from .fhirpath_parser import FHIRPathParser
from .exceptions import ParseError, LexerError

__all__ = [
    "FHIRPathLexer",
    "Token",
    "TokenType",
    "SourceLocation",
    "FHIRPathParser",
    "ParseError",
    "LexerError",
]
