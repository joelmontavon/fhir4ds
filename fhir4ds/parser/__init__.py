# FHIR4DS Parser Package

"""
FHIRPath Parser
---------------

This package contains the complete FHIRPath parsing system, including
unified lexer for tokenization and advanced parser for generating
Abstract Syntax Trees (AST) with complete grammar support.

The parser is a key component of the FHIR4DS library, enabling the
interpretation and execution of FHIRPath expressions.

Components:
- lexer: Unified lexer with comprehensive token support
- parser: Advanced parser with complete FHIRPath grammar
- core: High-level parsing interface for test integration
"""

from .lexer import Lexer, Token, TokenType, SourceLocation, LexerError
from .parser import Parser
from .core import parse

__all__ = [
    "Lexer",
    "Parser",
    "Token",
    "TokenType",
    "SourceLocation",
    "LexerError",
    "parse",
]