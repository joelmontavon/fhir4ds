"""
This module defines the custom exceptions used by the FHIRPath parser and lexer.
"""
from dataclasses import dataclass
from fhir4ds.parser.tokens import SourceLocation
from fhir4ds.ast.nodes import FHIRPathNode


class LexerError(Exception):
    """
    Raised when the lexer encounters an error while tokenizing a FHIRPath expression.

    Attributes:
        message: The error message.
        location: The source location where the error occurred.
    """
    def __init__(self, message: str, location: SourceLocation):
        self.message = message
        self.location = location
        super().__init__(f"Lexer error at line {location.line}, column {location.column}: {message}")


class ParseError(Exception):
    """
    Parser error with location and context.
    """
    def __init__(self, message: str, location: SourceLocation, context: str):
        self.message = message
        self.location = location
        self.context = context
        super().__init__(f"Parse error at line {location.line}, column {location.column}: {message}")


@dataclass
class ValidationError:
    """
    Semantic validation error.
    """
    message: str
    node: FHIRPathNode
    error_type: str