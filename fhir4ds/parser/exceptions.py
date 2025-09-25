"""
This module defines the custom exceptions used by the FHIRPath parser and lexer.
"""
from fhir4ds.parser.tokens import SourceLocation


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