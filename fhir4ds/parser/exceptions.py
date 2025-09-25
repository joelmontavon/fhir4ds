# fhir4ds.parser.exceptions

"""
Custom exceptions for the FHIRPath parser and lexer.
"""

class ParseError(Exception):
    """
    Exception raised for errors during parsing of a FHIRPath expression.

    Attributes:
        message (str): The error message.
        line (int): The line number where the error occurred.
        column (int): The column number where the error occurred.
    """
    def __init__(self, message, line=None, column=None):
        self.message = message
        self.line = line
        self.column = column
        super().__init__(self.message)

    def __str__(self):
        if self.line is not None and self.column is not None:
            return f"ParseError at line {self.line}, column {self.column}: {self.message}"
        return f"ParseError: {self.message}"


class LexerError(Exception):
    """
    Raised when the lexer encounters an error while tokenizing a FHIRPath expression.

    Attributes:
        message: The error message.
        location: The source location where the error occurred.
    """
    def __init__(self, message: str, location):
        self.message = message
        self.location = location
        super().__init__(f"Lexer error at line {location.line}, column {location.column}: {message}")
