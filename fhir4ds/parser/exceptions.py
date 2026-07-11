from typing import Optional

# Attempt to import SourceLocation, but handle the case where it might not be available
# to avoid circular dependencies or issues in different contexts.
try:
    from fhir4ds.parser.tokens import SourceLocation
except ImportError:
    # Define a placeholder class if SourceLocation cannot be imported.
    # This allows the exception classes to be defined without breaking.
    class SourceLocation:
        def __init__(self, line: int, column: int):
            self.line = line
            self.column = column

class FHIRPathError(Exception):
    """Base exception for all FHIRPath parsing errors."""

    def __init__(self, message: str, location: Optional[SourceLocation] = None,
                 context: Optional[str] = None, suggestion: Optional[str] = None):
        """
        Initializes a FHIRPathError.

        Args:
            message: The primary error message.
            location: The location in the source where the error occurred.
            context: Additional context about the state of the parser.
            suggestion: A suggestion for how to fix the error.
        """
        self.message = message
        self.location = location
        self.context = context
        self.suggestion = suggestion
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Formats the complete error message with all available details."""
        msg = self.message
        if self.location:
            msg = f"Line {self.location.line}, Column {self.location.column}: {msg}"
        if self.context:
            msg += f"\nContext: {self.context}"
        if self.suggestion:
            msg += f"\nSuggestion: {self.suggestion}"
        return msg

class FHIRPathSyntaxError(FHIRPathError):
    """
    Raised for syntax errors in FHIRPath expressions.
    This typically occurs when the input does not conform to the expected grammar at a specific point.
    """
    pass

class FHIRPathParseError(FHIRPathError):
    """
    Raised for general parsing errors that are not strictly syntax violations.
    This could include issues where the grammar is met, but the structure is still invalid.
    """
    pass

class FHIRPathSemanticError(FHIRPathError):
    """
    Raised for semantic errors, such as type mismatches, invalid operations,
    or references to undefined variables.
    """
    pass

class FHIRPathFunctionError(FHIRPathError):
    """
    Raised for errors related to function calls, such as an unknown function name,
    incorrect number of arguments, or invalid argument types.
    """
    pass

class LexerError(FHIRPathError):
    """
    Raised for errors that occur during the lexing (tokenization) phase.
    Inherits from FHIRPathError to provide consistent error reporting.
    """
    pass