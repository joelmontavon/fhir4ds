from typing import Optional

from ..ast.nodes import SourceLocation

class FHIRPathError(Exception):
    """Base class for all errors raised by the FHIR4DS library."""
    def __init__(self, message: str, location: Optional[SourceLocation] = None):
        self.message = message
        self.location = location
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        if self.location:
            return f"Error at L{self.location.line}:C{self.location.column}: {self.message}"
        return self.message

class LexerError(FHIRPathError):
    """Raised for errors during the lexing phase."""
    pass

class ParserError(FHIRPathError):
    """Raised for errors during the parsing phase."""
    pass