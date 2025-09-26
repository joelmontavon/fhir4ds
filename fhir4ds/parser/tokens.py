from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

@dataclass(frozen=True)
class SourceLocation:
    """Represents the location of a token in the source code."""
    line: int
    column: int
    offset: int

@dataclass(frozen=True)
class Token:
    """Represents a token produced by the lexer."""
    token_type: 'TokenType'
    value: Any
    source_location: SourceLocation

class TokenType(Enum):
    """Enumeration of all possible token types in FHIRPath."""
    # Literals
    STRING_LITERAL = auto()
    INTEGER_LITERAL = auto()
    DECIMAL_LITERAL = auto()
    BOOLEAN_LITERAL = auto()
    DATE_LITERAL = auto()
    DATETIME_LITERAL = auto()
    TIME_LITERAL = auto()
    QUANTITY_LITERAL = auto()

    # Identifier
    IDENTIFIER = auto()

    # Keywords
    AND = auto()
    OR = auto()
    XOR = auto()
    IMPLIES = auto()
    NOT = auto()
    IS = auto()
    AS = auto()
    IN = auto()
    CONTAINS = auto()
    MOD = auto()

    # Operators
    EQUAL = auto()
    NOT_EQUAL = auto()
    LESS_THAN = auto()
    LESS_EQUAL = auto()
    GREATER_THAN = auto()
    GREATER_EQUAL = auto()
    EQUIVALENT = auto()
    NOT_EQUIVALENT = auto()
    PLUS = auto()
    MINUS = auto()
    MULTIPLY = auto()
    DIVIDE = auto()
    AMPERSAND = auto()
    PIPE = auto()

    # Delimiters
    DOT = auto()
    LPAREN = auto()
    RPAREN = auto()
    LBRACKET = auto()
    RBRACKET = auto()
    LBRACE = auto()
    RBRACE = auto()
    COMMA = auto()

    # Special
    EOF = auto()
    MISMATCH = auto()
    WHITESPACE = auto()
    UNTERMINATED_STRING = auto()