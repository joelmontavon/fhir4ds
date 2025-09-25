"""
This module defines the data structures used by the FHIRPath lexer and parser,
including token types, source location information, and the token itself.
"""
from dataclasses import dataclass
from enum import Enum, auto


@dataclass(frozen=True)
class SourceLocation:
    """
    Represents the location of a token in the source FHIRPath expression.

    Attributes:
        line: The line number (1-based).
        column: The column number (1-based).
        offset: The character offset from the beginning of the expression (0-based).
    """
    line: int
    column: int
    offset: int


class TokenType(Enum):
    """
    An enumeration of all possible token types in the FHIRPath language.
    """
    # Literals
    STRING_LITERAL = auto()
    INTEGER_LITERAL = auto()
    DECIMAL_LITERAL = auto()
    BOOLEAN_LITERAL = auto()
    DATETIME_LITERAL = auto()
    TIME_LITERAL = auto()
    QUANTITY_LITERAL = auto()

    # Identifiers
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

    # Operators
    EQUAL = auto()           # =
    NOT_EQUAL = auto()       # !=
    LESS_THAN = auto()       # <
    LESS_EQUAL = auto()      # <=
    GREATER_THAN = auto()    # >
    GREATER_EQUAL = auto()   # >=
    EQUIVALENT = auto()      # ~
    NOT_EQUIVALENT = auto()  # !~
    PLUS = auto()            # +
    MINUS = auto()           # -
    MULTIPLY = auto()        # *
    DIVIDE = auto()          # /
    MOD = auto()             # mod

    # Delimiters and Symbols
    DOT = auto()             # .
    LPAREN = auto()          # (
    RPAREN = auto()          # )
    LBRACKET = auto()        # [
    RBRACKET = auto()        # ]
    LBRACE = auto()          # {
    RBRACE = auto()          # }
    COMMA = auto()           # ,
    PIPE = auto()            # |
    AMPERSAND = auto()       # &

    # End of File
    EOF = auto()


from typing import Any

@dataclass(frozen=True)
class Token:
    """
    Represents a token produced by the lexer.

    Attributes:
        token_type: The type of the token.
        value: The value of the token, which can be a string or a structured type.
        location: The source location of the token.
    """
    token_type: TokenType
    value: Any
    location: SourceLocation