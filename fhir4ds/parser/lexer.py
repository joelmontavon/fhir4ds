"""
This module contains the lexer for FHIRPath expressions, which tokenizes a
string into a sequence of tokens for the parser.
"""
import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Generator, Dict, List, Tuple

# Updated TokenType that combines both implementations
class TokenType(Enum):
    # Literals
    IDENTIFIER = auto()
    STRING_LITERAL = auto()
    INTEGER_LITERAL = auto()
    DECIMAL_LITERAL = auto()
    BOOLEAN_LITERAL = auto()
    DATETIME_LITERAL = auto()
    TIME_LITERAL = auto()
    QUANTITY_LITERAL = auto()

    # For backwards compatibility with SP-001-005 parser
    NUMBER_LITERAL = auto()
    TRUE = auto()
    FALSE = auto()

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

    # Operators (using SP-001-005 naming for compatibility)
    PLUS = auto()
    MINUS = auto()
    STAR = auto()  # For multiply compatibility
    SLASH = auto() # For divide compatibility
    MULTIPLY = auto()
    DIVIDE = auto()

    # Comparison operators (both naming schemes)
    EQ = auto()
    EQUAL = auto()
    NE = auto()
    NOT_EQUAL = auto()
    GT = auto()
    GREATER_THAN = auto()
    LT = auto()
    LESS_THAN = auto()
    GTE = auto()
    GREATER_EQUAL = auto()
    LTE = auto()
    LESS_EQUAL = auto()
    EQUIVALENT = auto()
    NOT_EQUIVALENT = auto()

    # Delimiters
    DOT = auto()
    LPAREN = auto()
    RPAREN = auto()
    LBRACKET = auto()
    RBRACKET = auto()
    LBRACE = auto()
    RBRACE = auto()
    COMMA = auto()
    PIPE = auto()
    AMPERSAND = auto()

    # End of File
    EOF = auto()

@dataclass
class SourceLocation:
    line: int
    column: int
    offset: int = 0

@dataclass
class Token:
    type: TokenType
    value: str
    line: int
    column: int

    @property
    def location(self) -> SourceLocation:
        return SourceLocation(self.line, self.column, 0)

class LexerError(Exception):
    def __init__(self, message: str, location: SourceLocation):
        self.message = message
        self.location = location
        super().__init__(f"Lexer error at line {location.line}, column {location.column}: {message}")

class Lexer:
    """
    Unified lexer that combines the sophistication of FHIRPathLexer with
    compatibility for the SP-001-005 parser.
    """

    _KEYWORDS: Dict[str, TokenType] = {
        'and': TokenType.AND,
        'or': TokenType.OR,
        'xor': TokenType.XOR,
        'implies': TokenType.IMPLIES,
        'not': TokenType.NOT,
        'is': TokenType.IS,
        'as': TokenType.AS,
        'in': TokenType.IN,
        'contains': TokenType.CONTAINS,
        'mod': TokenType.MOD,
        'true': TokenType.TRUE,  # For SP-001-005 compatibility
        'false': TokenType.FALSE, # For SP-001-005 compatibility
    }

    def __init__(self, text: str):
        self.text = text
        self.pos = 0
        self.line = 1
        self.column = 1

    def _advance(self):
        if self.pos < len(self.text):
            if self.text[self.pos] == '\n':
                self.line += 1
                self.column = 1
            else:
                self.column += 1
            self.pos += 1

    def _peek(self) -> str | None:
        if self.pos < len(self.text):
            return self.text[self.pos]
        return None

    def _skip_whitespace(self):
        while self._peek() is not None and self._peek().isspace():
            self._advance()

    def _tokenize_string_literal(self) -> Token:
        start_pos = self.pos
        self._advance()  # Consume opening quote
        while self._peek() is not None and self._peek() != "'":
            if self._peek() == '\\': # handle escaped quotes
                self._advance()
            self._advance()
        self._advance()  # Consume closing quote
        value = self.text[start_pos+1:self.pos-1]
        return Token(TokenType.STRING_LITERAL, value, self.line, self.column)

    def _tokenize_number(self) -> Token:
        start_pos = self.pos
        has_decimal = False
        while self._peek() is not None and (self._peek().isdigit() or self._peek() == '.'):
            if self._peek() == '.':
                has_decimal = True
            self._advance()
        value = self.text[start_pos:self.pos]

        # Return compatible token types for SP-001-005 parser
        if has_decimal:
            return Token(TokenType.NUMBER_LITERAL, value, self.line, self.column)
        else:
            return Token(TokenType.NUMBER_LITERAL, value, self.line, self.column)

    def _tokenize_identifier(self) -> Token:
        start_pos = self.pos
        while self._peek() is not None and (self._peek().isalnum() or self._peek() == '_'):
            self._advance()
        value = self.text[start_pos:self.pos]

        token_type = self._KEYWORDS.get(value, TokenType.IDENTIFIER)
        return Token(token_type, value, self.line, self.column)

    def tokenize(self) -> list[Token]:
        tokens = []
        while self._peek() is not None:
            self._skip_whitespace()
            char = self._peek()
            if char is None:
                break

            if char == "'":
                tokens.append(self._tokenize_string_literal())
            elif char.isdigit():
                tokens.append(self._tokenize_number())
            elif char.isalpha() or char == '_':
                tokens.append(self._tokenize_identifier())
            elif char == '+':
                tokens.append(Token(TokenType.PLUS, char, self.line, self.column))
                self._advance()
            elif char == '-':
                tokens.append(Token(TokenType.MINUS, char, self.line, self.column))
                self._advance()
            elif char == '*':
                tokens.append(Token(TokenType.STAR, char, self.line, self.column))
                self._advance()
            elif char == '/':
                tokens.append(Token(TokenType.SLASH, char, self.line, self.column))
                self._advance()
            elif char == '.':
                tokens.append(Token(TokenType.DOT, char, self.line, self.column))
                self._advance()
            elif char == '(':
                tokens.append(Token(TokenType.LPAREN, char, self.line, self.column))
                self._advance()
            elif char == ')':
                tokens.append(Token(TokenType.RPAREN, char, self.line, self.column))
                self._advance()
            elif char == '[':
                tokens.append(Token(TokenType.LBRACKET, char, self.line, self.column))
                self._advance()
            elif char == ']':
                tokens.append(Token(TokenType.RBRACKET, char, self.line, self.column))
                self._advance()
            elif char == ',':
                tokens.append(Token(TokenType.COMMA, char, self.line, self.column))
                self._advance()
            elif char == '=':
                tokens.append(Token(TokenType.EQ, char, self.line, self.column))
                self._advance()
            elif char == '!':
                self._advance()
                if self._peek() == '=':
                    tokens.append(Token(TokenType.NE, '!=', self.line, self.column))
                    self._advance()
                else:
                    raise ValueError("Unexpected character: !")
            elif char == '>':
                self._advance()
                if self._peek() == '=':
                    tokens.append(Token(TokenType.GTE, '>=', self.line, self.column))
                    self._advance()
                else:
                    tokens.append(Token(TokenType.GT, '>', self.line, self.column))
            elif char == '<':
                self._advance()
                if self._peek() == '=':
                    tokens.append(Token(TokenType.LTE, '<=', self.line, self.column))
                    self._advance()
                else:
                    tokens.append(Token(TokenType.LT, '<', self.line, self.column))
            else:
                raise ValueError(f"Unexpected character: {char}")

        tokens.append(Token(TokenType.EOF, "", self.line, self.column))
        return tokens
