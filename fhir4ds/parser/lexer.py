import re
from dataclasses import dataclass
from enum import Enum, auto

class TokenType(Enum):
    # Literals
    IDENTIFIER = auto()
    STRING_LITERAL = auto()
    NUMBER_LITERAL = auto()
    DATE_LITERAL = auto()
    TIME_LITERAL = auto()
    DATETIME_LITERAL = auto()
    QUANTITY_LITERAL = auto()

    # Keywords
    AND = auto()
    OR = auto()
    XOR = auto()
    IMPLIES = auto()
    IS = auto()
    AS = auto()
    TRUE = auto()
    FALSE = auto()
    MOD = auto()

    # Operators
    PLUS = auto()
    MINUS = auto()
    STAR = auto()
    SLASH = auto()
    AMPERSAND = auto()
    PIPE = auto()
    TILDE = auto()

    # Comparison
    EQ = auto()
    NE = auto()
    GT = auto()
    LT = auto()
    GTE = auto()
    LTE = auto()

    # Delimiters
    DOT = auto()
    LPAREN = auto()
    RPAREN = auto()
    LBRACKET = auto()
    RBRACKET = auto()
    COMMA = auto()

    # End of File
    EOF = auto()

@dataclass
class Token:
    type: TokenType
    value: str
    line: int
    column: int

class Lexer:
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
        while self._peek() is not None and (self._peek().isdigit() or self._peek() == '.'):
            self._advance()
        value = self.text[start_pos:self.pos]
        return Token(TokenType.NUMBER_LITERAL, value, self.line, self.column)

    def _tokenize_identifier(self) -> Token:
        start_pos = self.pos
        while self._peek() is not None and (self._peek().isalnum() or self._peek() == '_'):
            self._advance()
        value = self.text[start_pos:self.pos]

        # Check for keywords
        keyword_map = {
            "and": TokenType.AND,
            "or": TokenType.OR,
            "xor": TokenType.XOR,
            "implies": TokenType.IMPLIES,
            "is": TokenType.IS,
            "as": TokenType.AS,
            "true": TokenType.TRUE,
            "false": TokenType.FALSE,
            "mod": TokenType.MOD,
        }
        token_type = keyword_map.get(value, TokenType.IDENTIFIER)
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
                    # Or handle as a "not" operator if it's supported
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