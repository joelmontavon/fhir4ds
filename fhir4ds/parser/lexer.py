"""
This module contains the lexer for FHIRPath expressions, which tokenizes a
string into a sequence of tokens for the parser.
"""
import re
import codecs
from typing import Generator, Dict, List, Tuple

from fhir4ds.parser.tokens import Token, TokenType, SourceLocation
from fhir4ds.parser.exceptions import LexerError


class FHIRPathLexer:
    """
    A regex-based lexer for FHIRPath R4 expressions.

    The lexer tokenizes a FHIRPath expression string into a generator of Tokens.
    It handles all specified keywords, operators, literals, and symbols, and
    tracks source location for accurate error reporting.
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
        'true': TokenType.BOOLEAN_LITERAL,
        'false': TokenType.BOOLEAN_LITERAL,
    }

    _TOKEN_SPECIFICATION: List[Tuple[str, str]] = [
        # Datetime literals must be matched before identifiers.
        # A more specific regex for datetime starting with a date part.
        ('DATETIME_LITERAL', r'@\d{4}(?:-\d{2}(?:-\d{2})?)?(?:T(?:[01]\d|2[0-3])(?::[0-5]\d(?::[0-5]\d(?:\.\d+)?)?)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)?)?'),
        # A more specific regex for time literals starting with @T.
        ('TIME_LITERAL', r'@T(?:[01]\d|2[0-3])(?::[0-5]\d(?::[0-5]\d(?:\.\d+)?)?)?'),
        # Quantity literals must be matched before numbers.
        ('QUANTITY_LITERAL', r"\d+(?:\.\d+)?\s*'(?:[^'\\]|\\.)*'"),
        ('STRING_LITERAL', r"'([^'\\]|\\.)*'"),
        ('DECIMAL_LITERAL', r'\d+\.\d+'),
        ('INTEGER_LITERAL', r'\d+'),
        ('IDENTIFIER', r'[A-Za-z_][A-Za-z0-9_]*'),

        # Operators (multi-character first to ensure correct matching)
        ('NOT_EQUAL', r'!='),
        ('LESS_EQUAL', r'<='),
        ('GREATER_EQUAL', r'>='),
        ('EQUIVALENT', r'~'),
        ('NOT_EQUIVALENT', r'!~'),

        # Delimiters and single-character operators
        ('EQUAL', r'='),
        ('LESS_THAN', r'<'),
        ('GREATER_THAN', r'>'),
        ('PLUS', r'\+'),
        ('MINUS', r'-'),
        ('MULTIPLY', r'\*'),
        ('DIVIDE', r'/'),
        ('DOT', r'\.'),
        ('LPAREN', r'\('),
        ('RPAREN', r'\)'),
        ('LBRACKET', r'\['),
        ('RBRACKET', r'\]'),
        ('LBRACE', r'\{'),
        ('RBRACE', r'\}'),
        ('COMMA', r','),
        ('PIPE', r'\|'),
        ('AMPERSAND', r'&'),

        # Whitespace (to be skipped by the tokenizer)
        ('WHITESPACE', r'\s+'),
        # This must come after the valid STRING_LITERAL pattern to catch unterminated strings.
        ('UNTERMINATED_STRING', r"'([^'\\]|\\.)*"),
        # Mismatch (to catch any characters that don't match other patterns)
        ('MISMATCH', r'.'),
    ]

    def __init__(self, expression: str):
        """
        Initializes the lexer with the expression to tokenize.
        """
        self.expression = expression
        self.token_regex = '|'.join(
            f'(?P<{name}>{pattern})' for name, pattern in self._TOKEN_SPECIFICATION
        )

    def tokenize(self) -> Generator[Token, None, None]:
        """
        Tokenizes the FHIRPath expression.

        This method scans the expression string and yields a sequence of tokens.
        It uses a single large regex to efficiently match all possible token types.

        Yields:
            A generator of Token objects.

        Raises:
            LexerError: If an unexpected character or an unterminated string is encountered.
        """
        line_num = 1
        line_start = 0

        for mo in re.finditer(self.token_regex, self.expression):
            kind = mo.lastgroup
            value = mo.group()
            column = mo.start() - line_start + 1
            location = SourceLocation(line=line_num, column=column, offset=mo.start())

            if kind == 'IDENTIFIER':
                token_type = self._KEYWORDS.get(value, TokenType.IDENTIFIER)
                yield Token(token_type, value, location)
            elif kind == 'STRING_LITERAL':
                yield self._unescape_string(value, location)
            elif kind == 'QUANTITY_LITERAL':
                # Split the quantity into value and unit.
                parts = value.split("'")
                numeric_part = parts[0].strip()
                unit_part = parts[1]
                quantity_value = {'value': numeric_part, 'unit': unit_part}
                yield Token(TokenType.QUANTITY_LITERAL, quantity_value, location)
            elif kind in ('INTEGER_LITERAL', 'DECIMAL_LITERAL', 'DATETIME_LITERAL', 'TIME_LITERAL'):
                yield Token(TokenType[kind], value, location)
            elif kind == 'WHITESPACE':
                newlines = value.count('\n')
                if newlines > 0:
                    line_num += newlines
                    line_start = mo.start() + value.rfind('\n') + 1
                continue  # Don't yield a token for whitespace
            elif kind == 'UNTERMINATED_STRING':
                raise LexerError("Unterminated string literal", location)
            elif kind == 'MISMATCH':
                raise LexerError(f"Unexpected character: '{value}'", location)
            else:
                # All other tokens are operators and delimiters
                token_type = TokenType[kind]
                yield Token(token_type, value, location)

        # Yield the End-Of-File token
        eof_offset = len(self.expression)
        eof_column = eof_offset - line_start + 1
        eof_location = SourceLocation(line=line_num, column=eof_column, offset=eof_offset)
        yield Token(TokenType.EOF, '', eof_location)

    def _unescape_string(self, value: str, location: SourceLocation) -> Token:
        """
        Processes a string literal, handling escape sequences.

        Args:
            value: The raw string literal from the expression (including quotes).
            location: The source location of the string literal.

        Returns:
            A STRING_LITERAL token with the unescaped string value.

        Raises:
            LexerError: If an invalid escape sequence is found.
        """
        content = value[1:-1]
        unescaped_chars = []
        i = 0
        while i < len(content):
            char = content[i]
            if char == '\\':
                i += 1
                if i >= len(content):
                    # This case should be caught by the UNTERMINATED_STRING regex,
                    # but as a safeguard:
                    raise LexerError("Unterminated string literal", location)

                escape_char = content[i]
                if escape_char == "'":
                    unescaped_chars.append("'")
                elif escape_char == '\\':
                    unescaped_chars.append('\\')
                elif escape_char == '/':
                    unescaped_chars.append('/')
                elif escape_char == 'f':
                    unescaped_chars.append('\f')
                elif escape_char == 'n':
                    unescaped_chars.append('\n')
                elif escape_char == 'r':
                    unescaped_chars.append('\r')
                elif escape_char == 't':
                    unescaped_chars.append('\t')
                elif escape_char == 'u':
                    # Unicode escape sequence \uXXXX
                    if i + 4 >= len(content):
                        raise LexerError("Invalid unicode escape sequence in string", location)
                    hex_code = content[i+1:i+5]
                    try:
                        unescaped_chars.append(chr(int(hex_code, 16)))
                        i += 4
                    except ValueError:
                        error_loc = SourceLocation(
                            line=location.line,
                            column=location.column + i + 1,
                            offset=location.offset + i + 1,
                        )
                        raise LexerError(f"Invalid unicode escape sequence: '\\u{hex_code}'", error_loc)
                else:
                    error_loc = SourceLocation(
                        line=location.line,
                        column=location.column + i + 1,
                        offset=location.offset + i + 1,
                    )
                    raise LexerError(f"Invalid escape sequence: '\\{escape_char}'", error_loc)
            else:
                unescaped_chars.append(char)
            i += 1

        return Token(TokenType.STRING_LITERAL, "".join(unescaped_chars), location)