"""
FHIRPath Lexer and Parser

This module contains the lexical analyzer and parser for FHIRPath expressions.
It converts FHIRPath strings into Abstract Syntax Trees (ASTs) for further processing.
"""

import re
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional
from contextlib import contextmanager

from .ast_nodes import (
    ASTNode, ThisNode, VariableNode, LiteralNode, IdentifierNode, FunctionCallNode,
    BinaryOpNode, UnaryOpNode, PathNode, IndexerNode, TupleNode, IntervalConstructorNode, ListLiteralNode,
    CQLQueryExpressionNode, ResourceQueryNode
)


class TokenType(Enum):
    """Token types for FHIRPath lexer"""
    IDENTIFIER = "IDENTIFIER"
    STRING = "STRING"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    QUANTITY = "QUANTITY"
    DATETIME = "DATETIME"
    BOOLEAN = "BOOLEAN"
    DOT = "DOT"
    LBRACKET = "LBRACKET"
    RBRACKET = "RBRACKET"
    LPAREN = "LPAREN"
    RPAREN = "RPAREN"
    LBRACE = "LBRACE"
    RBRACE = "RBRACE"
    COLON = "COLON"
    COMMA = "COMMA"
    PIPE = "PIPE"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    AS = "AS"
    EQUALS = "EQUALS"
    NOT_EQUALS = "NOT_EQUALS"
    EQUIVALENT = "EQUIVALENT"
    NOT_EQUIVALENT = "NOT_EQUIVALENT"
    GREATER = "GREATER"
    LESS_EQUAL = "LESS_EQUAL"
    LESS = "LESS"
    GREATER_EQUAL = "GREATER_EQUAL"
    PLUS = "PLUS"
    DOLLAR_THIS = "DOLLAR_THIS"
    DOLLAR_INDEX = "DOLLAR_INDEX"
    DOLLAR_TOTAL = "DOLLAR_TOTAL"
    MINUS = "MINUS"
    MULTIPLY = "MULTIPLY"
    DIVIDE = "DIVIDE"
    POWER = "POWER"
    # CQL Query Keywords
    WHERE = "WHERE"
    SORT = "SORT"
    BY = "BY"
    RETURN = "RETURN"
    WITH = "WITH"
    SUCH = "SUCH"
    THAT = "THAT"
    EOF = "EOF"


@dataclass
class Token:
    """Token representation"""
    type: TokenType
    value: str
    position: int


class FHIRPathLexer:
    """Lexer for FHIRPath expressions"""
    
    def __init__(self, expression: str):
        self.expression = expression
        self.position = 0
        self.current_char = self.expression[0] if expression else None
    
    def advance(self):
        """Move to the next character"""
        self.position += 1
        if self.position >= len(self.expression):
            self.current_char = None
        else:
            self.current_char = self.expression[self.position]
    
    def skip_whitespace(self):
        """Skip whitespace characters"""
        while self.current_char and self.current_char.isspace():
            self.advance()
    
    def read_string(self):
        """Read a string literal with proper escape sequence handling"""
        quote_char = self.current_char
        self.advance()  # Skip opening quote
        result = ""
        while self.current_char and self.current_char != quote_char:
            if self.current_char == '\\':
                self.advance()
                if self.current_char:
                    # Handle common escape sequences
                    if self.current_char == 't':
                        result += '\t'
                    elif self.current_char == 'n':
                        result += '\n'
                    elif self.current_char == 'r':
                        result += '\r'
                    elif self.current_char == '\\':
                        result += '\\'
                    elif self.current_char == '\'':
                        result += '\''
                    elif self.current_char == '"':
                        result += '"'
                    else:
                        # For unrecognized escape sequences, keep the backslash
                        result += '\\' + self.current_char
                    self.advance()
            else:
                result += self.current_char
                self.advance()
        if self.current_char == quote_char:
            self.advance()  # Skip closing quote
        return result
    
    def read_number(self):
        """Read a number (integer or decimal)"""
        result = ""
        while self.current_char and (self.current_char.isdigit() or self.current_char == '.'):
            result += self.current_char
            self.advance()
        return result
    
    def read_quantity(self, number_part):
        """Read a quantity literal with unit (e.g., 1.5'kg')"""
        if self.current_char != "'":
            return number_part, None
            
        self.advance()  # Skip opening quote
        unit = ""
        while self.current_char and self.current_char != "'":
            unit += self.current_char
            self.advance()
            
        if self.current_char == "'":
            self.advance()  # Skip closing quote
            return number_part, unit
        else:
            raise ValueError(f"Unterminated unit in quantity literal at position {self.position}")
    
    def read_long_integer(self, number_part):
        """Check if number is followed by 'L' suffix for long integers"""
        if self.current_char and self.current_char.upper() == 'L':
            self.advance()  # Skip the L
            return number_part + 'L'
        return number_part
    
    def read_datetime(self):
        """Read a DateTime literal starting with @"""
        result = ""
        
        # Skip the '@' character
        self.advance()
        
        # Read the datetime string until whitespace, operator, or end
        # Note: hyphens (-) are allowed in DateTime literals for dates like @2013-01-02
        while (self.current_char and 
               self.current_char not in [' ', '\t', '\n', '\r', ')', ']', '}', ',', 
                                       '|', '&', '=', '!', '<', '>', '+', '*', '/']):
            result += self.current_char
            self.advance()
        
        return result
    
    def read_identifier(self):
        """Read an identifier"""
        result = ""
        while self.current_char and (self.current_char.isalnum() or self.current_char in '_'):
            result += self.current_char
            self.advance()
        return result
    
    def tokenize(self) -> List[Token]:
        """Tokenize the FHIRPath expression"""
        tokens = []
        
        while self.current_char:
            self.skip_whitespace()
            # Check for context variables starting with $
            if self.current_char == '$':
                # Check for $this keyword
                if self.expression[self.position:].startswith('$this'):
                    # Ensure it's not part of a larger identifier like $thisValue
                    if self.position + 5 == len(self.expression) or not self.expression[self.position + 5].isalnum():
                        tokens.append(Token(TokenType.DOLLAR_THIS, '$this', self.position))
                        for _ in range(5): self.advance()
                        continue
                # Check for $index keyword
                elif self.expression[self.position:].startswith('$index'):
                    # Ensure it's not part of a larger identifier like $indexValue
                    if self.position + 6 == len(self.expression) or not self.expression[self.position + 6].isalnum():
                        tokens.append(Token(TokenType.DOLLAR_INDEX, '$index', self.position))
                        for _ in range(6): self.advance()
                        continue
                # Check for $total keyword
                elif self.expression[self.position:].startswith('$total'):
                    # Ensure it's not part of a larger identifier like $totalValue
                    if self.position + 6 == len(self.expression) or not self.expression[self.position + 6].isalnum():
                        tokens.append(Token(TokenType.DOLLAR_TOTAL, '$total', self.position))
                        for _ in range(6): self.advance()
                        continue
                # If we get here, it's an unrecognized $ variable
                else:
                    raise ValueError(
                        f"Unrecognized context variable at position {self.position} "
                        f"in FHIRPath expression '{self.expression}'. "
                        f"Supported context variables are: $this, $index, $total"
                    )
            if not self.current_char:
                break
            
            pos = self.position
            
            if self.current_char in ['"', "'"]:
                value = self.read_string()
                tokens.append(Token(TokenType.STRING, value, pos))
            elif self.current_char.isdigit():
                number_part = self.read_number()
                
                # Check for quantity literal (e.g., 1.5'kg')
                if self.current_char == "'":
                    number, unit = self.read_quantity(number_part)
                    quantity_value = {'value': number, 'unit': unit}
                    tokens.append(Token(TokenType.QUANTITY, quantity_value, pos))
                # Check for long integer suffix (e.g., 1L)
                elif self.current_char and self.current_char.upper() == 'L':
                    long_value = self.read_long_integer(number_part)
                    tokens.append(Token(TokenType.INTEGER, long_value, pos))
                else:
                    # Regular number
                    if '.' in number_part:
                        tokens.append(Token(TokenType.DECIMAL, number_part, pos))
                    else:
                        tokens.append(Token(TokenType.INTEGER, number_part, pos))
            elif self.current_char == '@':
                # DateTime literal (e.g., @2013-01-02T00:00:00.000Z)
                datetime_value = self.read_datetime()
                tokens.append(Token(TokenType.DATETIME, datetime_value, pos))
            elif self.current_char.isalpha() or self.current_char == '_':
                value = self.read_identifier()
                # Check for keywords - but only treat "not" as a keyword if it's not followed by "("
                # This allows .not() to be parsed as a function call
                if value.lower() == 'and':
                    tokens.append(Token(TokenType.AND, value, pos))
                elif value.lower() == 'or':
                    tokens.append(Token(TokenType.OR, value, pos))
                elif value.lower() == 'as':
                    tokens.append(Token(TokenType.AS, value, pos))
                elif value.lower() == 'where':
                    tokens.append(Token(TokenType.WHERE, value, pos))
                elif value.lower() == 'sort':
                    tokens.append(Token(TokenType.SORT, value, pos))
                elif value.lower() == 'by':
                    tokens.append(Token(TokenType.BY, value, pos))
                elif value.lower() == 'return':
                    tokens.append(Token(TokenType.RETURN, value, pos))
                elif value.lower() == 'with':
                    tokens.append(Token(TokenType.WITH, value, pos))
                elif value.lower() == 'such':
                    tokens.append(Token(TokenType.SUCH, value, pos))
                elif value.lower() == 'that':
                    tokens.append(Token(TokenType.THAT, value, pos))
                elif value.lower() == 'not':
                    # Check if this is followed by a '(' (function call) by looking ahead
                    temp_pos = self.position
                    self.skip_whitespace()
                    if self.current_char == '(':
                        # This is .not() function call, treat as identifier
                        tokens.append(Token(TokenType.IDENTIFIER, value, pos))
                    else:
                        # This is the "not" keyword for logical negation
                        tokens.append(Token(TokenType.NOT, value, pos))
                    # Reset position since we only peeked ahead
                    while self.position > temp_pos:
                        self.position -= 1
                        if self.position >= 0:
                            self.current_char = self.expression[self.position]
                        else:
                            self.current_char = None
                elif value.lower() in ['true', 'false']:
                    tokens.append(Token(TokenType.BOOLEAN, value, pos))
                else:
                    tokens.append(Token(TokenType.IDENTIFIER, value, pos))
            elif self.current_char == '.':
                tokens.append(Token(TokenType.DOT, '.', pos))
                self.advance()
            elif self.current_char == '[':
                tokens.append(Token(TokenType.LBRACKET, '[', pos))
                self.advance()
            elif self.current_char == ']':
                tokens.append(Token(TokenType.RBRACKET, ']', pos))
                self.advance()
            elif self.current_char == '(':
                tokens.append(Token(TokenType.LPAREN, '(', pos))
                self.advance()
            elif self.current_char == ')':
                tokens.append(Token(TokenType.RPAREN, ')', pos))
                self.advance()
            elif self.current_char == '{':
                tokens.append(Token(TokenType.LBRACE, '{', pos))
                self.advance()
            elif self.current_char == '}':
                tokens.append(Token(TokenType.RBRACE, '}', pos))
                self.advance()
            elif self.current_char == ':':
                tokens.append(Token(TokenType.COLON, ':', pos))
                self.advance()
            elif self.current_char == ',':
                tokens.append(Token(TokenType.COMMA, ',', pos))
                self.advance()
            elif self.current_char == '|':
                tokens.append(Token(TokenType.PIPE, '|', pos))
                self.advance()
            elif self.current_char == '!' and self.peek() == '~':
                tokens.append(Token(TokenType.NOT_EQUIVALENT, '!~', pos))
                self.advance()
                self.advance()
            elif self.current_char == '!' and self.peek() == '=':
                tokens.append(Token(TokenType.NOT_EQUALS, '!=', pos))
                self.advance()
                self.advance()
            elif self.current_char == '~':
                tokens.append(Token(TokenType.EQUIVALENT, '~', pos))
                self.advance()
            elif self.current_char == '=':
                tokens.append(Token(TokenType.EQUALS, '=', pos))
                self.advance()
            elif self.current_char == '>' and self.peek() == '=':
                tokens.append(Token(TokenType.GREATER_EQUAL, '>=', pos))
                self.advance()
                self.advance()
            elif self.current_char == '>':
                tokens.append(Token(TokenType.GREATER, '>', pos))
                self.advance()
            elif self.current_char == '<' and self.peek() == '=':
                tokens.append(Token(TokenType.LESS_EQUAL, '<=', pos))
                self.advance()
                self.advance()
            elif self.current_char == '<':
                tokens.append(Token(TokenType.LESS, '<', pos))
                self.advance()
            elif self.current_char == '+':
                tokens.append(Token(TokenType.PLUS, '+', pos))
                self.advance()
            elif self.current_char == '-':
                tokens.append(Token(TokenType.MINUS, '-', pos))
                self.advance()
            elif self.current_char == '*':
                tokens.append(Token(TokenType.MULTIPLY, '*', pos))
                self.advance()
            elif self.current_char == '/':
                tokens.append(Token(TokenType.DIVIDE, '/', pos))
                self.advance()
            elif self.current_char == '^':
                tokens.append(Token(TokenType.POWER, '^', pos))
                self.advance()
            else:
                # Invalid character - reject with helpful error
                raise ValueError(
                    f"Invalid character '{self.current_char}' at position {self.position} "
                    f"in FHIRPath expression '{self.expression}'. "
                    f"Valid operators are: = != < <= > >= + - * / and or not"
                )
        
        tokens.append(Token(TokenType.EOF, '', self.position))
        return tokens
    
    def peek(self) -> Optional[str]:
        """Peek at the next character"""
        peek_pos = self.position + 1
        if peek_pos >= len(self.expression):
            return None
        return self.expression[peek_pos]


class FHIRPathParser:
    """Parser for FHIRPath expressions"""
    
    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.position = 0
        self.current_token = tokens[0] if tokens else None
        self.original_expression = ""  # Store for error reporting
        
    @property
    def current_position(self) -> int:
        """Get current character position in original expression."""
        if self.current_token:
            return self.current_token.position
        elif self.tokens:
            # Return position after last token
            return self.tokens[-1].position + len(self.tokens[-1].value)
        else:
            return 0
    
    def advance(self):
        """Move to the next token"""
        self.position += 1
        if self.position >= len(self.tokens):
            self.current_token = Token(TokenType.EOF, '', -1)
        else:
            self.current_token = self.tokens[self.position]
    
    @contextmanager
    def preserve_parser_state(self):
        """Context manager to safely preserve and restore parser state during lookahead operations."""
        saved_pos = self.position
        saved_token = self.current_token
        try:
            yield
        finally:
            self.position = saved_pos
            self.current_token = saved_token
    
    def parse(self) -> ASTNode:
        """Parse the tokens into an AST"""
        return self.parse_union_expression()
    
    def parse_union_expression(self) -> ASTNode:
        """Parse union expressions (collection union operator |)"""
        node = self.parse_or_expression()
        
        while self.current_token.type == TokenType.PIPE:
            op = self.current_token.value
            self.advance()
            right = self.parse_or_expression()
            node = BinaryOpNode(node, op, right)
        
        return node
    
    def parse_or_expression(self) -> ASTNode:
        """Parse OR expressions"""
        node = self.parse_and_expression()
        
        while self.current_token.type == TokenType.OR:
            op = self.current_token.value
            self.advance()
            right = self.parse_and_expression()
            node = BinaryOpNode(node, op, right)
        
        return node
    
    def parse_and_expression(self) -> ASTNode:
        """Parse AND expressions"""
        node = self.parse_equality_expression()
        
        while self.current_token.type == TokenType.AND:
            op = self.current_token.value
            self.advance()
            right = self.parse_equality_expression()
            node = BinaryOpNode(node, op, right)
        
        return node
    
    def parse_equality_expression(self) -> ASTNode:
        """Parse equality expressions"""
        node = self.parse_as_expression()
        
        while self.current_token.type in [TokenType.EQUALS, TokenType.NOT_EQUALS, TokenType.EQUIVALENT, TokenType.NOT_EQUIVALENT]:
            op = self.current_token.value
            self.advance()
            right = self.parse_as_expression()
            node = BinaryOpNode(node, op, right)
        
        return node
    
    def parse_as_expression(self) -> ASTNode:
        """Parse AS type casting expressions"""
        node = self.parse_relational_expression()
        
        while self.current_token.type == TokenType.AS:
            op = self.current_token.value
            self.advance()
            right = self.parse_relational_expression()
            node = BinaryOpNode(node, op, right)
        
        return node
    
    def parse_relational_expression(self) -> ASTNode:
        """Parse relational expressions"""
        node = self.parse_additive_expression()
        
        while self.current_token.type in [TokenType.GREATER, TokenType.LESS, 
                                         TokenType.GREATER_EQUAL, TokenType.LESS_EQUAL]:
            op = self.current_token.value
            self.advance()
            right = self.parse_additive_expression()
            node = BinaryOpNode(node, op, right)
        
        return node
    
    def parse_additive_expression(self) -> ASTNode:
        """Parse additive expressions"""
        node = self.parse_multiplicative_expression()
        
        while self.current_token.type in [TokenType.PLUS, TokenType.MINUS]:
            op = self.current_token.value
            self.advance()
            right = self.parse_multiplicative_expression()
            node = BinaryOpNode(node, op, right)
        
        return node
    
    def parse_multiplicative_expression(self) -> ASTNode:
        """Parse multiplicative expressions"""
        node = self.parse_power_expression()
        
        while self.current_token.type in [TokenType.MULTIPLY, TokenType.DIVIDE]:
            op = self.current_token.value
            self.advance()
            right = self.parse_power_expression()
            node = BinaryOpNode(node, op, right)
        
        return node
    
    def parse_power_expression(self) -> ASTNode:
        """Parse power expressions (exponentiation)"""
        node = self.parse_unary_expression()
        
        # Power is right-associative (e.g., 2^3^2 = 2^(3^2) = 2^9 = 512)
        if self.current_token.type == TokenType.POWER:
            op = self.current_token.value
            self.advance()
            right = self.parse_power_expression()  # Right-associative recursion
            node = BinaryOpNode(node, op, right)
        
        return node
    
    def parse_unary_expression(self) -> ASTNode:
        """Parse unary expressions"""
        if self.current_token.type == TokenType.NOT:
            op = self.current_token.value
            self.advance()
            operand = self.parse_unary_expression()
            return UnaryOpNode(op, operand)
        elif self.current_token.type == TokenType.PLUS or self.current_token.type == TokenType.MINUS:
            op = self.current_token.value
            self.advance()
            # FHIRPath spec: UnaryExpression : PathExpression | ('+' | '-') UnaryExpression
            # We'll parse the right-hand side as a UnaryExpression to allow for e.g. --5 or -+5
            # Though typically it will be a PathExpression (which starts with PrimaryExpression)
            operand = self.parse_unary_expression()
            return UnaryOpNode(op, operand)
        
        return self.parse_path_expression()
    
    def parse_path_expression(self) -> ASTNode:
        """Parse path expressions with proper function call handling"""
        segments = []
        
        # Parse the first segment
        segments.append(self.parse_primary_expression())
        
        # Parse additional segments separated by dots
        while self.current_token.type == TokenType.DOT:
            self.advance()  # Skip the dot
            # The next segment is parsed as a primary expression, which handles
            # identifiers, function calls, and indexers.
            segments.append(self.parse_primary_expression())
        
        if len(segments) == 1:
            return segments[0]
        else:
            return PathNode(segments)
    
    def parse_tuple_literal(self) -> ASTNode:
        """Parse tuple literal {key: value, ...} or list literal {item1, item2, ...}"""
        
        if self.current_token.type != TokenType.LBRACE:
            raise ValueError("Expected '{' to start tuple/list literal")
        
        # Peek ahead to detect list literal vs tuple literal
        # We need to distinguish between tuple keys like {key: value} and DateTime literals like {@2013-01-02T00:00:00.000Z}
        is_tuple_literal = self._is_tuple_literal()
        if not is_tuple_literal:
            # This is a list literal {item1, item2, item3}
            return self._parse_list_literal()
        
        # This is a tuple literal {key: value, key: value, ...}
        self.advance()  # Skip '{'
        
        elements = []
        
        # Handle empty tuple
        if self.current_token.type == TokenType.RBRACE:
            self.advance()  # Skip '}'
            return TupleNode(elements)
        
        # Parse key-value pairs
        while True:
            # Parse key - try to parse as expression first, then fall back to simple cases
            saved_pos = self.position
            saved_token = self.current_token
            
            try:
                # Try parsing as an expression (handles all computed key cases)
                key_expr = self.parse_additive_expression()
                
                # Check if the next token is a colon (confirming this is a valid key)
                if self.current_token.type == TokenType.COLON:
                    # This is a computed key expression
                    key = key_expr
                else:
                    # Not a valid key expression, restore position and handle as simple case
                    self.position = saved_pos
                    self.current_token = saved_token
                    raise ValueError("Not an expression key")
                    
            except:
                # Restore position and handle simple cases
                self.position = saved_pos
                self.current_token = saved_token
                
                if self.current_token.type == TokenType.STRING:
                    # Simple string literal key
                    key = self.current_token.value
                    self.advance()
                elif self.current_token.type == TokenType.IDENTIFIER:
                    # Simple identifier key
                    key = self.current_token.value
                    self.advance()
                elif self.current_token.type == TokenType.DATETIME:
                    # DateTime literal key (for expressions like @2013-01-02T00:00:00.000Z)
                    key = self.current_token.value
                    self.advance()
                elif self.current_token.type == TokenType.INTEGER:
                    # Integer literal key
                    key = self.current_token.value
                    self.advance()
                elif self.current_token.type == TokenType.DECIMAL:
                    # Decimal literal key
                    key = self.current_token.value
                    self.advance()
                else:
                    raise ValueError(f"Expected string, identifier, or expression for tuple key, found {self.current_token}")
            
            # Expect colon
            if self.current_token.type != TokenType.COLON:
                raise ValueError(f"Expected ':' after tuple key, found {self.current_token}")
            self.advance()  # Skip ':'
            
            # Parse value (any expression)
            value = self.parse_or_expression()
            
            elements.append((key, value))
            
            # Check for comma (more elements) or closing brace
            if self.current_token.type == TokenType.COMMA:
                self.advance()  # Skip ','
                continue
            elif self.current_token.type == TokenType.RBRACE:
                self.advance()  # Skip '}'
                break
            else:
                raise ValueError(f"Expected ',' or '}}' in tuple literal, found {self.current_token}")
        
        return TupleNode(elements)
    
    def parse_primary_expression(self) -> ASTNode:
        """Parse primary expressions"""
        node: ASTNode

        # Initialize variables
        node = None
        name = None
        
        if self.current_token.type == TokenType.STRING:
            if self._is_quoted_identifier_context():
                # Handle quoted identifiers like "Patient Age at Asthma Encounter"
                name = self._normalize_quoted_identifier(self.current_token.value)
                self.advance()
            else:
                # Handle regular string literals
                value = self.current_token.value
                self.advance()
                node = LiteralNode(value, 'string')
        elif self.current_token.type == TokenType.INTEGER:
            value = self.current_token.value
            # Handle long integers (keep as string to preserve 'L' suffix)
            if isinstance(value, str) and value.endswith('L'):
                self.advance()
                node = LiteralNode(value, 'long_integer')
            else:
                self.advance()
                
                # Check if this integer is followed by a string (unit) to form a quantity
                if self.current_token.type == TokenType.STRING:
                    unit = self.current_token.value
                    self.advance()
                    quantity_data = {'value': str(value), 'unit': unit}
                    node = LiteralNode(quantity_data, 'quantity')
                else:
                    node = LiteralNode(int(value), 'integer')
        elif self.current_token.type == TokenType.DECIMAL:
            value = float(self.current_token.value)
            self.advance()
            
            # Check if this decimal is followed by a string (unit) to form a quantity
            if self.current_token.type == TokenType.STRING:
                unit = self.current_token.value
                self.advance()
                quantity_data = {'value': str(value), 'unit': unit}
                node = LiteralNode(quantity_data, 'quantity')
            else:
                node = LiteralNode(value, 'decimal')
        elif self.current_token.type == TokenType.QUANTITY:
            quantity_data = self.current_token.value
            self.advance()
            node = LiteralNode(quantity_data, 'quantity')
        elif self.current_token.type == TokenType.DATETIME:
            datetime_value = self.current_token.value
            self.advance()
            node = LiteralNode(datetime_value, 'datetime')
        elif self.current_token.type == TokenType.BOOLEAN:
            value = self.current_token.value.lower() == 'true'
            self.advance()
            node = LiteralNode(value, 'boolean')
        elif self.current_token.type == TokenType.IDENTIFIER:
            name = self.current_token.value
            self.advance()
        else:
            name = None
            
        if name is not None:
            # Check if it's a function call
            if self.current_token.type == TokenType.LPAREN:
                self.advance()  # Skip '('
                args = []
                if self.current_token.type != TokenType.RPAREN:
                    # Parse the first argument, which might be a CQL query expression
                    first_arg = self.parse_or_expression()
                    
                    # Check if this might be a CQL query expression with an alias
                    if (self.current_token.type == TokenType.IDENTIFIER and 
                        self.current_token.type != TokenType.COMMA and 
                        self.current_token.type != TokenType.RPAREN):
                        # This looks like a query expression with alias
                        first_arg = self.parse_cql_query_expression(first_arg)
                    
                    args.append(first_arg)
                    
                    while self.current_token.type == TokenType.COMMA:
                        self.advance()  # Skip ','
                        args.append(self.parse_or_expression())
                if self.current_token.type == TokenType.RPAREN:
                    self.advance()  # Skip ')'
                else:
                    raise ValueError(f"Expected ')' after function arguments, found {self.current_token}")
                node = FunctionCallNode(name, args)
            elif self.current_token.type == TokenType.LBRACE:
                # System type constructor (e.g., Code { code: '8480-6' })
                constructor_node = self.parse_tuple_literal()
                # Create a system type constructor node - for now, treat as a special function call
                node = FunctionCallNode(name, [constructor_node])
            else:
                node = IdentifierNode(name)
        elif self.current_token.type == TokenType.LPAREN:
            self.advance()  # Skip '('
            inner_node = self.parse_or_expression()
            
            # Check if this might be a CQL query expression with an alias
            if (self.current_token.type == TokenType.IDENTIFIER and 
                self.current_token.type != TokenType.RPAREN):
                # This looks like a query expression with alias inside parentheses
                inner_node = self.parse_cql_query_expression(inner_node)
            
            if self.current_token.type == TokenType.RPAREN:
                self.advance()  # Skip ')'
            else:
                raise ValueError(f"Expected ')' after parenthesized expression, found {self.current_token}")
            node = inner_node
        elif self.current_token.type == TokenType.DOLLAR_THIS:
            self.advance()
            node = ThisNode()
        elif self.current_token.type == TokenType.DOLLAR_INDEX:
            self.advance()
            node = VariableNode('index')
        elif self.current_token.type == TokenType.DOLLAR_TOTAL:
            self.advance()
            node = VariableNode('total')
        elif self.current_token.type == TokenType.LBRACE:
            node = self.parse_tuple_literal()
        elif self.current_token.type == TokenType.LBRACKET and self._is_resource_query_context():
            # Handle CQL resource queries like [Patient] or [Condition: "Asthma"]
            node = self._parse_resource_query()
        else:
            raise ValueError(f"Unexpected token: {self.current_token}")

        # After the primary component (literal, identifier, func call, paren-expr) is parsed,
        # check if an indexer is applied to it, or if it's an interval constructor.
        while self.current_token.type == TokenType.LBRACKET:
            # Check if this is an interval constructor (Interval[x, y])
            if (isinstance(node, IdentifierNode) and 
                node.name == 'Interval' and 
                ',' in self._peek_until_closing_bracket()):
                # This is an interval constructor
                node = self._parse_interval_constructor(node)
            else:
                # This is a regular indexer
                self.advance()  # Skip '['
                index_expr = self.parse_or_expression()
                if self.current_token.type == TokenType.RBRACKET:
                    self.advance()  # Skip ']'
                else:
                    raise ValueError(f"Expected ']' after index expression, found {self.current_token}")
                node = IndexerNode(node, index_expr)
        
        return node
    
    def _peek_until_closing_delimiter(self, open_token: 'TokenType', close_token: 'TokenType') -> str:
        """
        Peek ahead to find content within delimiters for syntax detection.
        Returns the content between delimiters without consuming tokens.
        
        Args:
            open_token: Opening delimiter token type (LBRACKET or LBRACE)
            close_token: Closing delimiter token type (RBRACKET or RBRACE)
            
        Returns:
            String content between delimiters
        """
        with self.preserve_parser_state():
            content = ""
            delimiter_depth = 0
            
            # We're currently at the opening delimiter, skip it
            if self.current_token and self.current_token.type == open_token:
                delimiter_depth = 1
                self.advance()
            
            # Collect tokens until we find the matching closing delimiter
            while self.current_token and delimiter_depth > 0:
                if self.current_token.type == open_token:
                    delimiter_depth += 1
                elif self.current_token.type == close_token:
                    delimiter_depth -= 1
                
                if delimiter_depth > 0:  # Don't include the final closing delimiter
                    content += self.current_token.value + " "
                
                self.advance()
            
            return content.strip()
    
    def _is_tuple_literal(self) -> bool:
        """
        Determine if the brace expression is a tuple literal {key: value} or list literal {item1, item2}.
        
        This method looks for the pattern where there's a colon at the top level (not inside 
        DateTime literals or other nested structures) that appears to separate a key from a value.
        
        Returns:
            True if this appears to be a tuple literal, False if it's a list literal
        """
        with self.preserve_parser_state():
            # Skip opening brace
            if self.current_token and self.current_token.type == TokenType.LBRACE:
                self.advance()
            
            # Try to parse the first element to see if it's followed by a colon
            try:
                # Look for a pattern like: identifier/string followed by colon
                if self.current_token.type in [TokenType.IDENTIFIER, TokenType.STRING]:
                    self.advance()
                    # Check if next token is colon (indicating tuple key:value pattern)
                    return self.current_token and self.current_token.type == TokenType.COLON
                elif self.current_token.type in [TokenType.DATETIME, TokenType.INTEGER, TokenType.DECIMAL]:
                    # For DateTime, integer, or decimal literals, check if they're followed by colon
                    self.advance()
                    return self.current_token and self.current_token.type == TokenType.COLON
                else:
                    # Try to parse a more complex expression
                    try:
                        # Parse expression and check if followed by colon
                        expr = self.parse_additive_expression()
                        return self.current_token and self.current_token.type == TokenType.COLON
                    except:
                        # If expression parsing fails, assume it's a list
                        return False
            except:
                # If parsing fails, default to list literal
                return False
    
    def _peek_until_closing_bracket(self) -> str:
        """Peek ahead to find content within brackets for comma detection."""
        return self._peek_until_closing_delimiter(TokenType.LBRACKET, TokenType.RBRACKET)
    
    def _peek_until_closing_brace(self) -> str:
        """Peek ahead to find content within braces for colon detection."""
        return self._peek_until_closing_delimiter(TokenType.LBRACE, TokenType.RBRACE)
    
    def _parse_interval_constructor(self, identifier_node: ASTNode) -> IntervalConstructorNode:
        """
        Parse Interval[start, end] syntax.
        identifier_node should be 'Interval' identifier.
        """
        # We're currently at the opening bracket
        self.advance()  # Skip '['
        
        # Parse start expression
        start_expr = self.parse_or_expression()
        
        # Expect comma
        if self.current_token.type != TokenType.COMMA:
            raise ValueError(f"Expected ',' in interval constructor, found {self.current_token}")
        self.advance()  # Skip comma
        
        # Parse end expression
        end_expr = self.parse_or_expression()
        
        # Expect closing bracket
        if self.current_token.type != TokenType.RBRACKET:
            raise ValueError(f"Expected ']' in interval constructor, found {self.current_token}")
        self.advance()  # Skip ']'
        
        return IntervalConstructorNode(start_expr, end_expr)
    
    def _parse_list_literal(self) -> ListLiteralNode:
        """
        Parse {item1, item2, item3} list literal syntax.
        We're currently at the opening brace.
        """
        self.advance()  # Skip '{'
        
        elements = []
        
        # Handle empty list
        if self.current_token.type == TokenType.RBRACE:
            self.advance()  # Skip '}'
            return ListLiteralNode(elements)
        
        # Parse first element
        elements.append(self.parse_or_expression())
        
        # Parse remaining elements
        while self.current_token.type == TokenType.COMMA:
            self.advance()  # Skip comma
            elements.append(self.parse_or_expression())
        
        # Expect closing brace
        if self.current_token.type != TokenType.RBRACE:
            raise ValueError(f"Expected '}}' or ',' in list literal, found {self.current_token}")
        self.advance()  # Skip '}'
        
        return ListLiteralNode(elements)
    
    def parse_cql_query_expression(self, source_expr: ASTNode) -> CQLQueryExpressionNode:
        """Parse a CQL query expression with optional alias, where, sort clauses"""
        alias = None
        where_clause = None
        sort_clause = None
        sort_direction = "asc"
        return_clause = None
        
        # Parse alias if present
        if self.current_token.type == TokenType.IDENTIFIER:
            alias = self.current_token.value
            self.advance()
        
        # Parse optional clauses
        while self.current_token.type in [TokenType.WHERE, TokenType.SORT, TokenType.RETURN]:
            if self.current_token.type == TokenType.WHERE:
                self.advance()  # Skip 'where'
                where_clause = self.parse_or_expression()
            elif self.current_token.type == TokenType.SORT:
                self.advance()  # Skip 'sort'
                if self.current_token.type == TokenType.BY:
                    self.advance()  # Skip 'by'
                    sort_clause = self.parse_or_expression()
                    # Check for optional direction (asc/desc)
                    if self.current_token.type == TokenType.IDENTIFIER:
                        if self.current_token.value.lower() in ['asc', 'desc']:
                            sort_direction = self.current_token.value.lower()
                            self.advance()
                else:
                    raise ValueError(f"Expected 'by' after 'sort', found {self.current_token}")
            elif self.current_token.type == TokenType.RETURN:
                self.advance()  # Skip 'return'
                return_clause = self.parse_or_expression()
        
        return CQLQueryExpressionNode(
            source=source_expr,
            alias=alias,
            where_clause=where_clause,
            sort_clause=sort_clause,
            sort_direction=sort_direction,
            return_clause=return_clause
        )
    
    def _is_quoted_identifier_context(self) -> bool:
        """
        Determine if a quoted string should be treated as an identifier.
        
        This helps distinguish between actual string literals and quoted identifiers
        like "Patient Age at Asthma Encounter" which should be treated as variable names.
        
        Returns:
            True if the current context suggests the quoted string is an identifier
        """
        # Look ahead to see what follows the quoted string
        if self.position + 1 < len(self.tokens):
            next_token = self.tokens[self.position + 1]
            
            # If followed by (, it's likely a function call with quoted name
            if next_token.type == TokenType.LPAREN:
                return True
            
            # If followed by operators that work on identifiers, likely an identifier
            if next_token.type in [TokenType.DOT, TokenType.EQUALS, TokenType.NOT_EQUALS, 
                                   TokenType.AND, TokenType.OR]:
                return True
            
            # If at beginning of expression or after certain keywords, likely identifier
            if self.position == 0:
                return True
            
            prev_token = self.tokens[self.position - 1]
            if prev_token.type in [TokenType.LPAREN, TokenType.COMMA, TokenType.AND, TokenType.OR]:
                return True
        
        # Default to treating as string literal
        return False
    
    def _normalize_quoted_identifier(self, quoted_string: str) -> str:
        """
        Convert quoted identifiers to valid normalized identifiers.
        
        Args:
            quoted_string: The quoted string value (without quotes)
            
        Returns:
            Normalized identifier suitable for use as variable/function names
        """
        # Remove any remaining quotes if present
        identifier = quoted_string.strip('"\'')
        
        # Replace spaces with underscores
        identifier = re.sub(r'\s+', '_', identifier)
        
        # Remove special characters except underscores
        identifier = re.sub(r'[^\w_]', '', identifier)
        
        # Ensure it starts with a letter or underscore
        if identifier and not (identifier[0].isalpha() or identifier[0] == '_'):
            identifier = '_' + identifier
        
        # Convert to lowercase for consistency
        return identifier.lower()
    
    def _is_resource_query_context(self) -> bool:
        """
        Determine if a bracketed expression is a CQL resource query.
        
        CQL resource queries have the form: [ResourceType] or [ResourceType: "ValueSet"]
        FHIRPath indexers have the form: expression[index]
        
        Returns:
            True if this appears to be a CQL resource query
        """
        if self.current_token.type != TokenType.LBRACKET:
            return False
            
        # Look ahead to see the content within brackets
        bracket_content = self._peek_until_closing_bracket()
        
        # Check for resource query patterns
        if ':' in bracket_content:
            # Pattern: [ResourceType: "ValueSet"]
            parts = bracket_content.split(':', 1)
            resource_part = parts[0].strip()
            
            # Check if first part looks like a FHIR resource type (starts with capital)
            if resource_part and resource_part[0].isupper():
                return True
        else:
            # Pattern: [ResourceType]  
            resource_part = bracket_content.strip()
            
            # Check if it looks like a FHIR resource type
            if (resource_part and 
                resource_part[0].isupper() and 
                resource_part.isalpha() and
                len(resource_part) > 1):
                return True
        
        return False
    
    def _parse_resource_query(self) -> ResourceQueryNode:
        """
        Parse a CQL resource query like [Patient] or [Condition: "Asthma"].
        
        Returns:
            ResourceQueryNode representing the parsed resource query
        """
        if self.current_token.type != TokenType.LBRACKET:
            raise ValueError(f"Expected '[' at start of resource query, found {self.current_token}")
        
        self.advance()  # Skip '['
        
        # Parse resource type
        if self.current_token.type != TokenType.IDENTIFIER:
            raise ValueError(f"Expected resource type identifier in resource query, found {self.current_token}")
        
        resource_type = self.current_token.value
        self.advance()
        
        # Check for optional filter specification
        code_filter = None
        code_path = "code"  # Default code path
        
        if self.current_token.type == TokenType.COLON:
            self.advance()  # Skip ':'
            
            # Parse the filter specification (usually a quoted string)
            if self.current_token.type == TokenType.STRING:
                code_filter = self.current_token.value
                self.advance()
            elif self.current_token.type == TokenType.IDENTIFIER:
                # Handle unquoted filter names
                code_filter = self.current_token.value
                self.advance()
            else:
                raise ValueError(f"Expected filter specification after ':' in resource query, found {self.current_token}")
        
        # Expect closing bracket
        if self.current_token.type != TokenType.RBRACKET:
            raise ValueError(f"Expected ']' at end of resource query, found {self.current_token}")
        
        self.advance()  # Skip ']'
        
        return ResourceQueryNode(
            resource_type=resource_type,
            code_filter=code_filter,
            code_path=code_path
        )
    
    def parse_with_error_context(self, expression: str) -> ASTNode:
        """
        Parse FHIRPath/CQL expression with enhanced error context.
        
        Args:
            expression: The expression text to parse
            
        Returns:
            Parsed AST node
            
        Raises:
            ValueError: Enhanced parsing error with context information
        """
        try:
            # Store original expression for error reporting
            self.original_expression = expression
            return self.parse()
        except (ValueError, IndexError, AttributeError) as e:
            # Add context information to the error
            current_pos = getattr(self, 'current_position', 0)
            line_num = self._get_line_number(expression, current_pos)
            column_num = self._get_column_number(expression, current_pos)
            context_lines = self._get_context_lines(expression, line_num)
            
            enhanced_error = f"""
Parse Error at line {line_num}, column {column_num}:
{str(e)}

Context:
{context_lines}

Expression: {expression}
"""
            raise ValueError(enhanced_error) from e
    
    def _get_line_number(self, expression: str, position: int) -> int:
        """Get line number for character position in expression."""
        if position >= len(expression):
            position = len(expression) - 1
        
        return expression[:position].count('\n') + 1
    
    def _get_column_number(self, expression: str, position: int) -> int:
        """Get column number for character position in expression."""
        if position >= len(expression):
            position = len(expression) - 1
        
        last_newline = expression.rfind('\n', 0, position)
        if last_newline == -1:
            return position + 1
        else:
            return position - last_newline
    
    def _get_context_lines(self, expression: str, line_num: int, context_size: int = 2) -> str:
        """Get context lines around the error line."""
        lines = expression.split('\n')
        start_line = max(0, line_num - context_size - 1)
        end_line = min(len(lines), line_num + context_size)
        
        context_lines = []
        for i in range(start_line, end_line):
            line_number = i + 1
            marker = " -> " if line_number == line_num else "    "
            context_lines.append(f"{line_number:3d}{marker}{lines[i]}")
        
        return '\n'.join(context_lines)