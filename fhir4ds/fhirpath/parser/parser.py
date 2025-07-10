"""
FHIRPath Lexer and Parser

This module contains the lexical analyzer and parser for FHIRPath expressions.
It converts FHIRPath strings into Abstract Syntax Trees (ASTs) for further processing.
"""

import re
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional

from .ast_nodes import (
    ASTNode, ThisNode, LiteralNode, IdentifierNode, FunctionCallNode,
    BinaryOpNode, UnaryOpNode, PathNode, IndexerNode, TupleNode
)


class TokenType(Enum):
    """Token types for FHIRPath lexer"""
    IDENTIFIER = "IDENTIFIER"
    STRING = "STRING"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
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
    MINUS = "MINUS"
    MULTIPLY = "MULTIPLY"
    DIVIDE = "DIVIDE"
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
        """Read a string literal"""
        quote_char = self.current_char
        self.advance()  # Skip opening quote
        result = ""
        while self.current_char and self.current_char != quote_char:
            if self.current_char == '\\':
                self.advance()
                if self.current_char:
                    result += self.current_char
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
            # Check for $this keyword first
            if self.expression[self.position:].startswith('$this'):
                # Ensure it's not part of a larger identifier like $thisValue
                if self.position + 5 == len(self.expression) or not self.expression[self.position + 5].isalnum():
                    tokens.append(Token(TokenType.DOLLAR_THIS, '$this', self.position))
                    for _ in range(5): self.advance()
                    continue
            if not self.current_char:
                break
            
            pos = self.position
            
            if self.current_char in ['"', "'"]:
                value = self.read_string()
                tokens.append(Token(TokenType.STRING, value, pos))
            elif self.current_char.isdigit():
                value = self.read_number()
                if '.' in value:
                    tokens.append(Token(TokenType.DECIMAL, value, pos))
                else:
                    tokens.append(Token(TokenType.INTEGER, value, pos))
            elif self.current_char.isalpha() or self.current_char == '_':
                value = self.read_identifier()
                # Check for keywords - but only treat "not" as a keyword if it's not followed by "("
                # This allows .not() to be parsed as a function call
                if value.lower() == 'and':
                    tokens.append(Token(TokenType.AND, value, pos))
                elif value.lower() == 'or':
                    tokens.append(Token(TokenType.OR, value, pos))
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
    
    def advance(self):
        """Move to the next token"""
        self.position += 1
        if self.position >= len(self.tokens):
            self.current_token = Token(TokenType.EOF, '', -1)
        else:
            self.current_token = self.tokens[self.position]
    
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
        node = self.parse_relational_expression()
        
        while self.current_token.type in [TokenType.EQUALS, TokenType.NOT_EQUALS, TokenType.EQUIVALENT, TokenType.NOT_EQUIVALENT]:
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
        node = self.parse_unary_expression()
        
        while self.current_token.type in [TokenType.MULTIPLY, TokenType.DIVIDE]:
            op = self.current_token.value
            self.advance()
            right = self.parse_unary_expression()
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
        """Parse tuple literal {key: value, key: value, ...}"""
        
        if self.current_token.type != TokenType.LBRACE:
            raise ValueError("Expected '{' to start tuple literal")
        
        self.advance()  # Skip '{'
        
        elements = []
        
        # Handle empty tuple
        if self.current_token.type == TokenType.RBRACE:
            self.advance()  # Skip '}'
            return TupleNode(elements)
        
        # Parse key-value pairs
        while True:
            # Parse key (must be string or identifier)
            if self.current_token.type == TokenType.STRING:
                key = self.current_token.value
                self.advance()
            elif self.current_token.type == TokenType.IDENTIFIER:
                key = self.current_token.value
                self.advance()
            else:
                raise ValueError(f"Expected string or identifier for tuple key, found {self.current_token}")
            
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

        if self.current_token.type == TokenType.STRING:
            value = self.current_token.value
            self.advance()
            node = LiteralNode(value, 'string')
        elif self.current_token.type == TokenType.INTEGER:
            value = int(self.current_token.value)
            self.advance()
            node = LiteralNode(value, 'integer')
        elif self.current_token.type == TokenType.DECIMAL:
            value = float(self.current_token.value)
            self.advance()
            node = LiteralNode(value, 'decimal')
        elif self.current_token.type == TokenType.BOOLEAN:
            value = self.current_token.value.lower() == 'true'
            self.advance()
            node = LiteralNode(value, 'boolean')
        elif self.current_token.type == TokenType.IDENTIFIER:
            name = self.current_token.value
            self.advance()
            # Check if it's a function call
            if self.current_token.type == TokenType.LPAREN:
                self.advance()  # Skip '('
                args = []
                if self.current_token.type != TokenType.RPAREN:
                    args.append(self.parse_or_expression())
                    while self.current_token.type == TokenType.COMMA:
                        self.advance()  # Skip ','
                        args.append(self.parse_or_expression())
                if self.current_token.type == TokenType.RPAREN:
                    self.advance()  # Skip ')'
                else:
                    raise ValueError(f"Expected ')' after function arguments, found {self.current_token}")
                node = FunctionCallNode(name, args)
            else:
                node = IdentifierNode(name)
        elif self.current_token.type == TokenType.LPAREN:
            self.advance()  # Skip '('
            inner_node = self.parse_or_expression()
            if self.current_token.type == TokenType.RPAREN:
                self.advance()  # Skip ')'
            else:
                raise ValueError(f"Expected ')' after parenthesized expression, found {self.current_token}")
            node = inner_node
        elif self.current_token.type == TokenType.DOLLAR_THIS:
            self.advance()
            node = ThisNode()
        elif self.current_token.type == TokenType.LBRACE:
            node = self.parse_tuple_literal()
        else:
            raise ValueError(f"Unexpected token: {self.current_token}")

        # After the primary component (literal, identifier, func call, paren-expr) is parsed,
        # check if an indexer is applied to it.
        while self.current_token.type == TokenType.LBRACKET:
            self.advance()  # Skip '['
            index_expr = self.parse_or_expression()
            if self.current_token.type == TokenType.RBRACKET:
                self.advance()  # Skip ']'
            else:
                raise ValueError(f"Expected ']' after index expression, found {self.current_token}")
            node = IndexerNode(node, index_expr)
        
        return node