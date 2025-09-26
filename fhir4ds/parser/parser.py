from ..ast import nodes
from ..ast.metadata import PopulationMetadata
from .lexer import FHIRPathLexer
from .tokens import Token, TokenType
from .exceptions import ParserError


class FHIRPathParser:
    """
    Parses a FHIRPath expression into an Abstract Syntax Tree (AST).

    This is a simplified parser that focuses on path expressions and function
    calls, which are the core components for the CTE generation logic. It uses
    a recursive descent approach.
    """

    def __init__(self):
        self.lexer = None
        self.tokens = None
        self.current_token: Token = None

    def parse(self, expression: str) -> nodes.FHIRPathNode:
        self.lexer = FHIRPathLexer(expression)
        self.tokens = self.lexer.tokenize()
        self._advance()

        ast = self._parse_path_expression()

        if self.current_token.token_type != TokenType.EOF:
            raise ParserError(
                f"Unexpected token '{self.current_token.value}' at end of expression.",
                self.current_token.source_location,
            )
        return ast

    def _advance(self):
        self.current_token = next(self.tokens, None)

    def _eat(self, token_type: TokenType):
        if self.current_token and self.current_token.token_type == token_type:
            self._advance()
        else:
            expected = token_type.name
            actual = self.current_token.token_type.name if self.current_token else "EOF"
            location = self.current_token.source_location if self.current_token else None
            raise ParserError(f"Expected token {expected}, but got {actual}", location)

    def _parse_path_expression(self) -> nodes.FHIRPathNode:
        """Parses a path expression like `Patient.name.given.first()`."""
        path_parts = [self._parse_term()]

        while self.current_token and self.current_token.token_type == TokenType.DOT:
            self._eat(TokenType.DOT)
            path_parts.append(self._parse_term())

        if len(path_parts) == 1:
            return path_parts[0]
        else:
            return nodes.PathExpression(
                path=path_parts,
                source_location=path_parts[0].source_location,
                metadata=PopulationMetadata(),
            )

    def _parse_term(self) -> nodes.FHIRPathNode:
        """Parses a term, which can be an identifier or a function call."""
        if not self.current_token or self.current_token.token_type != TokenType.IDENTIFIER:
            raise ParserError(
                f"Expected an identifier, but got {self.current_token.token_type if self.current_token else 'None'}",
                self.current_token.source_location if self.current_token else None,
            )

        name_node = nodes.Identifier(
            value=self.current_token.value,
            source_location=self.current_token.source_location,
            metadata=PopulationMetadata(),
        )
        self._advance()

        if self.current_token and self.current_token.token_type == TokenType.LPAREN:
            return self._parse_function_call(name_node)

        return name_node

    def _parse_function_call(self, name_node: nodes.Identifier) -> nodes.FunctionCall:
        """Parses function arguments, e.g., `()` or `(expression, ...)`."""
        self._eat(TokenType.LPAREN)

        args = []
        if self.current_token and self.current_token.token_type != TokenType.RPAREN:
            while True:
                args.append(self._parse_literal_or_path())
                if not self.current_token or self.current_token.token_type == TokenType.RPAREN:
                    break
                self._eat(TokenType.COMMA)

        self._eat(TokenType.RPAREN)
        return nodes.FunctionCall(
            name=name_node,
            arguments=args,
            source_location=name_node.source_location,
            metadata=PopulationMetadata(),
        )

    def _parse_literal_or_path(self) -> nodes.FHIRPathNode:
        """Parses a literal value or a path expression (for function arguments)."""
        token = self.current_token
        if token.token_type == TokenType.STRING_LITERAL:
            self._advance()
            return nodes.StringLiteral(value=token.value, source_location=token.source_location, metadata=PopulationMetadata())
        elif token.token_type == TokenType.INTEGER_LITERAL:
            self._advance()
            return nodes.NumberLiteral(value=int(token.value), source_location=token.source_location, metadata=PopulationMetadata())
        elif token.token_type == TokenType.DECIMAL_LITERAL:
            self._advance()
            return nodes.NumberLiteral(value=float(token.value), source_location=token.source_location, metadata=PopulationMetadata())
        elif token.token_type == TokenType.BOOLEAN_LITERAL:
            self._advance()
            return nodes.BooleanLiteral(value=token.value == 'true', source_location=token.source_location, metadata=PopulationMetadata())
        else:
            return self._parse_path_expression()