# fhir4ds.parser.fhirpath_parser

"""
FHIRPath Parser
---------------

This module contains the core FHIRPath parser. It takes a stream of tokens
from the lexer and builds an Abstract Syntax Tree (AST).
"""

from dataclasses import dataclass
from typing import List, Any

from fhir4ds.ast.nodes import (
    Identifier, StringLiteral, NumberLiteral, BooleanLiteral,
    DateLiteral, DateTimeLiteral, TimeLiteral, QuantityLiteral,
    FHIRPathNode, SourceLocation, PathExpression, FunctionCall,
    UnaryOperation, UnaryOperator, BinaryOperation, Operator
)
from fhir4ds.ast.metadata import PopulationMetadata
from .exceptions import ParseError
from .precedence import PRECEDENCE, ASSOCIATIVITY


@dataclass(frozen=True)
class Token:
    """Represents a token from the lexer."""
    type: str
    value: Any
    line: int
    column: int


class FHIRPathParser:
    """
    FHIRPath recursive descent parser.
    """
    def __init__(self, tokens):
        self.tokens = list(tokens)
        self.pos = 0

    def parse(self):
        expression = self._parse_expression()
        if not self._is_at_end():
            token = self._peek()
            raise ParseError(
                f"Unexpected token '{token.value}' found after expression.",
                token.line,
                token.column
            )
        return expression

    def _parse_expression(self, precedence=0) -> FHIRPathNode:
        """
        Parses a FHIRPath expression using a precedence climbing algorithm
        to handle binary operators correctly.
        """
        left = self._parse_invocation_expression()

        while not self._is_at_end():
            op_token = self._peek()
            if op_token is None or op_token.value not in PRECEDENCE:
                break

            op_precedence = PRECEDENCE[op_token.value]
            if op_precedence < precedence:
                break

            next_precedence = op_precedence + (1 if ASSOCIATIVITY.get(op_token.value, 'LEFT') == 'LEFT' else 0)

            self._consume() # Consume the operator token
            right = self._parse_expression(next_precedence)
            op = Operator(op_token.value)

            left = BinaryOperation(
                left=left,
                operator=op,
                right=right,
                source_location=left.source_location,
                metadata=self._create_metadata()
            )
        return left

    def _parse_invocation_expression(self) -> FHIRPathNode:
        """
        Parses an invocation expression, which is a unary expression followed by
        zero or more path invocations (e.g., .member or .function()).
        """
        node = self._parse_unary_expression()

        while self._match('.'):
            identifier = self._parse_identifier()

            if self._match('('):
                invocation = self._parse_function_call(identifier)
            else:
                invocation = identifier

            base_path = node.path if isinstance(node, PathExpression) else [node]
            node = PathExpression(
                path=base_path + [invocation],
                source_location=node.source_location,
                metadata=self._create_metadata()
            )
        return node

    def _parse_unary_expression(self) -> FHIRPathNode:
        """ Parses unary expressions like `-1` or `not true`. """
        if self._match('-') or self._match_identifier('not'):
            op_token = self._peek(-1)
            operator = UnaryOperator.MINUS if op_token.value == '-' else UnaryOperator.NOT

            # Unary operators apply to the entire following invocation expression.
            operand = self._parse_invocation_expression()

            loc = self._get_source_location(op_token)
            return UnaryOperation(operator=operator, operand=operand, source_location=loc, metadata=self._create_metadata())

        return self._parse_primary_expression()

    def _parse_primary_expression(self) -> FHIRPathNode:
        """
        Parses the most basic elements: literals, parenthesized expressions,
        and identifiers (which could be standalone function calls).
        """
        token = self._peek()
        if not token:
            raise ParseError("Unexpected end of expression.")

        if self._match('('):
            expr = self._parse_expression()
            self._expect_value(')')
            return expr
        elif token.type.endswith('_LITERAL'):
            return self._parse_literal()
        elif token.type == 'IDENTIFIER':
            identifier = self._parse_identifier()
            if self._match('('):
                return self._parse_function_call(identifier)
            return identifier
        else:
            raise ParseError(
                f"Unexpected token '{token.value}' when expecting an expression.",
                token.line, token.column
            )

    def _parse_function_call(self, func_name_node: Identifier) -> FunctionCall:
        # Assumes the '(' has just been matched.
        args = self._parse_argument_list()
        self._expect_value(')')
        return FunctionCall(
            name=func_name_node,
            arguments=args,
            source_location=func_name_node.source_location,
            metadata=self._create_metadata()
        )

    def _parse_argument_list(self) -> List[FHIRPathNode]:
        args = []
        if not self._check(')'):
            args.append(self._parse_expression())
            while self._match(','):
                args.append(self._parse_expression())
        return args

    def _parse_identifier(self) -> Identifier:
        token = self._expect_type('IDENTIFIER')
        return Identifier(
            value=token.value,
            source_location=self._get_source_location(token),
            metadata=self._create_metadata()
        )

    def _parse_literal(self) -> FHIRPathNode:
        token = self._consume()
        loc = self._get_source_location(token)
        meta = self._create_metadata()

        if token.type == 'STRING_LITERAL':
            return StringLiteral(value=token.value, source_location=loc, metadata=meta)
        if token.type in ('NUMBER_LITERAL', 'INTEGER_LITERAL', 'DECIMAL_LITERAL'):
            return NumberLiteral(value=float(token.value), source_location=loc, metadata=meta)
        if token.type == 'BOOLEAN_LITERAL':
            return BooleanLiteral(value=(token.value.lower() == 'true'), source_location=loc, metadata=meta)
        if token.type == 'DATE_LITERAL':
            return DateLiteral(value=token.value, source_location=loc, metadata=meta)
        if token.type == 'DATETIME_LITERAL':
            return DateTimeLiteral(value=token.value, source_location=loc, metadata=meta)
        if token.type == 'TIME_LITERAL':
            return TimeLiteral(value=token.value, source_location=loc, metadata=meta)
        if token.type == 'QUANTITY_LITERAL':
            val, unit = token.value
            return QuantityLiteral(value=float(val), unit=unit, source_location=loc, metadata=meta)

        raise ParseError(f"Unknown literal type: {token.type}", token.line, token.column)

    def _get_source_location(self, token: Token) -> SourceLocation:
        return SourceLocation(line=token.line, column=token.column)

    def _create_metadata(self) -> PopulationMetadata:
        return PopulationMetadata()

    def _peek(self, offset=0):
        if self.pos + offset >= len(self.tokens):
            return None
        return self.tokens[self.pos + offset]

    def _consume(self):
        token = self._peek()
        if token:
            self.pos += 1
        return token

    def _match(self, *token_values):
        token = self._peek()
        if token and token.value in token_values:
            self._consume()
            return True
        return False

    def _match_identifier(self, *values):
        token = self._peek()
        if token and token.type == 'IDENTIFIER' and token.value in values:
            self._consume()
            return True
        return False

    def _check(self, value):
        token = self._peek()
        return token and token.value == value

    def _expect_type(self, token_type):
        token = self._peek()
        if not token:
            raise ParseError(f"Expected token of type {token_type}, but found EOF")
        if token.type != token_type:
            raise ParseError(
                f"Expected token of type {token_type}, but found {token.type}",
                token.line,
                token.column
            )
        return self._consume()

    def _expect_value(self, value):
        token = self._peek()
        if not token or token.value != value:
            found = f"'{token.value}'" if token else "EOF"
            raise ParseError(f"Expected '{value}', but found {found}",
                             token.line if token else -1,
                             token.column if token else -1)
        return self._consume()

    def _is_at_end(self):
        return self.pos >= len(self.tokens)
