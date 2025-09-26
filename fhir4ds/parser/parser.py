from decimal import Decimal
from typing import Optional, List
from fhir4ds.parser.lexer import Token, TokenType
from fhir4ds.parser.literals import DateTimeParser
from fhir4ds.ast.nodes import (
    FHIRPathNode, Identifier, StringLiteral, NumberLiteral, BooleanLiteral,
    TimeLiteral, DateTimeLiteral, QuantityLiteral, CollectionLiteral,
    BinaryOperation, UnaryOperation, InvocationExpression, MemberAccess,
    Indexer, Operator, UnaryOperator, SourceLocation
)
from fhir4ds.ast.metadata import PopulationMetadata
from fhir4ds.parser.functions.registry import FHIRPathFunctionRegistry
from fhir4ds.parser.functions.core import (
    FirstFunction, LastFunction, TailFunction, ExistsFunction, EmptyFunction,
    NotFunction, CountFunction, WhereFunction, SelectFunction, SumFunction, AvgFunction
)
from fhir4ds.parser.exceptions import (
    FHIRPathParseError, FHIRPathSyntaxError, FHIRPathFunctionError, FHIRPathSemanticError
)
from fhir4ds.parser.configuration import ParserConfiguration, ConfigurationManager
from fhir4ds.parser.metadata_inference import MetadataInferenceEngine, ParseContext

class Parser:
    """
    Enhanced parser with architecture compliance, including configuration management,
    a proper exception hierarchy, and metadata inference before node creation.
    """
    def __init__(self, tokens: list[Token], config: Optional[ParserConfiguration] = None):
        self.tokens = tokens
        self.pos = 0
        self.config = config or ConfigurationManager().get_config()
        self.metadata_engine = MetadataInferenceEngine()
        self.parse_context = ParseContext()
        self.function_registry = FHIRPathFunctionRegistry()
        self._register_core_functions()

    def _register_core_functions(self):
        core_functions = [
            FirstFunction(), LastFunction(), TailFunction(), ExistsFunction(),
            EmptyFunction(), NotFunction(), CountFunction(), WhereFunction(),
            SelectFunction(), SumFunction(), AvgFunction()
        ]
        for func in core_functions:
            self.function_registry.register_function(func)

    def _peek(self) -> Token:
        return self.tokens[self.pos]

    def _previous(self) -> Token:
        return self.tokens[self.pos - 1]

    def _is_at_end(self) -> bool:
        return self._peek().token_type == TokenType.EOF

    def _advance(self) -> Token:
        if not self._is_at_end():
            self.pos += 1
        return self._previous()

    def _check(self, token_type: TokenType) -> bool:
        if self._is_at_end():
            return False
        return self._peek().token_type == token_type

    def _match(self, *token_types: TokenType) -> bool:
        for token_type in token_types:
            if self._check(token_type):
                self._advance()
                return True
        return False

    def _consume(self, token_type: TokenType, message: str) -> Token:
        if self._check(token_type):
            return self._advance()
        self._raise_syntax_error(expected=token_type.name, found=self._peek().token_type.name)

    def _get_current_location(self) -> SourceLocation:
        token = self._peek()
        return SourceLocation(line=token.location.line, column=token.location.column)

    def _get_error_context(self) -> str:
        start = max(0, self.pos - 5)
        end = min(len(self.tokens), self.pos + 5)
        context_tokens = " ".join([t.value for t in self.tokens[start:end]])
        return f"Around: ...{context_tokens}..."

    def _raise_parse_error(self, message: str, suggestion: Optional[str] = None):
        raise FHIRPathParseError(
            message, location=self._get_current_location(),
            context=self._get_error_context(), suggestion=suggestion
        )

    def _raise_syntax_error(self, expected: str, found: str):
        message = f"Expected {expected}, but found {found}"
        suggestion = f"Check for missing or misplaced tokens."
        raise FHIRPathSyntaxError(
            message, location=self._get_current_location(),
            context=self._get_error_context(), suggestion=suggestion
        )

    def parse(self) -> FHIRPathNode:
        expr = self._parse_expression()
        if not self._is_at_end():
            self._raise_parse_error("Unexpected tokens at the end of the expression.")
        return expr

    OPERATOR_MAP = {
        TokenType.PLUS: Operator.ADD, TokenType.MINUS: Operator.SUB,
        TokenType.MULTIPLY: Operator.MUL, TokenType.DIVIDE: Operator.DIV,
        TokenType.MOD: Operator.MOD, TokenType.EQUAL: Operator.EQ,
        TokenType.NOT_EQUAL: Operator.NE, TokenType.GREATER_THAN: Operator.GT,
        TokenType.GREATER_EQUAL: Operator.GTE, TokenType.LESS_THAN: Operator.LT,
        TokenType.LESS_EQUAL: Operator.LTE, TokenType.AND: Operator.AND,
        TokenType.OR: Operator.OR, TokenType.XOR: Operator.XOR,
        TokenType.IMPLIES: Operator.IMPLIES, TokenType.IS: Operator.IS,
        TokenType.AS: Operator.AS,
    }

    def _token_to_operator(self, token: Token) -> Operator:
        operator = self.OPERATOR_MAP.get(token.token_type)
        if operator is None:
            self._raise_parse_error(f"Unknown operator token: {token.value}")
        return operator

    def _binary_op_parser(self, higher_precedence_parser, *token_types: TokenType) -> FHIRPathNode:
        expr = higher_precedence_parser()
        while self._match(*token_types):
            op_token = self._previous()
            operator = self._token_to_operator(op_token)
            right = higher_precedence_parser()
            metadata = self.metadata_engine.infer_for_binary_operation(expr, operator, right, self.parse_context)
            expr = BinaryOperation(
                left=expr, operator=operator, right=right,
                source_location=self._get_current_location(), metadata=metadata
            )
        return expr

    def _parse_expression(self) -> FHIRPathNode:
        return self._parse_implies()

    def _parse_implies(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_logical_or, TokenType.IMPLIES)

    def _parse_logical_or(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_logical_and, TokenType.OR, TokenType.XOR)

    def _parse_logical_and(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_equality, TokenType.AND)

    def _parse_equality(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_comparison, TokenType.EQUAL, TokenType.NOT_EQUAL)

    def _parse_comparison(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_type_ops, TokenType.GREATER_THAN, TokenType.GREATER_EQUAL, TokenType.LESS_THAN, TokenType.LESS_EQUAL)

    def _parse_type_ops(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_term, TokenType.IS, TokenType.AS)

    def _parse_term(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_factor, TokenType.PLUS, TokenType.MINUS)

    def _parse_factor(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_unary, TokenType.MULTIPLY, TokenType.DIVIDE, TokenType.MOD)

    def _parse_unary(self) -> FHIRPathNode:
        if self._match(TokenType.MINUS):
            op = UnaryOperator.MINUS
            operand = self._parse_unary()
            metadata = self.metadata_engine.infer_for_unary_operation(op, operand, self.parse_context)
            return UnaryOperation(
                operator=op, operand=operand,
                source_location=self._get_current_location(), metadata=metadata
            )
        return self._parse_path_expression()

    def _parse_path_expression(self) -> FHIRPathNode:
        expr = self._parse_primary()
        while self._match(TokenType.DOT, TokenType.LBRACKET):
            token = self._previous()
            if token.token_type == TokenType.DOT:
                member_token = self._peek()
                if member_token.token_type == TokenType.EOF:
                    self._raise_parse_error("Unexpected end of expression after '.'")
                self._advance()
                if self._match(TokenType.LPAREN):
                    function_name = member_token.value
                    if self.function_registry.get_function(function_name) is None:
                        raise FHIRPathFunctionError(f"Unknown function: '{function_name}'", location=self._get_current_location())

                    arguments = []
                    if not self._check(TokenType.RPAREN):
                        arguments.append(self._parse_expression())
                        while self._match(TokenType.COMMA):
                            arguments.append(self._parse_expression())
                    self._consume(TokenType.RPAREN, "Expect ')' after function arguments.")

                    metadata = self.metadata_engine.infer_for_invocation(expr, function_name, arguments, self.parse_context)
                    function_impl = self.function_registry.get_function(function_name)
                    expr = function_impl.create_ast_node(expr, arguments)
                    expr.metadata = metadata
                else:
                    if member_token.token_type != TokenType.IDENTIFIER:
                        self._raise_semantic_error(f"Cannot access property '{member_token.value}' because it is a reserved keyword.")

                    member_metadata = self.metadata_engine.infer_for_identifier(member_token.value, self.parse_context)
                    member_identifier = Identifier(
                        value=member_token.value,
                        source_location=SourceLocation(member_token.location.line, member_token.location.column),
                        metadata=member_metadata,
                    )

                    access_metadata = self.metadata_engine.infer_for_member_access(expr, member_identifier, self.parse_context)
                    expr = MemberAccess(
                        expression=expr, member=member_identifier,
                        source_location=self._get_current_location(), metadata=access_metadata
                    )
            elif token.token_type == TokenType.LBRACKET:
                index = self._parse_expression()
                self._consume(TokenType.RBRACKET, "Expect ']' after index expression.")
                metadata = self.metadata_engine.infer_for_indexer(expr, index, self.parse_context)
                expr = Indexer(
                    collection=expr, index=index,
                    source_location=self._get_current_location(), metadata=metadata
                )
        return expr

    def _parse_primary(self) -> FHIRPathNode:
        if self._match(TokenType.STRING_LITERAL):
            value = self._previous().value
            metadata = self.metadata_engine.infer_for_literal(value, self.parse_context)
            return StringLiteral(value=value, source_location=self._get_current_location(), metadata=metadata)

        if self._match(TokenType.INTEGER_LITERAL, TokenType.DECIMAL_LITERAL):
            value = Decimal(self._previous().value)
            metadata = self.metadata_engine.infer_for_literal(value, self.parse_context)
            return NumberLiteral(value=value, source_location=self._get_current_location(), metadata=metadata)

        if self._match(TokenType.BOOLEAN_LITERAL):
            value = self._previous().value == 'true'
            metadata = self.metadata_engine.infer_for_literal(value, self.parse_context)
            return BooleanLiteral(value=value, source_location=self._get_current_location(), metadata=metadata)

        if self._match(TokenType.DATETIME_LITERAL):
            token = self._previous()
            dt_parser = DateTimeParser()
            dt_literal = dt_parser.parse_datetime(token.value)
            metadata = self.metadata_engine.infer_for_literal(dt_literal, self.parse_context)
            return DateTimeLiteral(
                value=dt_literal.value,
                precision=dt_literal.precision,
                timezone=dt_literal.timezone,
                source_location=SourceLocation(token.location.line, token.location.column),
                metadata=metadata
            )

        if self._match(TokenType.TIME_LITERAL):
            token = self._previous()
            dt_parser = DateTimeParser()
            time_literal = dt_parser.parse_time(token.value)
            metadata = self.metadata_engine.infer_for_literal(time_literal, self.parse_context)
            return TimeLiteral(
                value=time_literal.value,
                precision=time_literal.precision,
                source_location=SourceLocation(token.location.line, token.location.column),
                metadata=metadata
            )

        if self._match(TokenType.QUANTITY_LITERAL):
            token = self._previous()
            value_str = token.value['value']
            unit_str = token.value['unit']
            metadata = self.metadata_engine.infer_for_literal(Decimal(value_str), self.parse_context)
            return QuantityLiteral(
                value=Decimal(value_str),
                unit=unit_str,
                source_location=SourceLocation(token.location.line, token.location.column),
                metadata=metadata
            )

        if self._match(TokenType.LBRACE):
            start_token = self._previous()
            elements = []
            if not self._check(TokenType.RBRACE):
                elements.append(self._parse_expression())
                while self._match(TokenType.COMMA):
                    elements.append(self._parse_expression())
            self._consume(TokenType.RBRACE, "Expect '}' after collection elements.")
            metadata = self.metadata_engine.infer_for_collection(elements, self.parse_context)
            return CollectionLiteral(
                elements=elements,
                source_location=SourceLocation(start_token.location.line, start_token.location.column),
                metadata=metadata
            )

        if self._match(TokenType.IDENTIFIER):
            value = self._previous().value
            metadata = self.metadata_engine.infer_for_identifier(value, self.parse_context)
            return Identifier(value=value, source_location=self._get_current_location(), metadata=metadata)

        if self._match(TokenType.LPAREN):
            expr = self._parse_expression()
            self._consume(TokenType.RPAREN, "Expect ')' after expression.")
            return expr

        self._raise_parse_error("Expression expected.", "Did you forget an identifier or a literal value?")