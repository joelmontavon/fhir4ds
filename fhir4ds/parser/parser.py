from decimal import Decimal
from fhir4ds.parser.lexer import Token, TokenType, Lexer
from fhir4ds.parser.literals import DateTimeParser
from fhir4ds.ast.nodes import (
    FHIRPathNode,
    Identifier,
    StringLiteral,
    NumberLiteral,
    BooleanLiteral,
    TimeLiteral,
    DateTimeLiteral,
    QuantityLiteral,
    CollectionLiteral,
    BinaryOperation,
    UnaryOperation,
    FunctionCall,
    InvocationExpression,
    MemberAccess,
    Indexer,
    Operator,
    UnaryOperator,
    SourceLocation,
)
from fhir4ds.ast.metadata import Cardinality, PopulationMetadata

class Parser:
    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0

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
        raise Exception(message) # TODO: better error handling

    def _get_source_location(self) -> SourceLocation:
        token = self._peek()
        return SourceLocation(line=token.location.line, column=token.location.column)

    def _create_mock_metadata(self) -> PopulationMetadata:
        # In a real scenario, this would involve more complex logic.
        return PopulationMetadata(
            cardinality=Cardinality.COLLECTION,
            fhir_type="Any"
        )

    def parse(self) -> FHIRPathNode:
        return self._parse_expression()

    OPERATOR_MAP = {
        TokenType.PLUS: Operator.ADD,
        TokenType.MINUS: Operator.SUB,
        TokenType.MULTIPLY: Operator.MUL,
        TokenType.DIVIDE: Operator.DIV,
        TokenType.MOD: Operator.MOD,
        TokenType.EQUAL: Operator.EQ,
        TokenType.NOT_EQUAL: Operator.NE,
        TokenType.GREATER_THAN: Operator.GT,
        TokenType.GREATER_EQUAL: Operator.GTE,
        TokenType.LESS_THAN: Operator.LT,
        TokenType.LESS_EQUAL: Operator.LTE,
        TokenType.AND: Operator.AND,
        TokenType.OR: Operator.OR,
        TokenType.XOR: Operator.XOR,
        TokenType.IMPLIES: Operator.IMPLIES,
        TokenType.IS: Operator.IS,
        TokenType.AS: Operator.AS,
    }

    def _token_to_operator(self, token: Token) -> Operator:
        operator = self.OPERATOR_MAP.get(token.token_type)
        if operator is None:
            raise Exception(f"Unknown operator token: {token}")
        return operator

    def _binary_op_parser(self, higher_precedence_parser, *token_types: TokenType) -> FHIRPathNode:
        expr = higher_precedence_parser()
        while self._match(*token_types):
            op_token = self._previous()
            operator = self._token_to_operator(op_token)
            right = higher_precedence_parser()
            expr = BinaryOperation(
                left=expr,
                operator=operator,
                right=right,
                source_location=self._get_source_location(),
                metadata=self._create_mock_metadata(),
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
            operator = UnaryOperator.MINUS
            operand = self._parse_unary()
            return UnaryOperation(
                operator=operator,
                operand=operand,
                source_location=self._get_source_location(),
                metadata=self._create_mock_metadata(),
            )
        return self._parse_path_expression()

    def _parse_path_expression(self) -> FHIRPathNode:
        expr = self._parse_primary()
        while self._match(TokenType.DOT, TokenType.LBRACKET):
            token = self._previous()
            if token.token_type == TokenType.DOT:
                member = self._consume(TokenType.IDENTIFIER, "Expect identifier after '.'.")

                if self._match(TokenType.LPAREN):
                    # Invocation expression
                    arguments = []
                    if not self._check(TokenType.RPAREN):
                        arguments.append(self._parse_expression())
                        while self._match(TokenType.COMMA):
                            arguments.append(self._parse_expression())
                    self._consume(TokenType.RPAREN, "Expect ')' after arguments.")
                    expr = InvocationExpression(
                        expression=expr,
                        name=Identifier(
                            value=member.value,
                            source_location=SourceLocation(member.location.line, member.location.column),
                            metadata=self._create_mock_metadata(),
                        ),
                        arguments=arguments,
                        source_location=self._get_source_location(),
                        metadata=self._create_mock_metadata(),
                    )
                else:
                    # Member access
                    expr = MemberAccess(
                        expression=expr,
                        member=Identifier(
                            value=member.value,
                            source_location=SourceLocation(member.location.line, member.location.column),
                            metadata=self._create_mock_metadata(),
                        ),
                        source_location=self._get_source_location(),
                        metadata=self._create_mock_metadata(),
                    )
            elif token.token_type == TokenType.LBRACKET:
                index = self._parse_expression()
                self._consume(TokenType.RBRACKET, "Expect ']' after index.")
                expr = Indexer(
                    collection=expr,
                    index=index,
                    source_location=self._get_source_location(),
                    metadata=self._create_mock_metadata(),
                )
        return expr

    def _infer_collection_metadata(self, elements: list[FHIRPathNode]) -> PopulationMetadata:
        """Infer metadata for collection based on elements"""
        return PopulationMetadata(
            cardinality=Cardinality.COLLECTION,
            fhir_type="Collection",
            complexity_score=len(elements),
            dependencies=set()
        )

    def _parse_collection_literal(self) -> CollectionLiteral:
        """Parse collection literal {element1, element2, ...}"""
        start_token = self._previous() # This is the LBRACE token
        elements = []

        if not self._check(TokenType.RBRACE):
            elements.append(self._parse_expression())
            while self._match(TokenType.COMMA):
                elements.append(self._parse_expression())

        self._consume(TokenType.RBRACE, "Expect '}' after collection elements.")

        return CollectionLiteral(
            elements=elements,
            source_location=SourceLocation(start_token.location.line, start_token.location.column),
            metadata=self._infer_collection_metadata(elements)
        )

    def _parse_quantity_literal(self) -> QuantityLiteral:
        token = self._previous()
        # The lexer pre-parses the quantity into a dict
        value_str = token.value['value']
        unit_str = token.value['unit']
        return QuantityLiteral(
            value=Decimal(value_str),
            unit=unit_str,
            source_location=SourceLocation(token.location.line, token.location.column),
            metadata=self._create_mock_metadata()
        )

    def _parse_datetime_literal(self) -> DateTimeLiteral:
        """Parse datetime literal token into AST node"""
        token = self._previous()
        parser = DateTimeParser()
        # We need to manually set source location and metadata here
        dt_literal = parser.parse_datetime(token.value)
        return DateTimeLiteral(
            value=dt_literal.value,
            precision=dt_literal.precision,
            timezone=dt_literal.timezone,
            source_location=SourceLocation(token.location.line, token.location.column),
            metadata=self._create_mock_metadata()
        )

    def _parse_time_literal(self) -> TimeLiteral:
        """Parse time literal token into AST node"""
        token = self._previous()
        parser = DateTimeParser()
        # We need to manually set source location and metadata here
        time_literal = parser.parse_time(token.value)
        return TimeLiteral(
            value=time_literal.value,
            precision=time_literal.precision,
            source_location=SourceLocation(token.location.line, token.location.column),
            metadata=self._create_mock_metadata()
        )

    def _parse_primary(self) -> FHIRPathNode:
        if self._match(TokenType.STRING_LITERAL):
            return StringLiteral(
                value=self._previous().value,
                source_location=self._get_source_location(),
                metadata=self._create_mock_metadata(),
            )
        if self._match(TokenType.INTEGER_LITERAL, TokenType.DECIMAL_LITERAL):
            return NumberLiteral(
                value=float(self._previous().value),
                source_location=self._get_source_location(),
                metadata=self._create_mock_metadata(),
            )
        if self._match(TokenType.BOOLEAN_LITERAL):
            token_value = self._previous().value
            return BooleanLiteral(value=(token_value == 'true'), source_location=self._get_source_location(), metadata=self._create_mock_metadata())

        if self._match(TokenType.DATETIME_LITERAL):
            return self._parse_datetime_literal()

        if self._match(TokenType.TIME_LITERAL):
            return self._parse_time_literal()

        if self._match(TokenType.QUANTITY_LITERAL):
            return self._parse_quantity_literal()

        if self._match(TokenType.LBRACE):
            return self._parse_collection_literal()

        if self._match(TokenType.IDENTIFIER):
            return Identifier(
                value=self._previous().value,
                source_location=self._get_source_location(),
                metadata=self._create_mock_metadata(),
            )
        if self._match(TokenType.LPAREN):
            expr = self._parse_expression()
            self._consume(TokenType.RPAREN, "Expect ')' after expression.")
            return expr

        raise Exception("Expect expression.") # TODO: better error handling