from fhir4ds.parser.lexer import Token, TokenType, Lexer
from fhir4ds.ast.nodes import (
    FHIRPathNode,
    Identifier,
    StringLiteral,
    NumberLiteral,
    BooleanLiteral,
    DateLiteral,
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
        return self._peek().type == TokenType.EOF

    def _advance(self) -> Token:
        if not self._is_at_end():
            self.pos += 1
        return self._previous()

    def _check(self, token_type: TokenType) -> bool:
        if self._is_at_end():
            return False
        return self._peek().type == token_type

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
        return SourceLocation(line=token.line, column=token.column)

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
        TokenType.STAR: Operator.MUL,
        TokenType.SLASH: Operator.DIV,
        TokenType.MOD: Operator.MOD,
        TokenType.EQ: Operator.EQ,
        TokenType.NE: Operator.NE,
        TokenType.GT: Operator.GT,
        TokenType.GTE: Operator.GTE,
        TokenType.LT: Operator.LT,
        TokenType.LTE: Operator.LTE,
        TokenType.AND: Operator.AND,
        TokenType.OR: Operator.OR,
        TokenType.XOR: Operator.XOR,
        TokenType.IMPLIES: Operator.IMPLIES,
        TokenType.IS: Operator.IS,
        TokenType.AS: Operator.AS,
    }

    def _token_to_operator(self, token: Token) -> Operator:
        operator = self.OPERATOR_MAP.get(token.type)
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
        return self._binary_op_parser(self._parse_comparison, TokenType.EQ, TokenType.NE)

    def _parse_comparison(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_type_ops, TokenType.GT, TokenType.GTE, TokenType.LT, TokenType.LTE)

    def _parse_type_ops(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_term, TokenType.IS, TokenType.AS)

    def _parse_term(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_factor, TokenType.PLUS, TokenType.MINUS)

    def _parse_factor(self) -> FHIRPathNode:
        return self._binary_op_parser(self._parse_unary, TokenType.STAR, TokenType.SLASH, TokenType.MOD)

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
            if token.type == TokenType.DOT:
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
                            source_location=SourceLocation(member.line, member.column),
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
                            source_location=SourceLocation(member.line, member.column),
                            metadata=self._create_mock_metadata(),
                        ),
                        source_location=self._get_source_location(),
                        metadata=self._create_mock_metadata(),
                    )
            elif token.type == TokenType.LBRACKET:
                index = self._parse_expression()
                self._consume(TokenType.RBRACKET, "Expect ']' after index.")
                expr = Indexer(
                    collection=expr,
                    index=index,
                    source_location=self._get_source_location(),
                    metadata=self._create_mock_metadata(),
                )
        return expr

    def _parse_primary(self) -> FHIRPathNode:
        if self._match(TokenType.STRING_LITERAL):
            return StringLiteral(
                value=self._previous().value,
                source_location=self._get_source_location(),
                metadata=self._create_mock_metadata(),
            )
        if self._match(TokenType.NUMBER_LITERAL):
            return NumberLiteral(
                value=float(self._previous().value),
                source_location=self._get_source_location(),
                metadata=self._create_mock_metadata(),
            )
        if self._match(TokenType.TRUE):
            return BooleanLiteral(value=True, source_location=self._get_source_location(), metadata=self._create_mock_metadata())
        if self._match(TokenType.FALSE):
            return BooleanLiteral(value=False, source_location=self._get_source_location(), metadata=self._create_mock_metadata())
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