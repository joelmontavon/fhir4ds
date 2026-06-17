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
from fhir4ds.parser.functions.registry import FHIRPathFunctionRegistry
from fhir4ds.parser.functions.core.first import FirstFunction
from fhir4ds.parser.functions.core.last import LastFunction
from fhir4ds.parser.functions.core.tail import TailFunction
from fhir4ds.parser.functions.core.exists import ExistsFunction
from fhir4ds.parser.functions.core.empty import EmptyFunction
from fhir4ds.parser.functions.core.not_function import NotFunction
from fhir4ds.parser.functions.core.count import CountFunction
from fhir4ds.parser.functions.core.where import WhereFunction
from fhir4ds.parser.functions.core.select import SelectFunction
from fhir4ds.parser.functions.core.sum import SumFunction
from fhir4ds.parser.functions.core.avg import AvgFunction


class Parser:
    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0
        self.function_registry = FHIRPathFunctionRegistry()
        self._register_core_functions()

    def _register_core_functions(self):
        self.function_registry.register_function(FirstFunction())
        self.function_registry.register_function(LastFunction())
        self.function_registry.register_function(TailFunction())
        self.function_registry.register_function(ExistsFunction())
        self.function_registry.register_function(EmptyFunction())
        self.function_registry.register_function(NotFunction())
        self.function_registry.register_function(CountFunction())
        self.function_registry.register_function(WhereFunction())
        self.function_registry.register_function(SelectFunction())
        self.function_registry.register_function(SumFunction())
        self.function_registry.register_function(AvgFunction())

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
                member_token = self._peek()
                if member_token.token_type == TokenType.EOF:
                    raise Exception("Unexpected end of expression after '.'")

                self._advance() # Consume the member/function name

                if self._match(TokenType.LPAREN):
                    # It's a function call, use the new registry
                    function_name = member_token.value
                    function_impl = self.function_registry.get_function(function_name)

                    if not function_impl:
                        raise Exception(f"Unknown function: {function_name}")

                    # Parse arguments
                    arguments = []
                    if not self._check(TokenType.RPAREN):
                        arguments.append(self._parse_expression())
                        while self._match(TokenType.COMMA):
                            arguments.append(self._parse_expression())
                    self._consume(TokenType.RPAREN, "Expect ')' after arguments.")

                    # Validate arguments
                    validation_errors = function_impl.validate_arguments(arguments)
                    if validation_errors:
                        # For now, just raise the first error
                        raise Exception(f"Invalid arguments for {function_name}: {validation_errors[0].message}")

                    # Create AST node using the function's own logic
                    expr = function_impl.create_ast_node(expr, arguments)
                else:
                    # Member access. Keywords are not allowed here.
                    if member_token.token_type != TokenType.IDENTIFIER:
                        raise Exception(f"Cannot access property '{member_token.value}' because it is a reserved keyword.")

                    expr = MemberAccess(
                        expression=expr,
                        member=Identifier(
                            value=member_token.value,
                            source_location=SourceLocation(member_token.location.line, member_token.location.column),
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