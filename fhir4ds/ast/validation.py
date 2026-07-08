from fhir4ds.ast.visitors import ASTVisitor
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
)


class SemanticValidator(ASTVisitor[None]):
    """
    An AST visitor that performs semantic validation on the AST.
    """

    def __init__(self):
        self.errors = []

    def _visit_children(self, node: FHIRPathNode):
        for child in node.children:
            self.visit(child)

    def visit_identifier(self, node: Identifier) -> None:
        self._visit_children(node)

    def visit_string_literal(self, node: StringLiteral) -> None:
        self._visit_children(node)

    def visit_number_literal(self, node: NumberLiteral) -> None:
        self._visit_children(node)

    def visit_boolean_literal(self, node: BooleanLiteral) -> None:
        self._visit_children(node)

    def visit_date_literal(self, node: DateLiteral) -> None:
        self._visit_children(node)

    def visit_time_literal(self, node: TimeLiteral) -> None:
        self._visit_children(node)

    def visit_datetime_literal(self, node: DateTimeLiteral) -> None:
        self._visit_children(node)

    def visit_quantity_literal(self, node: QuantityLiteral) -> None:
        self._visit_children(node)

    def visit_collection_literal(self, node: CollectionLiteral) -> None:
        self._visit_children(node)

    def visit_binary_operation(self, node: BinaryOperation) -> None:
        if (
            node.operator == Operator.DIV
            and isinstance(node.right, NumberLiteral)
            and node.right.value == 0
        ):
            self.errors.append("Division by zero is not allowed.")
        self._visit_children(node)

    def visit_unary_operation(self, node: UnaryOperation) -> None:
        self._visit_children(node)

    def visit_function_call(self, node: FunctionCall) -> None:
        self._visit_children(node)

    def visit_invocation_expression(self, node: InvocationExpression) -> None:
        self._visit_children(node)

    def visit_member_access(self, node: MemberAccess) -> None:
        self._visit_children(node)

    def visit_indexer(self, node: Indexer) -> None:
        self._visit_children(node)