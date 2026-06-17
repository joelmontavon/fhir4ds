from typing import TypeVar, Generic
from fhir4ds.ast.nodes import (
    FHIRPathNode,
    Identifier,
    Literal,
    StringLiteral,
    NumberLiteral,
    BooleanLiteral,
    DateLiteral,
    TimeLiteral,
    DateTimeLiteral,
    QuantityLiteral,
    CollectionLiteral,
    Expression,
    BinaryOperation,
    UnaryOperation,
    FunctionCall,
    PathExpression,
)

T = TypeVar("T")


class ASTVisitor(Generic[T]):
    """
    Base class for visitors that traverse the FHIRPath AST.
    """

    def visit(self, node: FHIRPathNode) -> T:
        """
        Dispatches to the appropriate visit method based on the node type.
        """
        return node.accept(self)

    def visit_identifier(self, node: Identifier) -> T:
        raise NotImplementedError

    def visit_string_literal(self, node: StringLiteral) -> T:
        raise NotImplementedError

    def visit_number_literal(self, node: NumberLiteral) -> T:
        raise NotImplementedError

    def visit_boolean_literal(self, node: BooleanLiteral) -> T:
        raise NotImplementedError

    def visit_date_literal(self, node: DateLiteral) -> T:
        raise NotImplementedError

    def visit_time_literal(self, node: TimeLiteral) -> T:
        raise NotImplementedError

    def visit_datetime_literal(self, node: DateTimeLiteral) -> T:
        raise NotImplementedError

    def visit_quantity_literal(self, node: QuantityLiteral) -> T:
        raise NotImplementedError

    def visit_collection_literal(self, node: CollectionLiteral) -> T:
        raise NotImplementedError

    def visit_binary_operation(self, node: BinaryOperation) -> T:
        raise NotImplementedError

    def visit_unary_operation(self, node: UnaryOperation) -> T:
        raise NotImplementedError

    def visit_function_call(self, node: FunctionCall) -> T:
        raise NotImplementedError

    def visit_path_expression(self, node: PathExpression) -> T:
        raise NotImplementedError


class ASTPrinter(ASTVisitor[str]):
    """
    An AST visitor that creates a string representation of the AST.
    """

    def __init__(self):
        self._indent = 0

    def _make_indent(self) -> str:
        return "  " * self._indent

    def visit_identifier(self, node: Identifier) -> str:
        return f"{self._make_indent()}Identifier(value={node.value})\n"

    def visit_string_literal(self, node: StringLiteral) -> str:
        return f"{self._make_indent()}StringLiteral(value='{node.value}')\n"

    def visit_number_literal(self, node: NumberLiteral) -> str:
        return f"{self._make_indent()}NumberLiteral(value={node.value})\n"

    def visit_boolean_literal(self, node: BooleanLiteral) -> str:
        return f"{self._make_indent()}BooleanLiteral(value={node.value})\n"

    def visit_date_literal(self, node: DateLiteral) -> str:
        return f"{self._make_indent()}DateLiteral(value={node.value})\n"

    def visit_time_literal(self, node: TimeLiteral) -> str:
        return f"{self._make_indent()}TimeLiteral(value={node.value})\n"

    def visit_datetime_literal(self, node: DateTimeLiteral) -> str:
        return f"{self._make_indent()}DateTimeLiteral(value={node.value})\n"

    def visit_quantity_literal(self, node: QuantityLiteral) -> str:
        return f"{self._make_indent()}QuantityLiteral(value={node.value}, unit='{node.unit}')\n"

    def visit_collection_literal(self, node: CollectionLiteral) -> str:
        s = f"{self._make_indent()}CollectionLiteral[\n"
        self._indent += 1
        for element in node.elements:
            s += self.visit(element)
        self._indent -= 1
        s += f"{self._make_indent()}]\n"
        return s

    def visit_binary_operation(self, node: BinaryOperation) -> str:
        s = f"{self._make_indent()}BinaryOperation(operator={node.operator.value})[\n"
        self._indent += 1
        s += f"{self._make_indent()}LEFT:\n"
        s += self.visit(node.left)
        s += f"{self._make_indent()}RIGHT:\n"
        s += self.visit(node.right)
        self._indent -= 1
        s += f"{self._make_indent()}]\n"
        return s

    def visit_unary_operation(self, node: UnaryOperation) -> str:
        s = f"{self._make_indent()}UnaryOperation(operator={node.operator.value})[\n"
        self._indent += 1
        s += self.visit(node.operand)
        self._indent -= 1
        s += f"{self._make_indent()}]\n"
        return s

    def visit_function_call(self, node: FunctionCall) -> str:
        s = f"{self._make_indent()}FunctionCall(name={node.name.value})[\n"
        self._indent += 1
        for arg in node.arguments:
            s += self.visit(arg)
        self._indent -= 1
        s += f"{self._make_indent()}]\n"
        return s

    def visit_path_expression(self, node: PathExpression) -> str:
        s = f"{self._make_indent()}PathExpression[\n"
        self._indent += 1
        for part in node.path:
            s += self.visit(part)
        self._indent -= 1
        s += f"{self._make_indent()}]\n"
        return s