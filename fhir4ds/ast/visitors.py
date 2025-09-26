from abc import ABC, abstractmethod
from typing import Generic, TypeVar

# Forward reference for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from . import nodes

T = TypeVar("T")


class ASTVisitor(Generic[T], ABC):
    """
    Base class for visiting nodes in the FHIRPath AST.
    This uses the Visitor design pattern to allow for different operations
    to be performed on the AST without changing the node classes.
    """

    def visit(self, node: "nodes.FHIRPathNode") -> T:
        """Public entry point for visiting any node."""
        return node.accept(self)

    @abstractmethod
    def visit_identifier(self, node: "nodes.Identifier") -> T:
        pass

    @abstractmethod
    def visit_string_literal(self, node: "nodes.StringLiteral") -> T:
        pass

    @abstractmethod
    def visit_number_literal(self, node: "nodes.NumberLiteral") -> T:
        pass

    @abstractmethod
    def visit_boolean_literal(self, node: "nodes.BooleanLiteral") -> T:
        pass

    @abstractmethod
    def visit_date_literal(self, node: "nodes.DateLiteral") -> T:
        pass

    @abstractmethod
    def visit_datetime_literal(self, node: "nodes.DateTimeLiteral") -> T:
        pass

    @abstractmethod
    def visit_time_literal(self, node: "nodes.TimeLiteral") -> T:
        pass

    @abstractmethod
    def visit_quantity_literal(self, node: "nodes.QuantityLiteral") -> T:
        pass

    @abstractmethod
    def visit_collection_literal(self, node: "nodes.CollectionLiteral") -> T:
        pass

    @abstractmethod
    def visit_binary_operation(self, node: "nodes.BinaryOperation") -> T:
        pass

    @abstractmethod
    def visit_unary_operation(self, node: "nodes.UnaryOperation") -> T:
        pass

    @abstractmethod
    def visit_function_call(self, node: "nodes.FunctionCall") -> T:
        pass

    @abstractmethod
    def visit_path_expression(self, node: "nodes.PathExpression") -> T:
        pass