from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from fhir4ds.ast.visitors import ASTVisitor

T = TypeVar("T")

from fhir4ds.ast.metadata import PopulationMetadata

@dataclass(frozen=True)
class SourceLocation:
    """
    Represents the location of a node in the original source code.
    """
    line: int
    column: int

@dataclass(frozen=True)
class FHIRPathNode(ABC):
    """
    Abstract base class for all nodes in the FHIRPath Abstract Syntax Tree (AST).
    """
    source_location: SourceLocation
    metadata: PopulationMetadata

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        ...

    @abstractmethod
    def __hash__(self) -> int:
        ...

    @property
    def children(self) -> List['FHIRPathNode']:
        return []

    @abstractmethod
    def accept(self, visitor: "ASTVisitor[T]") -> T:
        ...


# Identifier Node
@dataclass(frozen=True)
class Identifier(FHIRPathNode):
    """
    Represents an identifier in a FHIRPath expression, such as a property or function name.
    """
    value: str

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Identifier) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_identifier(self)


# Literal Nodes
@dataclass(frozen=True)
class Literal(FHIRPathNode, ABC):
    """
    Abstract base class for all literal value nodes in the AST.
    """
    pass


@dataclass(frozen=True)
class StringLiteral(Literal):
    """
    Represents a string literal value.
    """
    value: str

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, StringLiteral) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_string_literal(self)


@dataclass(frozen=True)
class NumberLiteral(Literal):
    """
    Represents a numeric literal value.
    """
    value: float

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, NumberLiteral) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_number_literal(self)


@dataclass(frozen=True)
class BooleanLiteral(Literal):
    """
    Represents a boolean literal value.
    """
    value: bool

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, BooleanLiteral) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_boolean_literal(self)


@dataclass(frozen=True)
class DateLiteral(Literal):
    """
    Represents a date literal value.
    """
    value: str  # Using string to preserve lexical representation, e.g., @2025-01-01

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, DateLiteral) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_date_literal(self)


@dataclass(frozen=True)
class TimeLiteral(Literal):
    """
    Represents a time literal value.
    """
    value: str  # Using string to preserve lexical representation, e.g., @T12:30:00

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, TimeLiteral) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_time_literal(self)


@dataclass(frozen=True)
class DateTimeLiteral(Literal):
    """
    Represents a dateTime literal value.
    """
    value: str  # Using string to preserve lexical representation, e.g., @2025-01-01T12:30:00Z

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, DateTimeLiteral) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_datetime_literal(self)


@dataclass(frozen=True)
class QuantityLiteral(Literal):
    """
    Represents a quantity literal with a value and a unit.
    """
    value: float
    unit: str

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, QuantityLiteral) and self.value == other.value and self.unit == other.unit

    def __hash__(self) -> int:
        return hash((self.value, self.unit))

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_quantity_literal(self)


@dataclass(frozen=True)
class CollectionLiteral(Literal):
    """
    Represents a collection literal, such as an array or set.
    """
    elements: List[FHIRPathNode]

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, CollectionLiteral) and self.elements == other.elements

    def __hash__(self) -> int:
        return hash(tuple(self.elements))

    @property
    def children(self) -> List[FHIRPathNode]:
        return self.elements

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_collection_literal(self)


# Expression Nodes
@dataclass(frozen=True)
class Expression(FHIRPathNode, ABC):
    """
    Abstract base class for all expression nodes in the AST.
    """
    pass


class Operator(Enum):
    # Mathematical
    ADD = "+"
    SUB = "-"
    MUL = "*"
    DIV = "/"
    MOD = "mod"
    # Logical
    AND = "and"
    OR = "or"
    XOR = "xor"
    IMPLIES = "implies"
    # Comparison
    EQ = "="
    NE = "!="
    GT = ">"
    LT = "<"
    GTE = ">="
    LTE = "<="
    # Type
    IS = "is"
    AS = "as"


@dataclass(frozen=True)
class BinaryOperation(Expression):
    """
    Represents a binary operation with a left operand, an operator, and a right operand.
    """
    left: FHIRPathNode
    operator: Operator
    right: FHIRPathNode

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, BinaryOperation)
            and self.left == other.left
            and self.operator == other.operator
            and self.right == other.right
        )

    def __hash__(self) -> int:
        return hash((self.left, self.operator, self.right))

    @property
    def children(self) -> List[FHIRPathNode]:
        return [self.left, self.right]

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_binary_operation(self)


class UnaryOperator(Enum):
    PLUS = "+"
    MINUS = "-"
    NOT = "not"


@dataclass(frozen=True)
class UnaryOperation(Expression):
    """
    Represents a unary operation with an operator and an operand.
    """
    operator: UnaryOperator
    operand: FHIRPathNode

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, UnaryOperation)
            and self.operator == other.operator
            and self.operand == other.operand
        )

    def __hash__(self) -> int:
        return hash((self.operator, self.operand))

    @property
    def children(self) -> List[FHIRPathNode]:
        return [self.operand]

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_unary_operation(self)


@dataclass(frozen=True)
class FunctionCall(Expression):
    """
    Represents a function call with a function name and a list of arguments.
    """
    name: Identifier
    arguments: List[FHIRPathNode]

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FunctionCall)
            and self.name == other.name
            and self.arguments == other.arguments
        )

    def __hash__(self) -> int:
        return hash((self.name, tuple(self.arguments)))

    @property
    def children(self) -> List[FHIRPathNode]:
        return [self.name] + self.arguments

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_function_call(self)


@dataclass(frozen=True)
class InvocationExpression(Expression):
    """
    Represents a function call on an expression, e.g., `Patient.children()`.
    """
    expression: FHIRPathNode
    name: Identifier
    arguments: List[FHIRPathNode]

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, InvocationExpression)
            and self.expression == other.expression
            and self.name == other.name
            and self.arguments == other.arguments
        )

    def __hash__(self) -> int:
        return hash((self.expression, self.name, tuple(self.arguments)))

    @property
    def children(self) -> List[FHIRPathNode]:
        return [self.expression, self.name] + self.arguments

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_invocation_expression(self)


@dataclass(frozen=True)
class MemberAccess(Expression):
    """
    Represents a member access on an expression, e.g., `Patient.name`.
    """
    expression: FHIRPathNode
    member: Identifier

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, MemberAccess)
            and self.expression == other.expression
            and self.member == other.member
        )

    def __hash__(self) -> int:
        return hash((self.expression, self.member))

    @property
    def children(self) -> List[FHIRPathNode]:
        return [self.expression, self.member]

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_member_access(self)


@dataclass(frozen=True)
class Indexer(Expression):
    """
    Represents an indexer operation on a collection, e.g., `Patient.name[0]`.
    """
    collection: FHIRPathNode
    index: FHIRPathNode

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Indexer)
            and self.collection == other.collection
            and self.index == other.index
        )

    def __hash__(self) -> int:
        return hash((self.collection, self.index))

    @property
    def children(self) -> List[FHIRPathNode]:
        return [self.collection, self.index]

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_indexer(self)