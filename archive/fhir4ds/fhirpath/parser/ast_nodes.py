"""
FHIRPath AST Node Definitions

This module contains the Abstract Syntax Tree (AST) node classes
used to represent parsed FHIRPath expressions.
"""

from dataclasses import dataclass
from typing import Any, List


@dataclass
class ASTNode:
    """Base class for AST nodes"""
    pass


@dataclass
class ThisNode(ASTNode):
    """Represents the context item '$this', typically the root of the current resource."""
    pass


@dataclass
class VariableNode(ASTNode):
    """Represents context variables like '$index', '$total', etc."""
    name: str  # 'index', 'total', etc. (without the $ prefix)


@dataclass
class LiteralNode(ASTNode):
    """Literal value node"""
    value: Any
    type: str  # 'string', 'integer', 'decimal', 'boolean'


@dataclass
class IdentifierNode(ASTNode):
    """Identifier node"""
    name: str


@dataclass
class FunctionCallNode(ASTNode):
    """Function call node"""
    name: str
    args: List[ASTNode]


@dataclass
class BinaryOpNode(ASTNode):
    """Binary operation node"""
    left: ASTNode
    operator: str
    right: ASTNode


@dataclass
class UnaryOpNode(ASTNode):
    """Unary operation node"""
    operator: str
    operand: ASTNode


@dataclass
class PathNode(ASTNode):
    """Path expression node"""
    segments: List[ASTNode]


@dataclass
class IndexerNode(ASTNode):
    """Indexer expression node"""
    expression: ASTNode
    index: ASTNode


@dataclass
class TupleNode(ASTNode):
    """Tuple literal node {key: value, ...}"""
    elements: List[tuple]  # List of (key, value) pairs where key is string or ASTNode and value is ASTNode


@dataclass
class IntervalConstructorNode(ASTNode):
    """Interval constructor node Interval[start, end]"""
    start: ASTNode
    end: ASTNode


@dataclass
class ListLiteralNode(ASTNode):
    """List literal node {item1, item2, ...}"""
    elements: List[ASTNode]


@dataclass
class CQLQueryExpressionNode(ASTNode):
    """Represents a CQL query expression with optional alias, where, sort clauses"""
    source: ASTNode  # The source expression (e.g., "Smoking status observation")
    alias: str = None  # Optional alias (e.g., "O")
    where_clause: ASTNode = None  # Optional where condition
    sort_clause: ASTNode = None  # Optional sort expression
    sort_direction: str = "asc"  # "asc" or "desc"
    return_clause: ASTNode = None  # Optional return expression


@dataclass
class ResourceQueryNode(ASTNode):
    """Represents a CQL resource query like [Patient] or [Condition: 'Asthma']"""
    resource_type: str  # The FHIR resource type (e.g., "Patient", "Condition")
    code_filter: str = None  # Optional code/value set filter (e.g., "Asthma")
    code_path: str = None  # Optional code path (defaults to "code")