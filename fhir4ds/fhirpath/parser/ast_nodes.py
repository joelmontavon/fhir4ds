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