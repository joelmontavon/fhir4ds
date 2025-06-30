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