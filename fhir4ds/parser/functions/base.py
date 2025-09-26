from abc import ABC, abstractmethod
from typing import List
from fhir4ds.ast.nodes import (
    FHIRPathNode,
    ValidationError,
    InvocationExpression,
    Identifier,
)


class FHIRPathFunction(ABC):
    """Base class for FHIRPath function implementations"""

    @abstractmethod
    def name(self) -> str:
        """Function name as it appears in FHIRPath expressions"""
        pass

    @abstractmethod
    def validate_arguments(self, arguments: List[FHIRPathNode]) -> List[ValidationError]:
        """Validate function arguments"""
        pass

    @abstractmethod
    def create_ast_node(
        self, expression: FHIRPathNode, arguments: List[FHIRPathNode]
    ) -> InvocationExpression:
        """Create AST node for this function call"""
        pass