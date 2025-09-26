from typing import List
from fhir4ds.ast.nodes import (
    FHIRPathNode,
    ValidationError,
    InvocationExpression,
    Identifier,
)
from fhir4ds.parser.functions.base import FHIRPathFunction


class EmptyFunction(FHIRPathFunction):
    """
    Implementation of the `empty()` FHIRPath function.
    """

    def name(self) -> str:
        return "empty"

    def validate_arguments(
        self, arguments: List[FHIRPathNode]
    ) -> List[ValidationError]:
        if len(arguments) != 0:
            return [
                ValidationError(
                    message="The 'empty' function does not take any arguments.",
                    node=arguments[0] if arguments else None,
                    error_type="ArgumentError",
                )
            ]
        return []

    def create_ast_node(
        self, expression: FHIRPathNode, arguments: List[FHIRPathNode],
    ) -> InvocationExpression:
        return InvocationExpression(
            expression=expression,
            name=Identifier(
                value=self.name(),
                source_location=expression.source_location,
                metadata=expression.metadata,
            ),
            arguments=arguments,
            source_location=expression.source_location,
            metadata=expression.metadata,
        )