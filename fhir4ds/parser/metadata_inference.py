from typing import Optional, Set, Dict, Any, List, Union
from fhir4ds.ast.nodes import (
    FHIRPathNode, Operator, UnaryOperator, Identifier
)
from fhir4ds.ast.metadata import Cardinality, PopulationMetadata

class ParseContext:
    """
    Holds contextual information during parsing, such as the current resource
    type, to aid in metadata inference.
    """
    def __init__(self):
        self.resource_type: Optional[str] = None
        self.current_path: List[str] = []
        self.function_context: Optional[str] = None
        # In a real implementation, this would be loaded with FHIR schema info.
        self.fhir_schema: Optional[Dict[str, Any]] = None

class MetadataInferenceEngine:
    """
    An engine for intelligently inferring metadata for AST nodes.
    This version infers metadata from node components *before* the node is
    created to avoid circular dependencies.
    """

    def infer_for_literal(self, value: Any, context: ParseContext) -> PopulationMetadata:
        """Infers metadata for a literal value."""
        # Determine FHIR type based on Python type
        fhir_type = "System.String"
        if isinstance(value, bool):
            fhir_type = "System.Boolean"
        elif isinstance(value, (int, float, Decimal)):
            fhir_type = "System.Decimal"

        return PopulationMetadata(
            cardinality=Cardinality.SINGLE,
            fhir_type=fhir_type,
            complexity_score=1,
            dependencies=set()
        )

    def infer_for_identifier(self, value: str, context: ParseContext) -> PopulationMetadata:
        """Infers metadata for an identifier."""
        return PopulationMetadata(
            cardinality=Cardinality.COLLECTION,  # Simplification
            fhir_type="Any",  # Simplification
            complexity_score=1,
            dependencies={value}
        )

    def infer_for_binary_operation(
        self, left: FHIRPathNode, op: Operator, right: FHIRPathNode, context: ParseContext
    ) -> PopulationMetadata:
        """Infers metadata for a binary operation from its operands."""
        # Most binary operations result in a single value.
        cardinality = Cardinality.SINGLE

        # Determine return type based on operator
        if op in [Operator.EQ, Operator.NE, Operator.GT, Operator.GTE, Operator.LT, Operator.LTE, Operator.AND, Operator.OR, Operator.XOR, Operator.IMPLIES, Operator.IS]:
            fhir_type = "System.Boolean"
        else:
            fhir_type = "System.Decimal" # Simplification for math ops

        complexity = (left.metadata.complexity_score if left.metadata else 1) + \
                     (right.metadata.complexity_score if right.metadata else 1) + 1
        dependencies = (left.metadata.dependencies if left.metadata else set()) | \
                       (right.metadata.dependencies if right.metadata else set())

        return PopulationMetadata(
            cardinality=cardinality,
            fhir_type=fhir_type,
            complexity_score=complexity,
            dependencies=dependencies
        )

    def infer_for_unary_operation(
        self, op: UnaryOperator, operand: FHIRPathNode, context: ParseContext
    ) -> PopulationMetadata:
        """Infers metadata for a unary operation."""
        complexity = (operand.metadata.complexity_score if operand.metadata else 1) + 1
        dependencies = operand.metadata.dependencies if operand.metadata else set()

        return PopulationMetadata(
            cardinality=operand.metadata.cardinality if operand.metadata else Cardinality.SINGLE,
            fhir_type=operand.metadata.fhir_type if operand.metadata else "System.Decimal",
            complexity_score=complexity,
            dependencies=dependencies
        )

    def infer_for_member_access(self, expression: FHIRPathNode, member: Identifier, context: ParseContext) -> PopulationMetadata:
        """Infers metadata for a member access operation."""
        complexity = (expression.metadata.complexity_score if expression.metadata else 1) + \
                     (member.metadata.complexity_score if member.metadata else 1)
        dependencies = (expression.metadata.dependencies if expression.metadata else set()) | \
                       (member.metadata.dependencies if member.metadata else set())

        return PopulationMetadata(
            cardinality=Cardinality.COLLECTION,  # Simplification
            fhir_type="Any",  # Simplification
            complexity_score=complexity,
            dependencies=dependencies
        )

    def infer_for_invocation(self, expression: FHIRPathNode, function_name: str, arguments: List[FHIRPathNode], context: ParseContext) -> PopulationMetadata:
        """Infers metadata for a function invocation."""
        cardinality = Cardinality.SINGLE
        if function_name in ['where', 'select', 'repeat']:
             cardinality = Cardinality.COLLECTION

        complexity = (expression.metadata.complexity_score if expression.metadata else 1) + 1
        dependencies = expression.metadata.dependencies if expression.metadata else set()
        for arg in arguments:
            complexity += arg.metadata.complexity_score if arg.metadata else 1
            dependencies |= arg.metadata.dependencies if arg.metadata else set()

        return PopulationMetadata(
            cardinality=cardinality,
            fhir_type="Any",  # Simplification
            complexity_score=complexity,
            dependencies=dependencies
        )

    def infer_for_indexer(self, collection: FHIRPathNode, index: FHIRPathNode, context: ParseContext) -> PopulationMetadata:
        """Infers metadata for an indexer operation."""
        complexity = (collection.metadata.complexity_score if collection.metadata else 1) + \
                     (index.metadata.complexity_score if index.metadata else 1)
        dependencies = (collection.metadata.dependencies if collection.metadata else set()) | \
                       (index.metadata.dependencies if index.metadata else set())

        return PopulationMetadata(
            cardinality=Cardinality.SINGLE,
            fhir_type=collection.metadata.fhir_type if collection.metadata else "Any",
            complexity_score=complexity,
            dependencies=dependencies
        )

    def infer_for_collection(self, elements: List[FHIRPathNode], context: ParseContext) -> PopulationMetadata:
        """Infers metadata for a collection literal."""
        complexity = 1
        dependencies = set()
        for element in elements:
            complexity += element.metadata.complexity_score if element.metadata else 1
            dependencies |= element.metadata.dependencies if element.metadata else set()

        return PopulationMetadata(
            cardinality=Cardinality.COLLECTION,
            fhir_type="Collection", # Simplification
            complexity_score=complexity,
            dependencies=dependencies
        )