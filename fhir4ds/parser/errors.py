from dataclasses import dataclass
# This import is problematic due to the circular dependency.
# We need to resolve it by moving the import statement.
# from fhir4ds.ast.nodes import FHIRPathNode

# Forward declaration to break the cycle
class FHIRPathNode:
    pass

@dataclass
class ValidationError:
    """
    Semantic validation error.
    """
    message: str
    node: "FHIRPathNode"
    error_type: str