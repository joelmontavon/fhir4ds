from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set

class Cardinality(Enum):
    SINGLE = "SINGLE"
    COLLECTION = "COLLECTION"
    OPTIONAL = "OPTIONAL"

@dataclass(frozen=True)
class ResourceImpact:
    """
    Represents the estimated impact of an AST node on system resources.
    This is a placeholder for a more sophisticated resource modeling system.
    """
    memory_impact: int = 0
    performance_impact: int = 0

@dataclass(frozen=True)
class PopulationMetadata:
    """
    Contains metadata for each AST node to support population-scale optimization.
    """
    cardinality: Cardinality = Cardinality.OPTIONAL
    fhir_type: Optional[str] = None
    complexity_score: int = 1
    dependencies: Set[str] = field(default_factory=set)
    resource_impact: ResourceImpact = field(default_factory=ResourceImpact)