from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class PopulationMetadata:
    """
    Stores metadata about a node's context in a population-scale query.
    """
    # Example field: the SQL alias for the CTE representing this node
    sql_alias: str = ""
    # Other potential metadata:
    # - Inferred data type
    # - Cardinality estimate
    # - Dependencies on other nodes
    annotations: Dict[str, Any] = field(default_factory=dict)