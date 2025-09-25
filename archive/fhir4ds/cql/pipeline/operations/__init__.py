"""
CQL-specific pipeline operations.

These operations handle CQL constructs that don't have direct
FHIRPath equivalents, such as retrieve statements, define blocks,
and context switching.
"""

from .retrieve import CQLRetrieveOperation
from .terminology import CQLTerminologyOperation
from .query import CQLQueryOperation
from .with_clause import CQLWithClauseOperation, CQLWithoutClauseOperation
from .define import CQLDefineOperation

__all__ = [
    'CQLRetrieveOperation',
    'CQLTerminologyOperation',
    'CQLQueryOperation',
    'CQLWithClauseOperation',
    'CQLWithoutClauseOperation',
    'CQLDefineOperation',
]