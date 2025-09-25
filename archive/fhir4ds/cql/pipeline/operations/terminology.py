"""
CQL terminology operations for complex terminology queries.

This module provides CQL-specific terminology operations that go beyond
simple retrieve filtering, such as value set membership testing,
code system hierarchy traversal, and terminology expansion.
"""

from typing import Optional, List, Dict, Any
import logging
from ....pipeline.core.base import PipelineOperation, SQLState, ExecutionContext

logger = logging.getLogger(__name__)

class CQLTerminologyOperation(PipelineOperation[SQLState]):
    """
    Standalone terminology operation for complex terminology queries.
    
    This handles more complex terminology operations that go beyond
    simple retrieve filtering, such as:
    - Value set membership testing
    - Code system hierarchy traversal
    - Terminology expansion
    """
    
    def __init__(self, operation_type: str, terminology_args: Dict[str, Any]):
        """
        Initialize terminology operation.
        
        Args:
            operation_type: Type of terminology operation ('in_valueset', 'expand', etc.)
            terminology_args: Arguments specific to the terminology operation
        """
        self.operation_type = operation_type
        self.terminology_args = terminology_args
        self._validate_terminology_operation()
    
    def _validate_terminology_operation(self) -> None:
        """Validate terminology operation parameters."""
        valid_operations = {
            'in_valueset', 'expand', 'lookup', 'validate_code',
            'subsumes', 'find_matches'
        }
        
        if self.operation_type not in valid_operations:
            raise ValueError(f"Invalid terminology operation: {self.operation_type}")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute terminology operation.
        
        Args:
            input_state: Current SQL state
            context: Execution context with terminology client
            
        Returns:
            New SQL state with terminology operation applied
        """
        if not context.terminology_client:
            raise ValueError("Terminology client required for terminology operations")
        
        # Check if terminology client has advanced SQL generation
        if hasattr(context.terminology_client, 'generate_operation_sql'):
            terminology_sql = context.terminology_client.generate_operation_sql(
                operation_type=self.operation_type,
                args=self.terminology_args,
                input_expression=input_state.sql_fragment,
                dialect=context.dialect
            )
        else:
            # Fall back to basic operation
            terminology_sql = self._build_basic_terminology_sql(input_state, context)
        
        return input_state.evolve(
            sql_fragment=terminology_sql,
            is_collection=self._operation_returns_collection(self.operation_type)
        )
    
    def _build_basic_terminology_sql(self, input_state: SQLState, 
                                    context: ExecutionContext) -> str:
        """
        Build basic terminology SQL as fallback.
        
        Args:
            input_state: Current SQL state
            context: Execution context
            
        Returns:
            Basic SQL for terminology operation
        """
        # For now, return input unchanged for unsupported operations
        logger.warning(f"Basic terminology operation {self.operation_type} not fully implemented")
        return input_state.sql_fragment
    
    def _operation_returns_collection(self, operation_type: str) -> bool:
        """Check if terminology operation returns collection."""
        collection_operations = {'expand', 'find_matches'}
        return operation_type in collection_operations
    
    def optimize_for_dialect(self, dialect) -> 'CQLTerminologyOperation':
        """Optimize terminology operation for dialect."""
        return self
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        return f"terminology_{self.operation_type}({len(self.terminology_args)} args)"
    
    def validate_preconditions(self, input_state: SQLState, 
                              context: ExecutionContext) -> None:
        """Validate terminology operation preconditions."""
        if not context.terminology_client:
            raise ValueError("Terminology client required for terminology operations")
    
    def estimate_complexity(self, input_state: SQLState, 
                           context: ExecutionContext) -> int:
        """Estimate terminology operation complexity."""
        # Terminology operations are generally expensive
        base_complexity = 7
        
        # Some operations are more expensive than others
        expensive_operations = {'expand', 'find_matches', 'subsumes'}
        if self.operation_type in expensive_operations:
            base_complexity += 2
        
        return min(base_complexity, 10)