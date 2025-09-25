"""
CQL retrieve operations for resource retrieval with terminology filtering.

Enhanced implementation with terminology integration that demonstrates
the full integration between CQL constructs and the FHIRPath pipeline system.

Example CQL:
    [Observation: "Blood Pressure Systolic"] 
    [Patient] P
    [Condition: "Diabetes mellitus"] C where C.status = 'active'
"""

from typing import Optional, List, Dict, Any
import logging
from ....pipeline.core.base import PipelineOperation, SQLState, ExecutionContext, ContextMode

logger = logging.getLogger(__name__)

class CQLRetrieveOperation(PipelineOperation[SQLState]):
    """
    Enhanced CQL retrieve operation with terminology integration.
    
    This operation handles CQL resource retrieval with optional
    terminology filtering. It replaces the _translate_retrieve
    method in the current CQLTranslator.
    
    Example CQL:
        [Observation: "Blood Pressure Systolic"]  -> CQLRetrieveOperation("Observation", terminology="Blood Pressure Systolic")
        [Patient] P                               -> CQLRetrieveOperation("Patient", alias="P")
        [Condition: "Diabetes"]                   -> CQLRetrieveOperation("Condition", terminology="Diabetes")
    """
    
    def __init__(self, resource_type: str, 
                 terminology: Optional[str] = None,
                 code_path: Optional[str] = None,
                 alias: Optional[str] = None):
        """
        Initialize CQL retrieve operation with terminology support.
        
        Args:
            resource_type: FHIR resource type (e.g., "Patient", "Observation")
            terminology: Optional terminology code/system for filtering
            code_path: Path to code field for terminology filtering (default: "code")
            alias: Optional alias for the retrieved resources (e.g., "P", "O")
        """
        self.resource_type = resource_type
        self.terminology = terminology
        self.code_path = code_path or "code"
        self.alias = alias
        self._validate_retrieve_params()
    
    def _validate_retrieve_params(self) -> None:
        """Validate retrieve operation parameters."""
        if not self.resource_type:
            raise ValueError("Resource type cannot be empty")
        
        if not isinstance(self.resource_type, str):
            raise ValueError("Resource type must be a string")
        
        # Basic FHIR resource type validation
        if not self.resource_type[0].isupper():
            raise ValueError("FHIR resource types must start with uppercase letter")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute CQL retrieve operation.
        
        Args:
            input_state: Current SQL state
            context: Execution context with dialect and services
            
        Returns:
            New SQL state with retrieve operation applied
        """
        logger.debug(f"Executing CQL retrieve: {self.resource_type}" + 
                    (f" with terminology: {self.terminology}" if self.terminology else "") +
                    (f" with alias: {self.alias}" if self.alias else ""))
        
        # Build base resource type filter
        resource_filter = self._build_resource_type_filter(input_state, context)
        
        # Add terminology filter if specified
        if self.terminology and context.terminology_client:
            terminology_filter = self._build_terminology_filter(input_state, context)
            combined_filter = f"({resource_filter}) AND ({terminology_filter})"
        else:
            combined_filter = resource_filter
        
        # Build complete retrieve SQL
        retrieve_sql = self._build_retrieve_sql(input_state, combined_filter, context)
        
        # Update state with retrieve results
        return input_state.evolve(
            sql_fragment=retrieve_sql,
            resource_type=self.resource_type,
            is_collection=True,  # Retrieve always returns a collection
            context_mode=ContextMode.COLLECTION,
            path_context="$"  # Reset to root context for retrieved resources
        )
    
    def _build_resource_type_filter(self, input_state: SQLState, 
                                   context: ExecutionContext) -> str:
        """
        Build SQL filter for resource type.
        
        Args:
            input_state: Current SQL state
            context: Execution context
            
        Returns:
            SQL condition for filtering by resource type
        """
        json_column = input_state.json_column
        
        if context.dialect.name.upper() == 'DUCKDB':
            return f"json_extract_string({json_column}, '$.resourceType') = '{self.resource_type}'"
        else:  # PostgreSQL
            return f"({json_column} ->> 'resourceType') = '{self.resource_type}'"
    
    def _build_terminology_filter(self, input_state: SQLState,
                                 context: ExecutionContext) -> str:
        """
        Build SQL filter for terminology codes.
        
        Args:
            input_state: Current SQL state
            context: Execution context with terminology client
            
        Returns:
            SQL condition for terminology filtering
        """
        if not context.terminology_client:
            logger.warning("Terminology client not available for filtering")
            return "1=1"  # No filtering
        
        try:
            # Check if terminology client has SQL generation method
            if hasattr(context.terminology_client, 'generate_filter_sql'):
                return context.terminology_client.generate_filter_sql(
                    terminology=self.terminology,
                    code_path=self.code_path,
                    json_column=input_state.json_column,
                    dialect=context.dialect
                )
            else:
                # Fall back to basic code matching
                return self._build_basic_code_filter(input_state, context)
        except Exception as e:
            logger.error(f"Failed to generate terminology filter: {e}")
            # Fall back to basic code matching
            return self._build_basic_code_filter(input_state, context)
    
    def _build_basic_code_filter(self, input_state: SQLState,
                                context: ExecutionContext) -> str:
        """
        Build basic code filter as fallback.
        
        Args:
            input_state: Current SQL state
            context: Execution context
            
        Returns:
            Basic SQL condition for code matching
        """
        json_column = input_state.json_column
        
        if context.dialect.name.upper() == 'DUCKDB':
            return f"""
            json_extract({json_column}, '$.{self.code_path}.coding[0].code') = '{self.terminology}'
            OR json_extract({json_column}, '$.{self.code_path}.text') = '{self.terminology}'
            """
        else:  # PostgreSQL
            return f"""
            ({json_column} -> '{self.code_path}' -> 'coding' -> 0 ->> 'code') = '{self.terminology}'
            OR ({json_column} -> '{self.code_path}' ->> 'text') = '{self.terminology}'
            """
    
    def _build_retrieve_sql(self, input_state: SQLState, filter_condition: str,
                           context: ExecutionContext) -> str:
        """
        Build complete SQL for retrieve operation.
        
        Args:
            input_state: Current SQL state
            filter_condition: Resource type filter condition
            context: Execution context
            
        Returns:
            Complete SQL for retrieve operation
        """
        base_table = input_state.base_table
        json_column = input_state.json_column
        
        # For proof-of-concept, use simple subquery without CTE optimization
        return f"""
        (
            SELECT {json_column}
            FROM {base_table}
            WHERE {filter_condition}
        )
        """
    
    def optimize_for_dialect(self, dialect) -> 'CQLRetrieveOperation':
        """
        Optimize retrieve operation for specific dialect.
        
        Args:
            dialect: Target database dialect
            
        Returns:
            Optimized retrieve operation (for now, returns self)
        """
        # In the proof-of-concept, no dialect-specific optimizations yet
        return self
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        if self.terminology:
            name = f"retrieve({self.resource_type}: '{self.terminology}')"
        else:
            name = f"retrieve({self.resource_type})"
        
        if self.alias:
            name += f" as {self.alias}"
        return name
    
    def validate_preconditions(self, input_state: SQLState, 
                              context: ExecutionContext) -> None:
        """
        Validate retrieve operation preconditions.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Raises:
            ValueError: If preconditions not met
        """
        if not input_state.base_table:
            raise ValueError("Base table required for retrieve operation")
        
        if not input_state.json_column:
            raise ValueError("JSON column required for retrieve operation")
        
        # Validate terminology client if terminology specified
        if self.terminology and not context.terminology_client:
            logger.warning(
                f"Terminology '{self.terminology}' specified but no terminology client available"
            )
    
    def estimate_complexity(self, input_state: SQLState, 
                           context: ExecutionContext) -> int:
        """
        Estimate complexity of retrieve operation.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            Complexity score (0-10)
        """
        base_complexity = 4  # Resource retrieval is moderately complex
        
        # Terminology filtering adds complexity
        if self.terminology:
            base_complexity += 3
        
        # Complex code paths add complexity
        if self.code_path and self.code_path != "code":
            base_complexity += 1
        
        return min(base_complexity, 10)