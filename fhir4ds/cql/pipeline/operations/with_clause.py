"""
CQL WITH clause operations for resource relationships.

These operations handle CQL WITH clauses that establish relationships
between resources, such as:

[Patient] P
    with [Observation: "Blood Pressure"] O
        such that O.subject.reference = 'Patient/' + P.id

WITH clauses are complex because they involve cross-resource
relationships and require careful context management.
"""

from typing import Optional, List, Dict, Any, Union
import logging
from ....pipeline.core.base import PipelineOperation, SQLState, ExecutionContext, ContextMode
from ....pipeline.core.builder import FHIRPathPipeline

logger = logging.getLogger(__name__)

class CQLWithClauseOperation(PipelineOperation[SQLState]):
    """
    CQL WITH clause operation for relationships/joins.
    
    This handles CQL WITH clauses that establish relationships
    between resources, such as:
    
    [Patient] P
        with [Observation: "Blood Pressure"] O
            such that O.subject.reference = 'Patient/' + P.id
    
    WITH clauses are complex because they involve cross-resource
    relationships and require careful context management.
    """
    
    def __init__(self, identifier: str, 
                 relation_operation: PipelineOperation,
                 such_that_condition: Optional[str] = None,
                 such_that_pipeline: Optional[PipelineOperation] = None):
        """
        Initialize CQL WITH clause operation.
        
        Args:
            identifier: Identifier for the related resources (e.g., "O")
            relation_operation: Operation for the related resources (e.g., CQLRetrieveOperation)
            such_that_condition: Optional simple condition string
            such_that_pipeline: Optional condition pipeline operation
        """
        self.identifier = identifier
        self.relation_operation = relation_operation
        self.such_that_condition = such_that_condition
        self.such_that_pipeline = such_that_pipeline
        self._validate_with_clause()
    
    def _validate_with_clause(self) -> None:
        """Validate WITH clause parameters."""
        if not self.identifier:
            raise ValueError("WITH clause identifier cannot be empty")
        
        if not self.relation_operation:
            raise ValueError("WITH clause relation operation required")
        
        if not hasattr(self.relation_operation, 'execute'):
            raise ValueError("Relation operation must be a valid pipeline operation")
        
        # Validate that we have some kind of condition if needed
        has_condition = bool(self.such_that_condition or self.such_that_pipeline)
        
        # Check pipeline validity if present
        if self.such_that_pipeline and not (hasattr(self.such_that_pipeline, 'execute') or hasattr(self.such_that_pipeline, 'operations')):
            raise ValueError("Such that pipeline must be a valid pipeline operation")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute CQL WITH clause.
        
        This creates a relationship between the current context
        and the related resources specified in the WITH clause.
        
        Args:
            input_state: Current SQL state (source resources)
            context: Execution context
            
        Returns:
            New SQL state with WITH clause applied
        """
        logger.debug(f"Executing CQL WITH clause: {self.identifier}")
        
        # Step 1: Execute relation operation to get related resources
        logger.debug("Executing relation operation")
        relation_result = self.relation_operation.execute(input_state, context)
        
        # Step 2: Build appropriate join based on conditions
        if self.such_that_condition or self.such_that_pipeline:
            logger.debug("Applying conditional WITH clause")
            with_sql, with_ctes = self._build_conditional_with_sql(
                input_state, relation_result, context
            )
        else:
            logger.debug("Applying simple cross join WITH clause")
            with_sql, with_ctes = self._build_simple_with_sql(
                input_state, relation_result, context
            )
        
        # Step 3: Combine CTEs from both operations
        all_ctes = list(input_state.ctes) + list(relation_result.ctes) + with_ctes
        
        return input_state.evolve(
            sql_fragment=with_sql,
            ctes=all_ctes,
            is_collection=True,  # WITH clauses typically expand results
            context_mode=ContextMode.COLLECTION
        )
    
    def _build_conditional_with_sql(self, input_state: SQLState,
                                   relation_result: SQLState,
                                   context: ExecutionContext) -> tuple[str, List[str]]:
        """
        Build SQL for conditional WITH clause.
        
        Args:
            input_state: Current SQL state
            relation_result: Relation operation result
            context: Execution context
            
        Returns:
            Tuple of (updated_sql, additional_ctes)
        """
        # Use simple condition if available
        if self.such_that_condition:
            return self._build_simple_conditional_with_sql(
                input_state, relation_result, context
            )
        
        # Use pipeline condition
        return self._build_pipeline_conditional_with_sql(
            input_state, relation_result, context
        )
    
    def _build_simple_conditional_with_sql(self, input_state: SQLState,
                                          relation_result: SQLState,
                                          context: ExecutionContext) -> tuple[str, List[str]]:
        """
        Build SQL for simple conditional WITH clause.
        
        Args:
            input_state: Current SQL state
            relation_result: Relation operation result
            context: Execution context
            
        Returns:
            Tuple of (updated_sql, additional_ctes)
        """
        # Build conditional join with simple condition
        with_sql = f"""
        SELECT 
            source_data.{input_state.json_column} as source_resource,
            relation_data.{relation_result.json_column} as {self.identifier}_resource
        FROM ({input_state.sql_fragment}) source_data
        CROSS JOIN ({relation_result.sql_fragment}) relation_data
        WHERE ({self.such_that_condition})
        """
        
        return with_sql, []
    
    def _build_pipeline_conditional_with_sql(self, input_state: SQLState,
                                            relation_result: SQLState,
                                            context: ExecutionContext) -> tuple[str, List[str]]:
        """
        Build SQL for pipeline-based conditional WITH clause.
        
        Args:
            input_state: Current SQL state
            relation_result: Relation operation result
            context: Execution context
            
        Returns:
            Tuple of (updated_sql, additional_ctes)
        """
        # Create a combined state for the condition evaluation
        combined_state = input_state.evolve(
            sql_fragment=f"""
            SELECT 
                source_data.{input_state.json_column} as source_resource,
                relation_data.{relation_result.json_column} as {self.identifier}_resource
            FROM ({input_state.sql_fragment}) source_data
            CROSS JOIN ({relation_result.sql_fragment}) relation_data
            """,
            context_mode=ContextMode.COLLECTION
        )
        
        # Execute condition pipeline
        if hasattr(self.such_that_pipeline, 'compile'):
            # FHIRPathPipeline
            condition_result = self.such_that_pipeline.compile(context, combined_state)
            condition_sql = condition_result.main_sql
            condition_ctes = condition_result.ctes
        else:
            # PipelineOperation
            condition_result = self.such_that_pipeline.execute(combined_state, context)
            condition_sql = condition_result.sql_fragment
            condition_ctes = condition_result.ctes
        
        # Apply conditional filter
        with_sql = f"""
        SELECT combined_data.*
        FROM ({combined_state.sql_fragment}) combined_data
        WHERE ({condition_sql}) = true
        """
        
        return with_sql, condition_ctes
    
    def _build_simple_with_sql(self, input_state: SQLState,
                              relation_result: SQLState,
                              context: ExecutionContext) -> tuple[str, List[str]]:
        """
        Build SQL for simple WITH clause (cross join).
        
        Args:
            input_state: Current SQL state
            relation_result: Relation operation result
            context: Execution context
            
        Returns:
            Tuple of (updated_sql, additional_ctes)
        """
        with_sql = f"""
        SELECT 
            source_data.{input_state.json_column} as source_resource,
            relation_data.{relation_result.json_column} as {self.identifier}_resource
        FROM ({input_state.sql_fragment}) source_data
        CROSS JOIN ({relation_result.sql_fragment}) relation_data
        """
        
        return with_sql, []
    
    def optimize_for_dialect(self, dialect) -> 'CQLWithClauseOperation':
        """Optimize WITH clause for specific dialect."""
        # Optimize relation operation
        optimized_relation = None
        if self.relation_operation and hasattr(self.relation_operation, 'optimize_for_dialect'):
            optimized_relation = self.relation_operation.optimize_for_dialect(dialect)
        else:
            optimized_relation = self.relation_operation
        
        # Optimize condition pipeline if present
        optimized_such_that = None
        if self.such_that_pipeline:
            if hasattr(self.such_that_pipeline, 'optimize'):
                # FHIRPathPipeline
                exec_context = ExecutionContext(dialect=dialect)
                optimized_such_that = self.such_that_pipeline.optimize(exec_context)
            elif hasattr(self.such_that_pipeline, 'optimize_for_dialect'):
                # PipelineOperation
                optimized_such_that = self.such_that_pipeline.optimize_for_dialect(dialect)
            else:
                optimized_such_that = self.such_that_pipeline
        
        return CQLWithClauseOperation(
            identifier=self.identifier,
            relation_operation=optimized_relation,
            such_that_condition=self.such_that_condition,
            such_that_pipeline=optimized_such_that
        )
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        name = f"with({self.identifier})"
        if self.such_that_condition or self.such_that_pipeline:
            name += "_conditional"
        return name
    
    def validate_preconditions(self, input_state: SQLState,
                              context: ExecutionContext) -> None:
        """Validate WITH clause preconditions."""
        if not self.relation_operation:
            raise ValueError("Relation operation required for WITH clause")
        
        if not input_state.sql_fragment:
            raise ValueError("Input SQL fragment required for WITH clause")
        
        # Validate relation operation if it has validation
        if hasattr(self.relation_operation, 'validate_preconditions'):
            self.relation_operation.validate_preconditions(input_state, context)
        
        # Validate condition pipeline if present
        if self.such_that_pipeline:
            if hasattr(self.such_that_pipeline, 'validate_preconditions'):
                self.such_that_pipeline.validate_preconditions(input_state, context)
    
    def estimate_complexity(self, input_state: SQLState,
                           context: ExecutionContext) -> int:
        """Estimate WITH clause complexity."""
        base_complexity = 7  # WITH clauses are complex (joins)
        
        # Add complexity for relation operation
        if self.relation_operation and hasattr(self.relation_operation, 'estimate_complexity'):
            relation_complexity = self.relation_operation.estimate_complexity(input_state, context)
            base_complexity += min(relation_complexity, 2)  # Cap contribution
        
        # Add complexity for conditional joins
        if self.such_that_condition or self.such_that_pipeline:
            base_complexity += 2  # Conditional joins are more complex
            
            if self.such_that_pipeline and hasattr(self.such_that_pipeline, 'estimate_complexity'):
                try:
                    condition_complexity = self.such_that_pipeline.estimate_complexity(context, input_state)
                    base_complexity += min(condition_complexity, 1)  # Small additional contribution
                except Exception:
                    # If estimation fails, just add base condition complexity
                    base_complexity += 1
        
        return min(base_complexity, 10)


class CQLWithoutClauseOperation(PipelineOperation[SQLState]):
    """
    CQL WITHOUT clause operation for anti-joins.
    
    This handles CQL WITHOUT clauses that exclude resources
    based on the absence of related resources, such as:
    
    [Patient] P
        without [Observation: "Blood Pressure"] O
            such that O.subject.reference = 'Patient/' + P.id
    
    WITHOUT clauses are essentially anti-joins.
    """
    
    def __init__(self, identifier: str,
                 relation_operation: PipelineOperation,
                 such_that_condition: Optional[str] = None,
                 such_that_pipeline: Optional[PipelineOperation] = None):
        """
        Initialize CQL WITHOUT clause operation.
        
        Args:
            identifier: Identifier for the excluded resources (e.g., "O")
            relation_operation: Operation for the excluded resources
            such_that_condition: Optional simple condition string
            such_that_pipeline: Optional condition pipeline operation
        """
        self.identifier = identifier
        self.relation_operation = relation_operation
        self.such_that_condition = such_that_condition
        self.such_that_pipeline = such_that_pipeline
        self._validate_without_clause()
    
    def _validate_without_clause(self) -> None:
        """Validate WITHOUT clause parameters."""
        if not self.identifier:
            raise ValueError("WITHOUT clause identifier cannot be empty")
        
        if not self.relation_operation:
            raise ValueError("WITHOUT clause relation operation required")
        
        if not hasattr(self.relation_operation, 'execute'):
            raise ValueError("Relation operation must be a valid pipeline operation")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute CQL WITHOUT clause (anti-join).
        
        Args:
            input_state: Current SQL state (source resources)
            context: Execution context
            
        Returns:
            New SQL state with WITHOUT clause applied
        """
        logger.debug(f"Executing CQL WITHOUT clause: {self.identifier}")
        
        # Execute relation operation to get resources to exclude
        relation_result = self.relation_operation.execute(input_state, context)
        
        # Build anti-join SQL
        if self.such_that_condition or self.such_that_pipeline:
            without_sql, without_ctes = self._build_conditional_without_sql(
                input_state, relation_result, context
            )
        else:
            without_sql, without_ctes = self._build_simple_without_sql(
                input_state, relation_result, context
            )
        
        # Combine CTEs
        all_ctes = list(input_state.ctes) + list(relation_result.ctes) + without_ctes
        
        return input_state.evolve(
            sql_fragment=without_sql,
            ctes=all_ctes,
            is_collection=True,  # WITHOUT preserves collection status
            context_mode=ContextMode.COLLECTION
        )
    
    def _build_conditional_without_sql(self, input_state: SQLState,
                                      relation_result: SQLState,
                                      context: ExecutionContext) -> tuple[str, List[str]]:
        """Build SQL for conditional WITHOUT clause."""
        if self.such_that_condition:
            # Simple conditional anti-join
            without_sql = f"""
            SELECT source_data.*
            FROM ({input_state.sql_fragment}) source_data
            WHERE NOT EXISTS (
                SELECT 1
                FROM ({relation_result.sql_fragment}) relation_data
                WHERE ({self.such_that_condition})
            )
            """
            return without_sql, []
        else:
            # Pipeline-based conditional anti-join
            # This is more complex and would require subquery correlation
            # For now, implement a simplified version
            without_sql = f"""
            SELECT source_data.*
            FROM ({input_state.sql_fragment}) source_data
            WHERE NOT EXISTS (
                SELECT 1
                FROM ({relation_result.sql_fragment}) relation_data
                WHERE source_data.{input_state.json_column} IS NOT NULL
            )
            """
            return without_sql, []
    
    def _build_simple_without_sql(self, input_state: SQLState,
                                 relation_result: SQLState,
                                 context: ExecutionContext) -> tuple[str, List[str]]:
        """Build SQL for simple WITHOUT clause."""
        without_sql = f"""
        SELECT source_data.*
        FROM ({input_state.sql_fragment}) source_data
        WHERE NOT EXISTS (
            SELECT 1
            FROM ({relation_result.sql_fragment}) relation_data
        )
        """
        
        return without_sql, []
    
    def optimize_for_dialect(self, dialect) -> 'CQLWithoutClauseOperation':
        """Optimize WITHOUT clause for specific dialect."""
        # Similar to WITH clause optimization
        optimized_relation = None
        if self.relation_operation and hasattr(self.relation_operation, 'optimize_for_dialect'):
            optimized_relation = self.relation_operation.optimize_for_dialect(dialect)
        else:
            optimized_relation = self.relation_operation
        
        return CQLWithoutClauseOperation(
            identifier=self.identifier,
            relation_operation=optimized_relation,
            such_that_condition=self.such_that_condition,
            such_that_pipeline=self.such_that_pipeline
        )
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        name = f"without({self.identifier})"
        if self.such_that_condition or self.such_that_pipeline:
            name += "_conditional"
        return name
    
    def validate_preconditions(self, input_state: SQLState,
                              context: ExecutionContext) -> None:
        """Validate WITHOUT clause preconditions."""
        if not self.relation_operation:
            raise ValueError("Relation operation required for WITHOUT clause")
        
        if not input_state.sql_fragment:
            raise ValueError("Input SQL fragment required for WITHOUT clause")
        
        # Validate relation operation if it has validation
        if hasattr(self.relation_operation, 'validate_preconditions'):
            self.relation_operation.validate_preconditions(input_state, context)
    
    def estimate_complexity(self, input_state: SQLState,
                           context: ExecutionContext) -> int:
        """Estimate WITHOUT clause complexity."""
        base_complexity = 8  # WITHOUT clauses are very complex (anti-joins)
        
        # Add complexity for relation operation
        if self.relation_operation and hasattr(self.relation_operation, 'estimate_complexity'):
            relation_complexity = self.relation_operation.estimate_complexity(input_state, context)
            base_complexity += min(relation_complexity, 2)  # Cap contribution
        
        return min(base_complexity, 10)