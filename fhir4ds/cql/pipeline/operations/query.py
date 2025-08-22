"""
CQL query operations for complex query constructs.

These operations handle CQL query expressions with where clauses,
return clauses, and relationship (with) clauses.
"""

from typing import Optional, List, Dict, Any, Union
import logging
from ....pipeline.core.base import PipelineOperation, SQLState, ExecutionContext, ContextMode
from ....pipeline.core.builder import FHIRPathPipeline

logger = logging.getLogger(__name__)

class CQLQueryOperation(PipelineOperation[SQLState]):
    """
    Enhanced CQL query operation with full WHERE/RETURN/SORT clause support.
    
    This handles complex CQL queries with proper pipeline composition:
    
    Example CQL:
        [Observation: "Blood Pressure"] O
            where O.status = 'final' 
              and O.effectiveDateTime during "Measurement Period"
            return O.valueQuantity.value
            sort by O.effectiveDateTime desc
    """
    
    def __init__(self, source_pipeline: Optional[PipelineOperation] = None,
                 where_condition: Optional[str] = None,
                 where_pipeline: Optional[PipelineOperation] = None,
                 return_expression: Optional[str] = None,
                 return_pipeline: Optional[PipelineOperation] = None,
                 sort_expression: Optional[str] = None,
                 sort_pipeline: Optional[PipelineOperation] = None,
                 sort_direction: str = "asc",
                 alias: Optional[str] = None):
        """
        Initialize enhanced CQL query operation.
        
        Args:
            source_pipeline: Source data pipeline operation (usually a retrieve)
            where_condition: Optional simple WHERE condition as string (legacy)
            where_pipeline: Optional WHERE clause pipeline operation
            return_expression: Optional simple RETURN expression as string
            return_pipeline: Optional RETURN clause pipeline operation  
            sort_expression: Optional simple SORT expression as string
            sort_pipeline: Optional SORT clause pipeline operation
            sort_direction: Sort direction ("asc" or "desc")
            alias: Optional alias for query context
        """
        self.source_pipeline = source_pipeline
        self.where_condition = where_condition
        self.where_pipeline = where_pipeline
        self.return_expression = return_expression
        self.return_pipeline = return_pipeline
        self.sort_expression = sort_expression
        self.sort_pipeline = sort_pipeline
        self.sort_direction = sort_direction.lower()
        self.alias = alias
        self._validate_query_components()
    
    def _validate_query_components(self) -> None:
        """Validate query component parameters."""
        if self.source_pipeline and not hasattr(self.source_pipeline, 'execute'):
            raise ValueError("Source must be a valid pipeline operation")
        
        # Validate sort direction
        if self.sort_direction not in ['asc', 'desc']:
            raise ValueError("Sort direction must be 'asc' or 'desc'")
        
        # Validate pipeline compatibility
        pipeline_components = [
            ("where_pipeline", self.where_pipeline),
            ("return_pipeline", self.return_pipeline), 
            ("sort_pipeline", self.sort_pipeline)
        ]
        
        for name, pipeline in pipeline_components:
            if pipeline and not (hasattr(pipeline, 'execute') or hasattr(pipeline, 'operations')):
                raise ValueError(f"{name} must be a valid pipeline operation")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute enhanced CQL query with full WHERE/RETURN/SORT support.
        
        This implements comprehensive query processing with proper context management:
        1. Execute source pipeline (retrieve operation)
        2. Apply WHERE clause filtering (pipeline or simple condition)
        3. Apply RETURN clause projection (pipeline or simple expression)  
        4. Apply SORT clause ordering (pipeline or simple expression)
        
        Args:
            input_state: Current SQL state
            context: Execution context
            
        Returns:
            New SQL state with complete query results
        """
        logger.debug(f"Executing enhanced CQL query" + (f" with alias {self.alias}" if self.alias else ""))
        
        # Step 1: Execute source pipeline if present
        if self.source_pipeline:
            logger.debug("Executing source pipeline")
            source_result = self.source_pipeline.execute(input_state, context)
            current_sql = source_result.sql_fragment
            all_ctes = list(source_result.ctes)
            current_state = source_result
        else:
            # Use input state as-is
            current_sql = input_state.sql_fragment
            all_ctes = list(input_state.ctes)
            current_state = input_state
        
        # Step 2: Apply WHERE clause if present
        current_sql, where_ctes = self._apply_where_clause(current_sql, current_state, context)
        all_ctes.extend(where_ctes)
        
        # Step 3: Apply RETURN clause if present  
        current_sql, return_ctes, is_collection = self._apply_return_clause(current_sql, current_state, context)
        all_ctes.extend(return_ctes)
        
        # Step 4: Apply SORT clause if present
        current_sql, sort_ctes = self._apply_sort_clause(current_sql, current_state, context)
        all_ctes.extend(sort_ctes)
        
        # Determine final collection status
        if not hasattr(locals(), 'is_collection'):
            is_collection = self._determine_result_collection_status(current_state)
        
        return input_state.evolve(
            sql_fragment=current_sql,
            ctes=all_ctes,
            is_collection=is_collection,
            context_mode=ContextMode.COLLECTION if is_collection else ContextMode.SINGLE_VALUE
        )
    
    def _apply_where_clause(self, current_sql: str, current_state: SQLState, 
                           context: ExecutionContext) -> tuple[str, List[str]]:
        """
        Apply WHERE clause (pipeline or simple condition) to current query.
        
        Args:
            current_sql: Current SQL expression
            current_state: Current SQL state
            context: Execution context
            
        Returns:
            Tuple of (updated_sql, additional_ctes)
        """
        # No WHERE clause to apply
        if not self.where_pipeline and not self.where_condition:
            return current_sql, []
        
        # Use pipeline where clause if available
        if self.where_pipeline:
            return self._apply_pipeline_where_clause(current_sql, current_state, context)
        else:
            return self._apply_simple_where_clause(current_sql, context), []
    
    def _apply_simple_where_clause(self, current_sql: str, 
                                  context: ExecutionContext) -> str:
        """
        Apply simple WHERE condition to current query.
        
        Args:
            current_sql: Current SQL expression
            context: Execution context
            
        Returns:
            SQL with WHERE condition applied
        """
        filtered_sql = f"""
        SELECT *
        FROM ({current_sql}) source_data
        WHERE {self.where_condition}
        """
        
        return filtered_sql
    
    def _apply_pipeline_where_clause(self, current_sql: str, current_state: SQLState,
                                    context: ExecutionContext) -> tuple[str, List[str]]:
        """
        Apply pipeline-based WHERE clause to current query.
        
        Args:
            current_sql: Current SQL expression
            current_state: Current SQL state
            context: Execution context
            
        Returns:
            Tuple of (updated_sql, additional_ctes)
        """
        # Create state for WHERE clause evaluation
        where_state = current_state.evolve(
            sql_fragment=current_sql,
            context_mode=ContextMode.COLLECTION  # WHERE operates on collections
        )
        
        # Execute WHERE pipeline to get boolean condition
        if hasattr(self.where_pipeline, 'compile'):
            # FHIRPathPipeline
            where_result = self.where_pipeline.compile(context, where_state)
            where_sql = where_result.main_sql
            where_ctes = where_result.ctes
        else:
            # PipelineOperation
            where_result = self.where_pipeline.execute(where_state, context)
            where_sql = where_result.sql_fragment
            where_ctes = where_result.ctes
        
        # Apply WHERE condition as filter
        filtered_sql = f"""
        SELECT source_data.*
        FROM ({current_sql}) source_data
        WHERE ({where_sql}) = true
        """
        
        return filtered_sql, where_ctes
    
    def _apply_return_clause(self, current_sql: str, current_state: SQLState,
                            context: ExecutionContext) -> tuple[str, List[str], bool]:
        """
        Apply RETURN clause (pipeline or simple expression) to current query.
        
        Args:
            current_sql: Current SQL expression
            current_state: Current SQL state
            context: Execution context
            
        Returns:
            Tuple of (updated_sql, additional_ctes, is_collection)
        """
        # No RETURN clause to apply
        if not self.return_pipeline and not self.return_expression:
            # Without RETURN clause, preserve current collection status
            collection_status = self._determine_result_collection_status(current_state)
            return current_sql, [], collection_status
        
        # Use pipeline return clause if available
        if self.return_pipeline:
            return self._apply_pipeline_return_clause(current_sql, current_state, context)
        else:
            return self._apply_simple_return_clause(current_sql, context)
    
    def _apply_simple_return_clause(self, current_sql: str,
                                   context: ExecutionContext) -> tuple[str, List[str], bool]:
        """
        Apply simple RETURN expression to current query.
        
        Args:
            current_sql: Current SQL expression
            context: Execution context
            
        Returns:
            Tuple of (updated_sql, additional_ctes, is_collection)
        """
        projected_sql = f"""
        SELECT {self.return_expression} as result
        FROM ({current_sql}) query_data
        """
        
        # RETURN clause typically produces a collection of the returned values
        return projected_sql, [], True
    
    def _apply_pipeline_return_clause(self, current_sql: str, current_state: SQLState,
                                     context: ExecutionContext) -> tuple[str, List[str], bool]:
        """
        Apply pipeline-based RETURN clause to current query.
        
        Args:
            current_sql: Current SQL expression
            current_state: Current SQL state
            context: Execution context
            
        Returns:
            Tuple of (updated_sql, additional_ctes, is_collection)
        """
        # Create state for RETURN clause evaluation
        return_state = current_state.evolve(
            sql_fragment=current_sql,
            context_mode=ContextMode.COLLECTION  # RETURN operates on collections
        )
        
        # Execute RETURN pipeline to get projected values
        if hasattr(self.return_pipeline, 'compile'):
            # FHIRPathPipeline
            return_result = self.return_pipeline.compile(context, return_state)
            return_sql = return_result.main_sql
            return_ctes = return_result.ctes
        else:
            # PipelineOperation
            return_result = self.return_pipeline.execute(return_state, context)
            return_sql = return_result.sql_fragment
            return_ctes = return_result.ctes
        
        # Apply RETURN projection
        projected_sql = f"""
        SELECT ({return_sql}) as result
        FROM ({current_sql}) query_data
        """
        
        # RETURN clause typically produces a collection of the returned values
        return projected_sql, return_ctes, True
    
    def _apply_sort_clause(self, current_sql: str, current_state: SQLState,
                          context: ExecutionContext) -> tuple[str, List[str]]:
        """
        Apply SORT clause (pipeline or simple expression) to current query.
        
        Args:
            current_sql: Current SQL expression
            current_state: Current SQL state
            context: Execution context
            
        Returns:
            Tuple of (updated_sql, additional_ctes)
        """
        # No SORT clause to apply
        if not self.sort_pipeline and not self.sort_expression:
            return current_sql, []
        
        # Use pipeline sort clause if available
        if self.sort_pipeline:
            return self._apply_pipeline_sort_clause(current_sql, current_state, context)
        else:
            return self._apply_simple_sort_clause(current_sql, context), []
    
    def _apply_simple_sort_clause(self, current_sql: str,
                                 context: ExecutionContext) -> str:
        """
        Apply simple SORT expression to current query.
        
        Args:
            current_sql: Current SQL expression
            context: Execution context
            
        Returns:
            SQL with ORDER BY clause applied
        """
        sorted_sql = f"""
        SELECT *
        FROM ({current_sql}) sorted_data
        ORDER BY {self.sort_expression} {self.sort_direction.upper()}
        """
        
        return sorted_sql
    
    def _apply_pipeline_sort_clause(self, current_sql: str, current_state: SQLState,
                                   context: ExecutionContext) -> tuple[str, List[str]]:
        """
        Apply pipeline-based SORT clause to current query.
        
        Args:
            current_sql: Current SQL expression
            current_state: Current SQL state
            context: Execution context
            
        Returns:
            Tuple of (updated_sql, additional_ctes)
        """
        # Create state for SORT clause evaluation
        sort_state = current_state.evolve(
            sql_fragment=current_sql,
            context_mode=ContextMode.COLLECTION  # SORT operates on collections
        )
        
        # Execute SORT pipeline to get sort expression
        if hasattr(self.sort_pipeline, 'compile'):
            # FHIRPathPipeline
            sort_result = self.sort_pipeline.compile(context, sort_state)
            sort_sql = sort_result.main_sql
            sort_ctes = sort_result.ctes
        else:
            # PipelineOperation
            sort_result = self.sort_pipeline.execute(sort_state, context)
            sort_sql = sort_result.sql_fragment
            sort_ctes = sort_result.ctes
        
        # Apply ORDER BY
        sorted_sql = f"""
        SELECT sorted_data.*
        FROM ({current_sql}) sorted_data
        ORDER BY ({sort_sql}) {self.sort_direction.upper()}
        """
        
        return sorted_sql, sort_ctes
    
    def _determine_result_collection_status(self, input_state: SQLState) -> bool:
        """
        Determine if query result is a collection.
        
        Args:
            input_state: Input SQL state
            
        Returns:
            True if result is a collection
        """
        # For basic queries, preserve input collection status
        # Sources (like retrieves) typically return collections
        if self.source_pipeline:
            return True  # Pipeline operations typically return collections
        else:
            return input_state.is_collection
    
    def optimize_for_dialect(self, dialect) -> 'CQLQueryOperation':
        """
        Optimize query operation for specific dialect.
        
        Args:
            dialect: Target database dialect
            
        Returns:
            Optimized query operation
        """
        # Optimize all pipeline components if present
        optimized_source = None
        if self.source_pipeline and hasattr(self.source_pipeline, 'optimize_for_dialect'):
            optimized_source = self.source_pipeline.optimize_for_dialect(dialect)
        else:
            optimized_source = self.source_pipeline
        
        optimized_where_pipeline = None
        if self.where_pipeline and hasattr(self.where_pipeline, 'optimize'):
            # FHIRPathPipeline has optimize method
            exec_context = ExecutionContext(dialect=dialect)
            optimized_where_pipeline = self.where_pipeline.optimize(exec_context)
        elif self.where_pipeline and hasattr(self.where_pipeline, 'optimize_for_dialect'):
            # PipelineOperation has optimize_for_dialect method
            optimized_where_pipeline = self.where_pipeline.optimize_for_dialect(dialect)
        else:
            optimized_where_pipeline = self.where_pipeline
        
        optimized_return_pipeline = None
        if self.return_pipeline and hasattr(self.return_pipeline, 'optimize'):
            # FHIRPathPipeline has optimize method
            exec_context = ExecutionContext(dialect=dialect)
            optimized_return_pipeline = self.return_pipeline.optimize(exec_context)
        elif self.return_pipeline and hasattr(self.return_pipeline, 'optimize_for_dialect'):
            # PipelineOperation has optimize_for_dialect method
            optimized_return_pipeline = self.return_pipeline.optimize_for_dialect(dialect)
        else:
            optimized_return_pipeline = self.return_pipeline
        
        optimized_sort_pipeline = None
        if self.sort_pipeline and hasattr(self.sort_pipeline, 'optimize'):
            # FHIRPathPipeline has optimize method
            exec_context = ExecutionContext(dialect=dialect)
            optimized_sort_pipeline = self.sort_pipeline.optimize(exec_context)
        elif self.sort_pipeline and hasattr(self.sort_pipeline, 'optimize_for_dialect'):
            # PipelineOperation has optimize_for_dialect method
            optimized_sort_pipeline = self.sort_pipeline.optimize_for_dialect(dialect)
        else:
            optimized_sort_pipeline = self.sort_pipeline
        
        return CQLQueryOperation(
            source_pipeline=optimized_source,
            where_condition=self.where_condition,
            where_pipeline=optimized_where_pipeline,
            return_expression=self.return_expression,
            return_pipeline=optimized_return_pipeline,
            sort_expression=self.sort_expression,
            sort_pipeline=optimized_sort_pipeline,
            sort_direction=self.sort_direction,
            alias=self.alias
        )
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        components = ["query"]
        
        if self.source_pipeline:
            components.append("with_source")
        if self.where_condition or self.where_pipeline:
            components.append("where")
        if self.return_expression or self.return_pipeline:
            components.append("return")
        if self.sort_expression or self.sort_pipeline:
            components.append(f"sort_{self.sort_direction}")
        if self.alias:
            components.append(f"as_{self.alias}")
        
        return f"cql_{'_'.join(components)}"
    
    def validate_preconditions(self, input_state: SQLState,
                              context: ExecutionContext) -> None:
        """
        Validate query operation preconditions.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Raises:
            ValueError: If preconditions not met
        """
        # Validate source pipeline if present
        if self.source_pipeline and hasattr(self.source_pipeline, 'validate_preconditions'):
            self.source_pipeline.validate_preconditions(input_state, context)
        
        # Validate WHERE components
        if self.where_condition and not isinstance(self.where_condition, str):
            raise ValueError("WHERE condition must be a string")
        if self.where_pipeline:
            # FHIRPathPipeline doesn't have validate_preconditions, PipelineOperation might
            if hasattr(self.where_pipeline, 'validate_preconditions'):
                self.where_pipeline.validate_preconditions(input_state, context)
        
        # Validate RETURN components
        if self.return_expression and not isinstance(self.return_expression, str):
            raise ValueError("RETURN expression must be a string")
        if self.return_pipeline:
            # FHIRPathPipeline doesn't have validate_preconditions, PipelineOperation might
            if hasattr(self.return_pipeline, 'validate_preconditions'):
                self.return_pipeline.validate_preconditions(input_state, context)
        
        # Validate SORT components
        if self.sort_expression and not isinstance(self.sort_expression, str):
            raise ValueError("SORT expression must be a string")
        if self.sort_pipeline:
            # FHIRPathPipeline doesn't have validate_preconditions, PipelineOperation might
            if hasattr(self.sort_pipeline, 'validate_preconditions'):
                self.sort_pipeline.validate_preconditions(input_state, context)
    
    def estimate_complexity(self, input_state: SQLState,
                           context: ExecutionContext) -> int:
        """
        Estimate query operation complexity.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            Complexity score (0-10)
        """
        base_complexity = 3  # Basic query complexity
        
        # Add complexity for source pipeline
        if self.source_pipeline and hasattr(self.source_pipeline, 'estimate_complexity'):
            source_complexity = self.source_pipeline.estimate_complexity(input_state, context)
            base_complexity += min(source_complexity, 2)  # Cap contribution
        
        # Add complexity for WHERE clause
        if self.where_condition or self.where_pipeline:
            base_complexity += 2
            if self.where_pipeline and hasattr(self.where_pipeline, 'estimate_complexity'):
                try:
                    where_complexity = self.where_pipeline.estimate_complexity(context, input_state)
                    base_complexity += min(where_complexity, 1)  # Small additional contribution
                except Exception:
                    # If estimation fails, just add base pipeline complexity
                    base_complexity += 1
        
        # Add complexity for RETURN clause
        if self.return_expression or self.return_pipeline:
            base_complexity += 1
            if self.return_pipeline and hasattr(self.return_pipeline, 'estimate_complexity'):
                try:
                    return_complexity = self.return_pipeline.estimate_complexity(context, input_state)
                    base_complexity += min(return_complexity, 1)  # Small additional contribution
                except Exception:
                    # If estimation fails, just add base pipeline complexity
                    base_complexity += 1
        
        # Add complexity for SORT clause  
        if self.sort_expression or self.sort_pipeline:
            base_complexity += 1
            if self.sort_pipeline and hasattr(self.sort_pipeline, 'estimate_complexity'):
                try:
                    sort_complexity = self.sort_pipeline.estimate_complexity(context, input_state)
                    base_complexity += min(sort_complexity, 1)  # Small additional contribution
                except Exception:
                    # If estimation fails, just add base pipeline complexity
                    base_complexity += 1
        
        return min(base_complexity, 10)