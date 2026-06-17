"""
Pipeline compiler that converts pipelines to SQL.

This module handles the compilation of immutable pipelines to SQL,
including CTE management and dialect-specific optimizations.
"""

from typing import Optional, List, Dict, Any
import logging
from .base import (
    PipelineOperation, SQLState, ExecutionContext, CompiledSQL, 
    ContextMode, PipelineCompilationError, PipelineValidationError
)
from .builder import FHIRPathPipeline
from .advanced_features import (
    get_cte_optimizer, get_query_plan_cache, get_smart_indexing_hints,
    get_advanced_features_stats
)

logger = logging.getLogger(__name__)

class PipelineCompiler:
    """
    Compiles FHIRPath pipelines to SQL.
    
    This replaces the monolithic SQLGenerator.visit() method with
    a focused compiler that handles pipeline-to-SQL conversion.
    """
    
    def __init__(self, dialect: 'DatabaseDialect'):
        """
        Initialize compiler for specific dialect.
        
        Args:
            dialect: Database dialect for SQL generation
        """
        self.dialect = dialect
        self.compilation_stats = {
            'pipelines_compiled': 0,
            'operations_executed': 0,
            'ctes_created': 0,
            'optimizations_applied': 0
        }
    
    def compile(self, pipeline: FHIRPathPipeline, context: ExecutionContext,
                initial_state: Optional[SQLState] = None) -> CompiledSQL:
        """
        Compile pipeline to SQL.
        
        Args:
            pipeline: Pipeline to compile
            context: Execution context
            initial_state: Optional initial SQL state
            
        Returns:
            Compiled SQL ready for execution
        """
        self.compilation_stats['pipelines_compiled'] += 1
        
        try:
            # Create initial state if not provided
            if initial_state is None:
                initial_state = self._create_default_initial_state(context)
            
            # Validate pipeline
            self._validate_pipeline(pipeline, initial_state, context)
            
            # Execute operations sequentially
            current_state = initial_state
            for i, operation in enumerate(pipeline.operations):
                logger.debug(f"Executing operation {i}: {operation.get_operation_name()}")
                
                # Validate preconditions
                operation.validate_preconditions(current_state, context)
                
                # Execute operation
                current_state = operation.execute(current_state, context)
                self.compilation_stats['operations_executed'] += 1
            
            # Build final compiled result
            result = CompiledSQL(
                main_sql=current_state.sql_fragment,
                ctes=list(current_state.ctes),
                lateral_joins=list(current_state.lateral_joins),
                is_collection_result=current_state.is_collection,
                estimated_complexity=pipeline.estimate_complexity(context, initial_state)
            )
            
            self.compilation_stats['ctes_created'] += len(result.ctes)
            
            logger.debug(f"Compiled pipeline: {len(pipeline.operations)} operations, "
                        f"{len(result.ctes)} CTEs, complexity={result.estimated_complexity}")
            
            return result
            
        except Exception as e:
            logger.error(f"Pipeline compilation failed: {e}")
            raise PipelineCompilationError(f"Failed to compile pipeline: {e}") from e
    
    def _create_default_initial_state(self, context: ExecutionContext) -> SQLState:
        """
        Create default initial SQL state.
        
        Args:
            context: Execution context
            
        Returns:
            Default initial state for compilation
        """
        return SQLState(
            base_table="fhir_resources",  # Default table name
            json_column="resource",       # Default JSON column
            sql_fragment="resource",      # Start with resource column
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _validate_pipeline(self, pipeline: FHIRPathPipeline, 
                          initial_state: SQLState, context: ExecutionContext) -> None:
        """
        Validate pipeline before compilation.
        
        Args:
            pipeline: Pipeline to validate
            initial_state: Initial state
            context: Execution context
            
        Raises:
            PipelineValidationError: If validation fails
        """
        if pipeline.is_empty():
            raise PipelineValidationError("Cannot compile empty pipeline")
        
        # Validate each operation can handle the state type
        current_state = initial_state
        for i, operation in enumerate(pipeline.operations):
            try:
                operation.validate_preconditions(current_state, context)
                # Simulate execution for validation
                current_state = operation.execute(current_state, context)
            except Exception as e:
                raise PipelineValidationError(
                    f"Operation {i} ({operation.get_operation_name()}) validation failed: {e}"
                ) from e
    
    def get_compilation_stats(self) -> Dict[str, Any]:
        """
        Get compilation statistics.
        
        Returns:
            Dictionary with compilation statistics
        """
        return self.compilation_stats.copy()
    
    def reset_stats(self) -> None:
        """Reset compilation statistics."""
        self.compilation_stats = {
            'pipelines_compiled': 0,
            'operations_executed': 0,
            'ctes_created': 0,
            'optimizations_applied': 0
        }

class OptimizedPipelineCompiler(PipelineCompiler):
    """
    Enhanced compiler with optimization passes.
    
    This compiler applies various optimization strategies
    before and during compilation to improve SQL performance.
    """
    
    def __init__(self, dialect: 'DatabaseDialect'):
        super().__init__(dialect)
        self.optimization_passes = [
            self._merge_adjacent_path_operations,
            self._optimize_collection_operations,
            self._inline_simple_literals,
            self._optimize_function_chains,
            self._optimize_cte_usage,
            self._optimize_sql_generation
        ]
    
    def compile(self, pipeline: FHIRPathPipeline, context: ExecutionContext,
                initial_state: Optional[SQLState] = None) -> CompiledSQL:
        """
        Compile pipeline with advanced optimization and caching.
        
        Args:
            pipeline: Pipeline to compile
            context: Execution context
            initial_state: Optional initial SQL state
            
        Returns:
            Optimized compiled SQL with advanced features
        """
        # Check query plan cache first
        cache = get_query_plan_cache()
        if context.enable_query_plan_cache:
            cache_key = cache.get_cache_key(pipeline, context, initial_state)
            cached_result = cache.get(cache_key)
            if cached_result:
                logger.debug("Using cached query plan")
                return cached_result
        
        # Apply CTE optimization analysis
        cte_optimizer = get_cte_optimizer()
        cte_analyses = []
        if context.enable_cte_optimization:
            cte_analyses = cte_optimizer.analyze_pipeline_for_cte(pipeline, context)
            logger.debug(f"CTE analysis found {len(cte_analyses)} optimization opportunities")
        
        # Apply standard optimization passes
        optimized_pipeline = pipeline
        if context.optimization_level > 0:
            for pass_func in self.optimization_passes:
                try:
                    optimized_pipeline = pass_func(optimized_pipeline, context)
                    self.compilation_stats['optimizations_applied'] += 1
                except Exception as e:
                    logger.warning(f"Optimization pass failed: {e}")
                    # Continue with non-optimized pipeline
                    break
        
        # Apply CTE optimizations based on analysis
        if cte_analyses and context.enable_cte_optimization:
            optimized_pipeline = self._apply_cte_optimizations(optimized_pipeline, cte_analyses, context)
        
        # Generate smart indexing hints
        smart_indexing = get_smart_indexing_hints()
        if context.enable_smart_indexing:
            indexing_hints = smart_indexing.analyze_paths(optimized_pipeline, context)
            if indexing_hints:
                logger.info(f"Generated {len(indexing_hints)} indexing hints")
        
        # Compile optimized pipeline
        compiled_sql = super().compile(optimized_pipeline, context, initial_state)
        
        # Cache the result
        if context.enable_query_plan_cache:
            cache.put(cache_key, compiled_sql)
        
        return compiled_sql
    
    def _apply_cte_optimizations(self, pipeline: FHIRPathPipeline, 
                                cte_analyses, context: ExecutionContext) -> FHIRPathPipeline:
        """
        Apply CTE optimizations based on analysis results.
        
        Args:
            pipeline: Pipeline to optimize
            cte_analyses: CTE analysis results
            context: Execution context
            
        Returns:
            Pipeline with CTE optimizations applied
        """
        cte_optimizer = get_cte_optimizer()
        
        # For each analysis that recommends CTE creation
        for analysis in cte_analyses:
            if analysis.should_create_cte:
                if analysis.reuse_existing_cte:
                    logger.debug(f"Reusing existing CTE: {analysis.reuse_existing_cte}")
                    cte_optimizer.optimization_stats['ctes_reused'] += 1
                else:
                    # Create new CTE
                    cte_name = f"cte_{analysis.signature.operation_hash}"
                    cte_optimizer.register_cte(cte_name, analysis.signature)
                    logger.debug(f"Created new CTE: {cte_name}")
        
        return pipeline
    
    def get_advanced_features_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics for all advanced features."""
        base_stats = self.get_compilation_stats()
        advanced_stats = get_advanced_features_stats()
        
        return {
            'compiler_stats': base_stats,
            'advanced_features': advanced_stats
        }
    
    def _merge_adjacent_path_operations(self, pipeline: FHIRPathPipeline, 
                                       context: ExecutionContext) -> FHIRPathPipeline:
        """
        Merge adjacent path navigation operations for better performance.
        
        Combines consecutive PathNavigationOperation instances into a single
        operation with a combined JSON path, reducing SQL complexity.
        
        Example: Patient → name → family becomes Patient.name.family
        """
        from ..operations.path import PathNavigationOperation
        from .builder import FHIRPathPipeline
        
        if len(pipeline.operations) < 2:
            return pipeline
        
        optimized_operations = []
        i = 0
        
        while i < len(pipeline.operations):
            current_op = pipeline.operations[i]
            
            # Check if current and next operations are both path navigation
            if (isinstance(current_op, PathNavigationOperation) and 
                i + 1 < len(pipeline.operations) and
                isinstance(pipeline.operations[i + 1], PathNavigationOperation)):
                
                # Collect consecutive path operations
                path_segments = [current_op.path_segment]
                j = i + 1
                
                while (j < len(pipeline.operations) and
                       isinstance(pipeline.operations[j], PathNavigationOperation)):
                    path_segments.append(pipeline.operations[j].path_segment)
                    j += 1
                
                # Create merged path operation
                combined_path = '.'.join(path_segments)
                merged_op = PathNavigationOperation(combined_path)
                optimized_operations.append(merged_op)
                i = j  # Skip the merged operations
                
            else:
                optimized_operations.append(current_op)
                i += 1
        
        return FHIRPathPipeline(optimized_operations)
    
    def _optimize_collection_operations(self, pipeline: FHIRPathPipeline,
                                       context: ExecutionContext) -> FHIRPathPipeline:
        """
        Optimize collection processing operations for better performance.
        
        Applies optimizations like:
        - Converting .where().first() to optimized single-element extraction
        - Optimizing .count().exists() to just exists()
        - Combining adjacent collection operations
        """
        from ..operations.functions import FunctionCallOperation
        from .builder import FHIRPathPipeline
        
        if len(pipeline.operations) < 2:
            return pipeline
        
        optimized_operations = []
        i = 0
        
        while i < len(pipeline.operations):
            current_op = pipeline.operations[i]
            
            # Optimize where().first() pattern
            if (isinstance(current_op, FunctionCallOperation) and 
                current_op.func_name == 'where' and
                i + 1 < len(pipeline.operations) and
                isinstance(pipeline.operations[i + 1], FunctionCallOperation) and
                pipeline.operations[i + 1].func_name == 'first'):
                
                # Create optimized single-element where operation
                optimized_where = FunctionCallOperation('where_first', current_op.args)
                optimized_operations.append(optimized_where)
                i += 2  # Skip both operations
                
            # Optimize count().exists() to just exists()
            elif (isinstance(current_op, FunctionCallOperation) and 
                  current_op.func_name == 'count' and
                  i + 1 < len(pipeline.operations) and
                  isinstance(pipeline.operations[i + 1], FunctionCallOperation) and
                  pipeline.operations[i + 1].func_name == 'exists'):
                
                # Replace with just exists() - more efficient
                optimized_op = FunctionCallOperation('exists', [])
                optimized_operations.append(optimized_op)
                i += 2  # Skip both operations
                
            # Optimize empty collection checks
            elif (isinstance(current_op, FunctionCallOperation) and 
                  current_op.func_name == 'count' and
                  i + 1 < len(pipeline.operations) and
                  isinstance(pipeline.operations[i + 1], FunctionCallOperation) and
                  pipeline.operations[i + 1].func_name == 'empty'):
                
                # Replace with just empty() check
                optimized_op = FunctionCallOperation('empty', [])
                optimized_operations.append(optimized_op)
                i += 2  # Skip both operations
                
            else:
                optimized_operations.append(current_op)
                i += 1
        
        return FHIRPathPipeline(optimized_operations)
    
    def _inline_simple_literals(self, pipeline: FHIRPathPipeline,
                               context: ExecutionContext) -> FHIRPathPipeline:
        """
        Inline simple literal values to reduce operation overhead.
        
        Converts simple literal operations directly into SQL values
        instead of going through the full operation pipeline for constants.
        """
        from ..operations.literals import LiteralOperation
        from ..operations.functions import FunctionCallOperation
        from .builder import FHIRPathPipeline
        
        optimized_operations = []
        
        for operation in pipeline.operations:
            # Inline simple string/number literals in function arguments
            if isinstance(operation, FunctionCallOperation):
                optimized_args = []
                for arg in operation.args:
                    if (isinstance(arg, LiteralOperation) and 
                        arg.value_type in ['string', 'integer', 'decimal'] and
                        len(str(arg.value)) < 20):  # Only inline short literals
                        optimized_args.append(arg.value)  # Direct value instead of operation
                    else:
                        optimized_args.append(arg)
                
                optimized_op = FunctionCallOperation(operation.func_name, optimized_args)
                optimized_operations.append(optimized_op)
            else:
                optimized_operations.append(operation)
        
        return FHIRPathPipeline(optimized_operations)
    
    def _optimize_cte_usage(self, pipeline: FHIRPathPipeline,
                           context: ExecutionContext) -> FHIRPathPipeline:
        """
        Optimize Common Table Expression usage for better performance.
        
        Decides when to use CTEs vs inline expressions based on:
        - Expression complexity
        - Reuse patterns  
        - Dialect capabilities
        """
        from ..operations.functions import FunctionCallOperation
        from .builder import FHIRPathPipeline
        
        # Analyze pipeline complexity
        total_complexity = pipeline.estimate_complexity(context)
        
        # If complexity is low, prefer inline expressions
        if total_complexity < 20:
            # Mark operations to avoid CTEs
            for operation in pipeline.operations:
                if hasattr(operation, '_prefer_inline'):
                    operation._prefer_inline = True
        
        # For high complexity operations, prefer CTEs
        elif total_complexity > 50:
            complex_operations = []
            for operation in pipeline.operations:
                if isinstance(operation, FunctionCallOperation):
                    op_complexity = operation.estimate_complexity(
                        context.dialect.default_initial_state() if hasattr(context.dialect, 'default_initial_state') 
                        else None, context
                    )
                    if op_complexity > 5:
                        complex_operations.append(operation)
                        if hasattr(operation, '_prefer_cte'):
                            operation._prefer_cte = True
        
        # For medium complexity, use dialect-specific heuristics
        else:
            # DuckDB handles inline expressions well
            if context.dialect.name.upper() == 'DUCKDB':
                # Prefer inline for DuckDB
                for operation in pipeline.operations:
                    if hasattr(operation, '_prefer_inline'):
                        operation._prefer_inline = True
            # PostgreSQL benefits more from CTEs
            elif context.dialect.name.upper() == 'POSTGRESQL':
                # Use CTEs for complex operations
                for operation in pipeline.operations:
                    if isinstance(operation, FunctionCallOperation):
                        if hasattr(operation, '_prefer_cte'):
                            operation._prefer_cte = True
        
        return pipeline
    
    def _optimize_function_chains(self, pipeline: FHIRPathPipeline,
                                 context: ExecutionContext) -> FHIRPathPipeline:
        """
        Optimize common function call chains.
        
        NEW OPTIMIZATION PASS: Identifies and optimizes common patterns like:
        - .select().where() → optimized filter-map
        - .first().exists() → optimized head check
        - .distinct().count() → optimized unique count
        """
        from ..operations.functions import FunctionCallOperation
        from .builder import FHIRPathPipeline
        
        if len(pipeline.operations) < 2:
            return pipeline
        
        optimized_operations = []
        i = 0
        
        while i < len(pipeline.operations):
            current_op = pipeline.operations[i]
            
            # Optimize select().where() chains
            if (isinstance(current_op, FunctionCallOperation) and 
                current_op.func_name == 'select' and
                i + 1 < len(pipeline.operations) and
                isinstance(pipeline.operations[i + 1], FunctionCallOperation) and
                pipeline.operations[i + 1].func_name == 'where'):
                
                # Combine into filter-map operation
                select_args = current_op.args
                where_args = pipeline.operations[i + 1].args
                combined_op = FunctionCallOperation('filter_map', [select_args, where_args])
                optimized_operations.append(combined_op)
                i += 2
                
            # Optimize distinct().count() chains  
            elif (isinstance(current_op, FunctionCallOperation) and 
                  current_op.func_name == 'distinct' and
                  i + 1 < len(pipeline.operations) and
                  isinstance(pipeline.operations[i + 1], FunctionCallOperation) and
                  pipeline.operations[i + 1].func_name == 'count'):
                
                # Create optimized unique count
                unique_count_op = FunctionCallOperation('unique_count', [])
                optimized_operations.append(unique_count_op)
                i += 2
                
            else:
                optimized_operations.append(current_op)
                i += 1
        
        return FHIRPathPipeline(optimized_operations)
    
    def _optimize_sql_generation(self, pipeline: FHIRPathPipeline,
                                context: ExecutionContext) -> FHIRPathPipeline:
        """
        Apply SQL-specific optimizations based on dialect capabilities.
        
        NEW OPTIMIZATION PASS: Applies dialect-specific optimizations:
        - DuckDB: JSON array optimizations, vectorized operations
        - PostgreSQL: JSONB operator optimizations, index hints
        """
        from .builder import FHIRPathPipeline
        
        dialect_name = context.dialect.name.upper()
        
        # DuckDB-specific optimizations
        if dialect_name == 'DUCKDB':
            # Optimize for DuckDB's excellent JSON array performance
            for operation in pipeline.operations:
                if hasattr(operation, '_duckdb_optimize'):
                    operation._duckdb_optimize = True
                    
        # PostgreSQL-specific optimizations  
        elif dialect_name == 'POSTGRESQL':
            # Optimize for PostgreSQL's JSONB operators
            for operation in pipeline.operations:
                if hasattr(operation, '_postgresql_optimize'):
                    operation._postgresql_optimize = True
        
        return pipeline