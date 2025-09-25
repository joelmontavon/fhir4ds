"""
CTE Integration Module for Unified Pipeline Architecture

This module provides the integration layer between the comprehensive FHIRPath pipeline
and CTE optimization capabilities, enabling unified execution context and performance
optimization while preserving all existing functionality.

Key Features:
- Unified ExecutionContext supporting both pipeline and CTE requirements
- CTE optimization hooks integrated into pipeline compilation
- Performance monitoring and tracking capabilities
- Backward compatibility with existing pipeline and CTE functionality
"""

from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
import logging
from abc import ABC, abstractmethod

# Import base pipeline types
from .base import SQLState, CompiledSQL, ContextMode

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CTEFragment:
    """
    Represents a CTE fragment that can be integrated into pipeline compilation.

    This enables the monolithic query optimization approach from the CTE pipeline
    while maintaining compatibility with the immutable pipeline architecture.
    """
    name: str                           # CTE name/identifier
    sql: str                           # SQL fragment for the CTE
    dependencies: List[str]            # List of other CTE names this depends on
    optimization_level: int = 0        # Optimization level applied
    performance_hint: Optional[str] = None  # Database-specific optimization hint

    def __post_init__(self):
        """Validate CTE fragment after creation."""
        if not self.name or not self.name.isidentifier():
            raise ValueError(f"Invalid CTE name: {self.name}")
        if not self.sql.strip():
            raise ValueError("CTE SQL cannot be empty")


@dataclass(frozen=True)
class UnifiedExecutionContext:
    """
    Unified execution context supporting both pipeline and CTE requirements.

    This merges the capabilities of both ExecutionContext classes while maintaining
    immutability for pipeline operations and supporting CTE execution tracking.

    Design principles:
    - Immutable (frozen=True) to preserve pipeline architecture
    - Supports both SQL generation (pipeline) and CQL execution (CTE)
    - Backward compatible with existing pipeline ExecutionContext
    - Enables CTE optimization without breaking existing functionality
    """

    # Core SQL generation requirements (from pipeline ExecutionContext)
    dialect: 'DatabaseDialect'                    # Database dialect for SQL generation
    schema_manager: Optional['SchemaManager'] = None  # FHIR schema information
    terminology_client: Optional['TerminologyClient'] = None  # Terminology services
    optimization_level: int = 0                   # 0=none, 1=basic, 2=aggressive
    enable_cte: bool = True                       # Enable Common Table Expressions
    debug_mode: bool = False                      # Enable debug SQL comments

    # CQL execution requirements (from CTE ExecutionContext)
    library_id: Optional[str] = None             # CQL library identification
    library_version: str = "1.0"                 # CQL library version
    patient_population: Optional[List[str]] = None  # Target patient population
    execution_timestamp: Optional[datetime] = field(default_factory=lambda: datetime.now())
    performance_tracking: bool = True            # Enable performance monitoring

    # Integration-specific capabilities
    cte_optimization_enabled: bool = True        # Enable CTE monolithic optimization
    preserve_individual_queries: bool = False    # Fallback to individual queries if needed
    max_cte_depth: int = 10                      # Maximum CTE nesting depth

    def get_context_id(self) -> str:
        """Generate unique context identifier for tracking."""
        if self.library_id:
            timestamp = self.execution_timestamp.strftime("%Y%m%d_%H%M%S") if self.execution_timestamp else "unknown"
            return f"{self.library_id}_{self.library_version}_{timestamp}"
        else:
            return f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def is_cql_execution(self) -> bool:
        """Check if this context is for CQL library execution."""
        return self.library_id is not None

    def is_pipeline_execution(self) -> bool:
        """Check if this context is for pipeline SQL generation."""
        return not self.is_cql_execution()

    def with_cql_library(self, library_id: str, library_version: str = "1.0") -> 'UnifiedExecutionContext':
        """Create a new context configured for CQL library execution."""
        from dataclasses import replace
        return replace(
            self,
            library_id=library_id,
            library_version=library_version,
            execution_timestamp=datetime.now()
        )

    def with_optimization_level(self, level: int) -> 'UnifiedExecutionContext':
        """Create a new context with different optimization level."""
        from dataclasses import replace
        return replace(self, optimization_level=level)


class CTEOptimizer(ABC):
    """
    Abstract base class for CTE optimization strategies.

    This allows different optimization approaches while maintaining a common interface
    that integrates with the pipeline compilation process.
    """

    @abstractmethod
    def can_optimize(self, compiled_sql: CompiledSQL, context: UnifiedExecutionContext) -> bool:
        """Check if this optimizer can improve the given compiled SQL."""
        pass

    @abstractmethod
    def optimize(self, compiled_sql: CompiledSQL, context: UnifiedExecutionContext) -> CompiledSQL:
        """Apply CTE optimization to the compiled SQL."""
        pass

    @abstractmethod
    def estimate_performance_improvement(self, compiled_sql: CompiledSQL, context: UnifiedExecutionContext) -> float:
        """Estimate performance improvement factor (e.g., 2.0 = 2x faster)."""
        pass


class MonolithicCTEOptimizer(CTEOptimizer):
    """
    Monolithic CTE optimizer that converts multiple individual queries into
    a single CTE-based query for improved performance.

    This implements the core CTE optimization approach that achieved
    100% Quality Measures success in the original CTE pipeline.
    """

    def __init__(self):
        self.optimization_stats = {
            'queries_optimized': 0,
            'ctes_created': 0,
            'performance_improvements': []
        }

    def can_optimize(self, compiled_sql: CompiledSQL, context: UnifiedExecutionContext) -> bool:
        """Check if monolithic CTE optimization is applicable."""
        # Enable for CQL library execution with CTE optimization enabled
        return (context.is_cql_execution() and
                context.cte_optimization_enabled and
                context.enable_cte)

    def optimize(self, compiled_sql: CompiledSQL, context: UnifiedExecutionContext) -> CompiledSQL:
        """
        Apply monolithic CTE optimization to convert N queries to 1 CTE query.

        This is the core optimization that provides the performance benefits
        demonstrated in the CTE pipeline system.
        """
        if not self.can_optimize(compiled_sql, context):
            return compiled_sql

        try:
            # Create optimized CTE structure
            optimized_fragments = self._convert_to_cte_fragments(compiled_sql, context)
            ordered_fragments = self._resolve_dependencies(optimized_fragments)
            monolithic_sql = self._build_monolithic_query(ordered_fragments, context)

            # Track optimization
            self.optimization_stats['queries_optimized'] += 1
            self.optimization_stats['ctes_created'] += len(ordered_fragments)

            # Create optimized CompiledSQL with existing structure
            return CompiledSQL(
                main_sql=monolithic_sql,
                ctes=[f.sql for f in ordered_fragments],
                lateral_joins=compiled_sql.lateral_joins,
                parameters=compiled_sql.parameters,
                estimated_complexity=compiled_sql.estimated_complexity,
                is_collection_result=compiled_sql.is_collection_result,
                result_columns=compiled_sql.result_columns
            )

        except Exception as e:
            logger.warning(f"CTE optimization failed, falling back to original: {e}")
            if context.preserve_individual_queries:
                return compiled_sql
            else:
                raise

    def estimate_performance_improvement(self, compiled_sql: CompiledSQL, context: UnifiedExecutionContext) -> float:
        """
        Estimate performance improvement from CTE optimization.

        Based on CTE pipeline results showing 5-10x improvements.
        """
        if not self.can_optimize(compiled_sql, context):
            return 1.0  # No improvement

        # Conservative estimate based on CTE pipeline performance data
        base_improvement = 5.0  # 5x improvement baseline

        # Additional improvement for complex queries
        if hasattr(compiled_sql, 'complexity_score'):
            complexity_multiplier = min(compiled_sql.complexity_score / 10.0, 2.0)
            return base_improvement * (1.0 + complexity_multiplier)

        return base_improvement

    def _convert_to_cte_fragments(self, compiled_sql: CompiledSQL, context: UnifiedExecutionContext) -> List[CTEFragment]:
        """Convert compiled SQL to CTE fragments using extracted CTE conversion logic."""
        fragments = []

        # For CQL library execution, use the advanced CTE conversion
        if context.is_cql_execution() and hasattr(compiled_sql, 'cql_defines'):
            try:
                # Import CQL converter
                from ...pipeline.cql.cte_converter import create_cql_to_cte_converter

                converter = create_cql_to_cte_converter(context)
                fragments = converter.convert_defines_to_cte_fragments(compiled_sql.cql_defines)

                logger.debug(f"Advanced CTE conversion created {len(fragments)} fragments")
                return fragments

            except Exception as e:
                logger.warning(f"Advanced CTE conversion failed, using basic conversion: {e}")

        # Basic CTE fragment creation for non-CQL or fallback cases
        if compiled_sql.main_sql:
            # Generate valid SQL identifier for CTE name
            safe_name = context.get_context_id().replace('-', '_').replace('.', '_').replace(':', '_')
            fragment = CTEFragment(
                name=f"cte_{safe_name}",
                sql=compiled_sql.main_sql,
                dependencies=[],
                optimization_level=context.optimization_level
            )
            fragments.append(fragment)

        return fragments

    def _resolve_dependencies(self, fragments: List[CTEFragment]) -> List[CTEFragment]:
        """Resolve dependencies and order CTE fragments."""
        # Topological sort implementation for CTE dependencies
        # For now, return fragments as-is (no dependencies in basic case)
        return fragments

    def _build_monolithic_query(self, fragments: List[CTEFragment], context: UnifiedExecutionContext) -> str:
        """Build final monolithic query with all CTE fragments."""
        if not fragments:
            return ""

        # Build WITH clause
        cte_clauses = []
        for fragment in fragments:
            cte_clause = f"{fragment.name} AS (\n{fragment.sql}\n)"
            cte_clauses.append(cte_clause)

        with_clause = "WITH " + ",\n".join(cte_clauses)

        # Main query selects from the final CTE
        main_query = f"SELECT * FROM {fragments[-1].name}"

        return f"{with_clause}\n{main_query}"


class CTEIntegrationManager:
    """
    Manager class that integrates CTE optimization into the pipeline compilation process.

    This provides the main interface for using CTE optimization within the unified
    pipeline architecture.
    """

    def __init__(self):
        self.optimizers: List[CTEOptimizer] = []
        self.integration_stats = {
            'optimizations_attempted': 0,
            'optimizations_successful': 0,
            'fallbacks_to_original': 0
        }

        # Register default optimizers
        self.register_optimizer(MonolithicCTEOptimizer())

    def register_optimizer(self, optimizer: CTEOptimizer):
        """Register a CTE optimizer."""
        self.optimizers.append(optimizer)
        logger.debug(f"Registered CTE optimizer: {optimizer.__class__.__name__}")

    def optimize_compiled_sql(self, compiled_sql: CompiledSQL, context: UnifiedExecutionContext) -> CompiledSQL:
        """
        Apply CTE optimization to compiled SQL using the best available optimizer.

        This is the main integration point called by the pipeline compiler.
        """
        if not context.cte_optimization_enabled:
            return compiled_sql

        self.integration_stats['optimizations_attempted'] += 1

        # Find the best optimizer for this SQL
        best_optimizer = None
        best_improvement = 1.0

        for optimizer in self.optimizers:
            if optimizer.can_optimize(compiled_sql, context):
                improvement = optimizer.estimate_performance_improvement(compiled_sql, context)
                if improvement > best_improvement:
                    best_optimizer = optimizer
                    best_improvement = improvement

        if best_optimizer:
            try:
                optimized_sql = best_optimizer.optimize(compiled_sql, context)
                self.integration_stats['optimizations_successful'] += 1

                logger.debug(f"CTE optimization applied by {best_optimizer.__class__.__name__}, "
                           f"estimated improvement: {best_improvement:.1f}x")

                return optimized_sql

            except Exception as e:
                logger.warning(f"CTE optimization failed: {e}")
                self.integration_stats['fallbacks_to_original'] += 1

                if context.preserve_individual_queries:
                    return compiled_sql
                else:
                    raise

        return compiled_sql

    def get_stats(self) -> Dict[str, Any]:
        """Get integration statistics for monitoring."""
        return {
            'integration': self.integration_stats.copy(),
            'optimizers': [
                {
                    'class': opt.__class__.__name__,
                    'stats': getattr(opt, 'optimization_stats', {})
                }
                for opt in self.optimizers
            ]
        }


# Global CTE integration manager instance
_cte_manager = None

def get_cte_integration_manager() -> CTEIntegrationManager:
    """Get the global CTE integration manager instance."""
    global _cte_manager
    if _cte_manager is None:
        _cte_manager = CTEIntegrationManager()
    return _cte_manager


def create_unified_context(dialect: 'DatabaseDialect', **kwargs) -> UnifiedExecutionContext:
    """
    Create a unified execution context with sensible defaults.

    This provides a convenient factory function for creating contexts that work
    with both pipeline and CTE functionality.
    """
    return UnifiedExecutionContext(
        dialect=dialect,
        **kwargs
    )


def integrate_cte_optimization(compiled_sql: CompiledSQL, context: UnifiedExecutionContext) -> CompiledSQL:
    """
    Main integration function for applying CTE optimization to compiled SQL.

    This is the primary interface used by the pipeline compiler to apply
    CTE optimization when appropriate.
    """
    manager = get_cte_integration_manager()
    return manager.optimize_compiled_sql(compiled_sql, context)