"""
Core immutable pipeline abstractions for FHIRPath SQL generation.

This module provides the fundamental building blocks for the pipeline architecture:
- Immutable state containers that prevent context propagation bugs
- Pipeline operations that compose without side effects  
- Execution contexts that carry dialect and configuration information
- Result containers that encapsulate SQL compilation results

Design Principles:
1. Immutability: All state objects are frozen dataclasses
2. Composability: Operations combine through pure functions
3. Context Flow: Execution context flows through pipeline unchanged
4. Dialect Agnostic: Core abstractions work with any database dialect
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace, field
from typing import Generic, TypeVar, List, Optional, Dict, Any, Union
from enum import Enum
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T')

class ContextMode(Enum):
    """
    Execution context modes that determine SQL generation strategy.
    
    This addresses the core issue where forEach operations fail because
    the generator doesn't know whether to generate single-value extraction
    or collection processing logic.
    """
    SINGLE_VALUE = "single"      # Extract single value: json_extract(resource, '$.name')
    COLLECTION = "collection"    # Process collection: json_each(resource, '$.name') 
    WHERE_CLAUSE = "where"       # Boolean for WHERE: COALESCE(json_extract(...) IS NOT NULL, FALSE)
    SELECT_CLAUSE = "select"     # Value for SELECT: json_extract(...) or json_group_array(...)

@dataclass(frozen=True)
class SQLState:
    """
    Immutable SQL generation state that flows through pipeline operations.
    
    This replaces the mutable state in the current SQLGenerator that causes
    context propagation issues. Every operation returns a new SQLState
    without modifying the input.
    
    Attributes:
        base_table: Current table/CTE reference for FROM clause
        json_column: Name of JSON column (usually 'resource')
        sql_fragment: Current SQL expression being built
        ctes: List of CTE definitions that have been created
        lateral_joins: PostgreSQL LATERAL JOIN clauses
        context_mode: Current execution context (single/collection/where/select)
        resource_type: Current FHIR resource type being processed
        is_collection: Whether current result represents a collection
        path_context: Current JSONPath context for nested operations
        variable_bindings: Variable bindings for forEach $this contexts
    """
    base_table: str                                    # e.g., "fhir_resources" or "cte_1"
    json_column: str                                   # e.g., "resource"
    sql_fragment: str                                  # Current SQL expression
    ctes: List[str] = field(default_factory=list)    # CTE SQL definitions
    lateral_joins: List[str] = field(default_factory=list)  # PostgreSQL LATERAL JOINs  
    context_mode: ContextMode = ContextMode.SINGLE_VALUE
    resource_type: Optional[str] = None               # e.g., "Patient", "Observation"
    is_collection: bool = False                       # True if result is array/collection
    path_context: str = "$"                          # JSONPath context (e.g., "$", "$.name")
    variable_bindings: Dict[str, str] = field(default_factory=dict)  # e.g., {"this": "current_element"}
    
    def evolve(self, **changes) -> 'SQLState':
        """
        Create new state with specified changes.
        
        This is the core immutability method that prevents the context
        propagation bugs in the current implementation.
        """
        return replace(self, **changes)
    
    def with_context_mode(self, mode: ContextMode) -> 'SQLState':
        """Create new state with different context mode."""
        return self.evolve(context_mode=mode)
    
    def with_collection(self, is_collection: bool = True) -> 'SQLState':
        """Create new state with collection flag."""
        return self.evolve(is_collection=is_collection)
    
    def with_path_context(self, path: str) -> 'SQLState':
        """Create new state with updated path context."""
        return self.evolve(path_context=path)
    
    def with_variable_binding(self, var_name: str, var_value: str) -> 'SQLState':
        """Create new state with additional variable binding."""
        new_bindings = dict(self.variable_bindings)
        new_bindings[var_name] = var_value
        return self.evolve(variable_bindings=new_bindings)
    
    def get_effective_base(self) -> str:
        """Get the effective base expression for JSON operations."""
        if self.variable_bindings.get("this"):
            return self.variable_bindings["this"]
        return f"{self.base_table}.{self.json_column}"
    
    @classmethod
    def create_default(cls) -> 'SQLState':
        """Create a default SQLState for testing and analysis purposes."""
        return cls(
            base_table='default_table',
            json_column='resource',
            sql_fragment='default_table.resource',
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )

@dataclass(frozen=True)
class ExecutionContext:
    """
    Immutable execution context that flows through pipeline.
    
    This carries all the configuration and services needed for
    SQL generation without being modified by operations.
    """
    dialect: 'DatabaseDialect'                       # Database dialect for SQL generation
    schema_manager: Optional['SchemaManager'] = None  # FHIR schema information
    terminology_client: Optional['TerminologyClient'] = None  # Terminology services
    optimization_level: int = 0                      # 0=none, 1=basic, 2=aggressive (TEMP: disabled for debugging)
    enable_cte: bool = True                         # Enable Common Table Expressions
    debug_mode: bool = False                        # Enable debug SQL comments
    max_sql_complexity: int = 1000                  # Threshold for CTE usage
    
    # Advanced Pipeline Features (TEMP: disabled for debugging)
    enable_cte_optimization: bool = False            # Enable intelligent CTE optimization
    enable_query_plan_cache: bool = False           # Enable compiled query plan caching
    cache_max_size: int = 1000                      # Maximum cache entries for query plans
    cte_reuse_threshold: int = 3                    # Minimum complexity score for CTE reuse
    enable_parallel_optimization: bool = False      # Enable parallel optimization passes
    enable_smart_indexing: bool = False             # Enable smart JSON path indexing hints
    
    def with_dialect(self, dialect: 'DatabaseDialect') -> 'ExecutionContext':
        """Create new context with different dialect."""
        return replace(self, dialect=dialect)
    
    def with_debug(self, debug: bool = True) -> 'ExecutionContext':
        """Create new context with debug mode."""
        return replace(self, debug_mode=debug)

@dataclass(frozen=True)
class CompiledSQL:
    """
    Result of pipeline compilation to SQL.
    
    This encapsulates all the SQL components that result from
    compiling a pipeline, including CTEs, JOINs, and metadata.
    """
    main_sql: str                                    # Primary SQL expression
    ctes: List[str] = field(default_factory=list)   # Common Table Expression definitions
    lateral_joins: List[str] = field(default_factory=list)  # PostgreSQL LATERAL JOIN clauses
    parameters: Dict[str, Any] = field(default_factory=dict)  # SQL parameters for prepared statements
    estimated_complexity: int = 0                   # Complexity score (0-100)
    is_collection_result: bool = False              # Whether result is a collection
    result_columns: List[str] = field(default_factory=list)  # Expected result column names
    
    def get_full_sql(self) -> str:
        """
        Get complete SQL including CTEs.
        
        Returns:
            Complete SQL ready for execution
        """
        if not self.ctes:
            return self.main_sql
        
        cte_clause = "WITH " + ",\n     ".join(self.ctes)
        return f"{cte_clause}\n{self.main_sql}"
    
    def get_complexity_score(self) -> int:
        """
        Calculate complexity score based on SQL components.
        
        Returns:
            Complexity score (0-100) for performance analysis
        """
        if self.estimated_complexity > 0:
            return self.estimated_complexity
        
        # Calculate based on components
        score = 0
        score += len(self.ctes) * 10           # Each CTE adds complexity
        score += len(self.lateral_joins) * 15  # LATERAL JOINs are expensive
        score += min(len(self.main_sql) // 100, 20)  # Length factor
        
        if 'json_each' in self.main_sql.lower():
            score += 25  # JSON iteration is expensive
        if 'recursive' in self.main_sql.lower():
            score += 40  # Recursive CTEs are very expensive
        
        return min(score, 100)

class PipelineOperation(ABC, Generic[T]):
    """
    Abstract base class for all pipeline operations.
    
    This is the core abstraction that replaces the monolithic
    SQLGenerator.visit() method with composable, testable operations.
    
    Design principles:
    1. Pure functions - no side effects
    2. Immutable inputs and outputs
    3. Composable - operations can be chained
    4. Testable - each operation can be tested in isolation
    5. Dialect-aware - can optimize for specific databases
    """
    
    @abstractmethod
    def execute(self, input_state: T, context: ExecutionContext) -> T:
        """
        Execute this operation on the input state.
        
        Args:
            input_state: Immutable input state
            context: Execution context with dialect and configuration
            
        Returns:
            New state with operation applied (input unchanged)
        """
        pass
    
    @abstractmethod
    def optimize_for_dialect(self, dialect: 'DatabaseDialect') -> 'PipelineOperation[T]':
        """
        Return dialect-optimized version of this operation.
        
        This enables database-specific optimizations while keeping
        the core operation logic dialect-agnostic.
        
        Args:
            dialect: Target database dialect
            
        Returns:
            Optimized operation for the dialect
        """
        pass
    
    @abstractmethod
    def get_operation_name(self) -> str:
        """
        Return human-readable operation name for debugging.
        
        Returns:
            Operation name for logs and debugging
        """
        pass
    
    def validate_preconditions(self, input_state: T, context: ExecutionContext) -> None:
        """
        Validate that operation can be executed with given state.
        
        Override this method to add operation-specific validation.
        Base implementation does nothing.
        
        Args:
            input_state: Input state to validate
            context: Execution context
            
        Raises:
            ValueError: If preconditions are not met
        """
        pass
    
    def estimate_complexity(self, input_state: T, context: ExecutionContext) -> int:
        """
        Estimate complexity of executing this operation.
        
        Used for CTE optimization decisions. Override to provide
        operation-specific complexity estimates.
        
        Args:
            input_state: Input state
            context: Execution context
            
        Returns:
            Complexity estimate (0-10, where 5+ suggests CTE usage)
        """
        return 5  # Default medium complexity
    
    def get_debug_info(self) -> Dict[str, Any]:
        """
        Return debugging information about this operation.
        
        Returns:
            Dictionary with debugging information
        """
        return {
            'operation_type': self.__class__.__name__,
            'operation_name': self.get_operation_name(),
        }
    
    def __str__(self) -> str:
        """String representation for debugging."""
        return self.get_operation_name()
    
    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"{self.__class__.__name__}({self.get_operation_name()})"

class PipelineError(Exception):
    """Base exception for pipeline operations."""
    
    def __init__(self, message: str, operation: Optional[PipelineOperation] = None, 
                 context: Optional[ExecutionContext] = None):
        super().__init__(message)
        self.operation = operation
        self.context = context

class PipelineValidationError(PipelineError):
    """Raised when pipeline validation fails."""
    pass

class PipelineCompilationError(PipelineError):
    """Raised when pipeline compilation fails."""
    pass

class PipelineOptimizationError(PipelineError):
    """Raised when pipeline optimization fails."""
    pass

# Utility functions for common operations

def ensure_collection_context(state: SQLState, dialect: 'DatabaseDialect') -> SQLState:
    """
    Ensure state is in collection context.
    
    This utility function handles the common pattern of ensuring
    a state represents a collection for operations like forEach.
    
    Args:
        state: Input SQL state
        dialect: Database dialect for collection handling
        
    Returns:
        State guaranteed to be in collection context
    """
    if state.is_collection:
        return state
    
    # Convert single value to collection
    if dialect.name.upper() == 'DUCKDB':
        collection_sql = f"""
        CASE 
            WHEN {state.sql_fragment} IS NOT NULL 
            THEN json_array({state.sql_fragment})
            ELSE json_array()
        END
        """
    else:  # PostgreSQL
        collection_sql = f"""
        CASE 
            WHEN {state.sql_fragment} IS NOT NULL 
            THEN jsonb_build_array({state.sql_fragment})
            ELSE '[]'::jsonb
        END
        """
    
    return state.evolve(
        sql_fragment=collection_sql,
        is_collection=True,
        context_mode=ContextMode.COLLECTION
    )

def create_cte_name(operation_name: str, unique_id: Optional[int] = None) -> str:
    """
    Create standardized CTE name.
    
    Args:
        operation_name: Name of operation creating CTE
        unique_id: Optional unique identifier
        
    Returns:
        Standardized CTE name
    """
    base_name = f"pipeline_{operation_name.lower().replace(' ', '_')}"
    if unique_id is not None:
        base_name += f"_{unique_id}"
    return base_name

def calculate_sql_complexity(sql: str) -> int:
    """
    Calculate complexity score for SQL expression.
    
    Args:
        sql: SQL expression to analyze
        
    Returns:
        Complexity score (0-10)
    """
    if not sql:
        return 0
    
    score = 0
    sql_lower = sql.lower()
    
    # Base complexity from length
    score += min(len(sql) // 200, 3)
    
    # Add complexity for expensive operations
    expensive_ops = {
        'select': 0.5,
        'json_each': 2,
        'json_group_array': 1.5,
        'recursive': 3,
        'lateral': 2,
        'case when': 1,
        'union': 1.5
    }
    
    for op, weight in expensive_ops.items():
        count = sql_lower.count(op)
        score += count * weight
    
    return min(int(score), 10)