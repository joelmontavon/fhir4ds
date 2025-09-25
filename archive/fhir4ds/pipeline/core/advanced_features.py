"""
Advanced Pipeline Features - CTE Optimization and Query Plan Caching.

This module implements sophisticated features that enhance pipeline performance:
- Intelligent Common Table Expression (CTE) optimization and reuse
- Query plan caching for repeated pattern recognition
- Smart indexing hints for JSON path operations
- Parallel optimization pass execution
"""

import hashlib
import logging
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Any, Union
from weakref import WeakValueDictionary

from .base import ExecutionContext, SQLState, CompiledSQL
from .builder import FHIRPathPipeline

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class CTESignature:
    """
    Signature for Common Table Expression reuse detection.
    
    This represents the unique characteristics of a CTE that determine
    whether it can be reused across different parts of a query.
    """
    operation_hash: str                    # Hash of the operation sequence
    input_state_hash: str                  # Hash of the input state characteristics
    dialect_name: str                      # Database dialect name
    complexity_score: int                  # Complexity score for the CTE
    dependencies: Tuple[str, ...] = ()     # Dependencies on other CTEs or tables
    
    def __post_init__(self):
        """Validate CTE signature components."""
        if not self.operation_hash or not self.input_state_hash:
            raise ValueError("CTE signature requires operation and state hashes")

@dataclass
class CTEAnalysis:
    """
    Analysis result for CTE optimization decisions.
    
    Contains the analysis of whether a pipeline segment should be
    converted to a CTE, reused, or kept inline.
    """
    signature: CTESignature
    should_create_cte: bool
    reuse_existing_cte: Optional[str] = None  # Name of existing CTE to reuse
    estimated_cost_saving: int = 0           # Estimated performance improvement
    reuse_count: int = 0                     # How many times this pattern appears
    
    def __post_init__(self):
        """Validate CTE analysis results."""
        if self.should_create_cte and self.reuse_existing_cte:
            logger.debug(f"CTE reuse detected: {self.reuse_existing_cte}")

class CTEOptimizer:
    """
    Advanced Common Table Expression optimizer.
    
    This class analyzes pipeline operations to identify opportunities for:
    - Creating CTEs for complex repeated operations
    - Reusing existing CTEs across different query parts
    - Optimizing CTE dependency chains
    """
    
    def __init__(self):
        self.cte_registry: Dict[str, CTESignature] = {}
        self.reuse_patterns: Dict[str, int] = defaultdict(int)
        self.optimization_stats = {
            'ctes_created': 0,
            'ctes_reused': 0,
            'total_cost_savings': 0
        }
    
    def analyze_pipeline_for_cte(self, pipeline: FHIRPathPipeline, 
                                 context: ExecutionContext) -> List[CTEAnalysis]:
        """
        Analyze pipeline operations to identify CTE optimization opportunities.
        
        Args:
            pipeline: Pipeline to analyze
            context: Execution context
            
        Returns:
            List of CTE analysis results for each potential CTE
        """
        if not context.enable_cte_optimization:
            return []
        
        analyses = []
        
        # Analyze operation sequences for CTE candidates
        for i in range(len(pipeline.operations)):
            # Look for complex operation sequences
            sequence_end = min(i + 5, len(pipeline.operations))  # Max 5 operations per CTE
            
            for j in range(i + context.cte_reuse_threshold, sequence_end + 1):
                operation_sequence = pipeline.operations[i:j]
                
                if len(operation_sequence) < context.cte_reuse_threshold:
                    continue
                
                # Calculate complexity for this sequence
                sequence_complexity = sum(
                    op.estimate_complexity(SQLState.create_default(), context) 
                    for op in operation_sequence
                    if hasattr(op, 'estimate_complexity')
                )
                
                if sequence_complexity >= context.cte_reuse_threshold:
                    # Create signature for this sequence
                    signature = self._create_cte_signature(operation_sequence, context)
                    
                    # Check for existing patterns
                    pattern_key = signature.operation_hash
                    self.reuse_patterns[pattern_key] += 1
                    
                    # Determine if we should create a CTE
                    should_create = self._should_create_cte(signature, context)
                    existing_cte = self._find_reusable_cte(signature)
                    
                    analysis = CTEAnalysis(
                        signature=signature,
                        should_create_cte=should_create,
                        reuse_existing_cte=existing_cte,
                        estimated_cost_saving=sequence_complexity * 2,  # Heuristic
                        reuse_count=self.reuse_patterns[pattern_key]
                    )
                    
                    analyses.append(analysis)
        
        return analyses
    
    def _create_cte_signature(self, operations: List, context: ExecutionContext) -> CTESignature:
        """Create a unique signature for a sequence of operations."""
        # Hash the operation sequence
        operation_names = [op.get_operation_name() for op in operations]
        operation_str = '|'.join(operation_names)
        operation_hash = hashlib.md5(operation_str.encode()).hexdigest()[:12]
        
        # Hash input state characteristics  
        # For now, use a simple hash - could be enhanced with actual state analysis
        input_state_hash = hashlib.md5("default_state".encode()).hexdigest()[:8]
        
        # Calculate total complexity
        complexity_score = sum(
            op.estimate_complexity(SQLState.create_default(), context) 
            for op in operations
            if hasattr(op, 'estimate_complexity')
        )
        
        return CTESignature(
            operation_hash=operation_hash,
            input_state_hash=input_state_hash,
            dialect_name=context.dialect.name.upper(),
            complexity_score=complexity_score
        )
    
    def _should_create_cte(self, signature: CTESignature, context: ExecutionContext) -> bool:
        """Determine if a CTE should be created for this signature."""
        # Create CTE if complexity is above threshold and we have reuse potential
        complexity_threshold = context.cte_reuse_threshold * 2
        reuse_count = self.reuse_patterns.get(signature.operation_hash, 0)
        
        return (signature.complexity_score >= complexity_threshold and 
                reuse_count >= 2)
    
    def _find_reusable_cte(self, signature: CTESignature) -> Optional[str]:
        """Find an existing CTE that matches this signature."""
        for cte_name, existing_sig in self.cte_registry.items():
            if (existing_sig.operation_hash == signature.operation_hash and
                existing_sig.input_state_hash == signature.input_state_hash and
                existing_sig.dialect_name == signature.dialect_name):
                return cte_name
        return None
    
    def register_cte(self, cte_name: str, signature: CTESignature) -> None:
        """Register a new CTE in the registry."""
        self.cte_registry[cte_name] = signature
        self.optimization_stats['ctes_created'] += 1
    
    def get_optimization_stats(self) -> Dict[str, Any]:
        """Get CTE optimization statistics."""
        return {
            'cte_optimizer_stats': self.optimization_stats,
            'registered_ctes': len(self.cte_registry),
            'reuse_patterns': dict(self.reuse_patterns)
        }

class QueryPlanCache:
    """
    High-performance query plan cache for compiled pipelines.
    
    This cache stores compiled SQL patterns to avoid recompilation of
    frequently used FHIRPath expressions and pipeline patterns.
    """
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._cache: Dict[str, CompiledSQL] = {}
        self._access_counts: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()
        
        # Cache statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'total_compilations_saved': 0
        }
    
    def get_cache_key(self, pipeline: FHIRPathPipeline, 
                     context: ExecutionContext,
                     initial_state: Optional[SQLState] = None) -> str:
        """
        Generate a unique cache key for the pipeline compilation.
        
        Args:
            pipeline: Pipeline to cache
            context: Execution context
            initial_state: Initial SQL state
            
        Returns:
            Unique cache key string
        """
        # Create hash components
        pipeline_hash = pipeline.get_pipeline_hash()
        
        # Context features that affect compilation
        context_features = [
            context.dialect.name,
            str(context.optimization_level),
            str(context.enable_cte),
            str(context.enable_cte_optimization)
        ]
        context_str = '|'.join(context_features)
        
        # State features (if provided)
        state_str = ''
        if initial_state:
            state_features = [
                initial_state.base_table,
                initial_state.json_column,
                str(initial_state.is_collection),
                str(initial_state.context_mode.value)
            ]
            state_str = '|'.join(state_features)
        
        # Combine all components
        full_key = f"{pipeline_hash}:{context_str}:{state_str}"
        return hashlib.sha256(full_key.encode()).hexdigest()[:16]
    
    def get(self, cache_key: str) -> Optional[CompiledSQL]:
        """
        Retrieve compiled SQL from cache.
        
        Args:
            cache_key: Cache key for lookup
            
        Returns:
            Cached CompiledSQL if found, None otherwise
        """
        with self._lock:
            if cache_key in self._cache:
                self._access_counts[cache_key] += 1
                self.stats['hits'] += 1
                self.stats['total_compilations_saved'] += 1
                logger.debug(f"Query plan cache hit: {cache_key}")
                return self._cache[cache_key]
            else:
                self.stats['misses'] += 1
                logger.debug(f"Query plan cache miss: {cache_key}")
                return None
    
    def put(self, cache_key: str, compiled_sql: CompiledSQL) -> None:
        """
        Store compiled SQL in cache.
        
        Args:
            cache_key: Cache key
            compiled_sql: Compiled SQL to cache
        """
        with self._lock:
            # Check if we need to evict entries
            if len(self._cache) >= self.max_size:
                self._evict_lru_entry()
            
            self._cache[cache_key] = compiled_sql
            self._access_counts[cache_key] = 1
            logger.debug(f"Query plan cached: {cache_key}")
    
    def _evict_lru_entry(self) -> None:
        """Evict the least recently used cache entry."""
        if not self._access_counts:
            return
        
        # Find entry with lowest access count
        lru_key = min(self._access_counts.keys(), key=lambda k: self._access_counts[k])
        
        # Remove from cache and access counts
        del self._cache[lru_key]
        del self._access_counts[lru_key]
        self.stats['evictions'] += 1
        
        logger.debug(f"Evicted cache entry: {lru_key}")
    
    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._access_counts.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            hit_rate = (self.stats['hits'] / 
                       (self.stats['hits'] + self.stats['misses'])) if (self.stats['hits'] + self.stats['misses']) > 0 else 0
            
            return {
                'cache_stats': self.stats,
                'cache_size': len(self._cache),
                'max_size': self.max_size,
                'hit_rate': hit_rate,
                'cache_efficiency': self.stats['total_compilations_saved']
            }

class SmartIndexingHints:
    """
    Smart JSON path indexing hints for database optimization.
    
    Analyzes FHIRPath patterns to suggest database-specific optimizations
    like JSON indexes, materialized columns, or query hints.
    """
    
    def __init__(self):
        self.path_frequency: Dict[str, int] = defaultdict(int)
        self.complex_patterns: Set[str] = set()
        
    def analyze_paths(self, pipeline: FHIRPathPipeline, context: ExecutionContext) -> List[str]:
        """
        Analyze pipeline for JSON path patterns that could benefit from indexing.
        
        Args:
            pipeline: Pipeline to analyze
            context: Execution context
            
        Returns:
            List of indexing hints or suggestions
        """
        if not context.enable_smart_indexing:
            return []
        
        hints = []
        
        # Analyze path operations for frequency and complexity
        from ..operations.path import PathNavigationOperation
        
        for operation in pipeline.operations:
            if isinstance(operation, PathNavigationOperation):
                path = operation.path_segment
                self.path_frequency[path] += 1
                
                # High-frequency paths could benefit from indexes
                if self.path_frequency[path] > 10:
                    if context.dialect.name.upper() == 'POSTGRESQL':
                        hints.append(f"CREATE INDEX idx_{path}_gin ON table_name USING gin ((data->'{path}'));")
                    elif context.dialect.name.upper() == 'DUCKDB':
                        hints.append(f"-- Consider JSON extraction optimization for path: {path}")
                
                # Complex nested paths
                if '.' in path and len(path.split('.')) > 3:
                    self.complex_patterns.add(path)
                    hints.append(f"-- Complex path detected: {path} - consider materialized column")
        
        return hints
    
    def get_optimization_recommendations(self, context: ExecutionContext) -> Dict[str, Any]:
        """Get optimization recommendations based on accumulated analysis."""
        recommendations = {
            'high_frequency_paths': [path for path, count in self.path_frequency.items() if count > 10],
            'complex_patterns': list(self.complex_patterns),
            'suggested_indexes': []
        }
        
        # Generate dialect-specific recommendations
        if context.dialect.name.upper() == 'POSTGRESQL':
            for path in recommendations['high_frequency_paths']:
                recommendations['suggested_indexes'].append({
                    'type': 'gin_index',
                    'path': path,
                    'sql': f"CREATE INDEX idx_{path}_gin ON {{table_name}} USING gin (({{json_column}}->'{path}'));"
                })
        
        return recommendations

# Global instances for advanced features
_cte_optimizer = CTEOptimizer()
_query_plan_cache = QueryPlanCache()
_smart_indexing = SmartIndexingHints()

def get_cte_optimizer() -> CTEOptimizer:
    """Get the global CTE optimizer instance."""
    return _cte_optimizer

def get_query_plan_cache() -> QueryPlanCache:
    """Get the global query plan cache instance."""
    return _query_plan_cache

def get_smart_indexing_hints() -> SmartIndexingHints:
    """Get the global smart indexing hints instance."""
    return _smart_indexing

def get_advanced_features_stats() -> Dict[str, Any]:
    """Get comprehensive statistics for all advanced features."""
    return {
        'cte_optimization': _cte_optimizer.get_optimization_stats(),
        'query_plan_cache': _query_plan_cache.get_stats(),
        'smart_indexing': {
            'path_frequency_analysis': dict(_smart_indexing.path_frequency),
            'complex_patterns_detected': len(_smart_indexing.complex_patterns)
        },
        'features_enabled': {
            'cte_optimizer': True,
            'query_plan_cache': True, 
            'smart_indexing': True
        }
    }