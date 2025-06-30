"""
Performance Monitoring Module

Provides query timing, optimization suggestions, and performance monitoring capabilities
for FHIR4DS analytics operations. Includes profiling tools and automated optimization
recommendations.

Features:
- Query execution timing and profiling
- Performance bottleneck identification
- Optimization suggestions based on query patterns
- Resource usage monitoring
- Query plan analysis (database-specific)
- Performance regression detection

Examples:
    # Monitor single query performance
    monitor = PerformanceMonitor(db)
    result = monitor.execute_with_profiling(view_definition)
    print(f"Query took {result.execution_time:.2f}s")
    
    # Get optimization suggestions
    suggestions = monitor.analyze_performance(view_definition)
    for suggestion in suggestions.recommendations:
        print(f"- {suggestion}")
    
    # Profile multiple queries
    profiler = QueryProfiler()
    for view_def in queries:
        profiler.profile_query(view_def, db)
    
    report = profiler.generate_report()
    print(report.summary)
"""

import time
import logging
import psutil
import threading
from typing import Dict, List, Any, Optional, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import json
import statistics

# Optional imports for advanced profiling
try:
    import memory_profiler
    MEMORY_PROFILER_AVAILABLE = True
except ImportError:
    MEMORY_PROFILER_AVAILABLE = False

try:
    import cProfile
    import pstats
    CPROFILE_AVAILABLE = True
except ImportError:
    CPROFILE_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class QueryMetrics:
    """
    Performance metrics for a single query execution.
    
    Attributes:
        query_id: Unique identifier for the query
        view_definition: The ViewDefinition that was executed
        execution_time: Total execution time in seconds
        cpu_time: CPU time consumed
        memory_peak: Peak memory usage in MB
        memory_delta: Memory usage change in MB
        row_count: Number of rows returned
        database_time: Time spent in database operations
        parsing_time: Time spent parsing ViewDefinition
        sql_generation_time: Time spent generating SQL
        timestamp: When the execution started
        database_type: Type of database (duckdb, postgresql)
        success: Whether the query executed successfully
        error_message: Error message if query failed
    """
    query_id: str
    view_definition: Dict[str, Any]
    execution_time: float = 0.0
    cpu_time: float = 0.0
    memory_peak: float = 0.0
    memory_delta: float = 0.0
    row_count: int = 0
    database_time: float = 0.0
    parsing_time: float = 0.0
    sql_generation_time: float = 0.0
    timestamp: str = ""
    database_type: str = ""
    success: bool = True
    error_message: Optional[str] = None
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()
    
    @property
    def throughput(self) -> float:
        """Rows per second throughput."""
        return self.row_count / self.execution_time if self.execution_time > 0 else 0.0
    
    @property
    def efficiency_score(self) -> float:
        """Simple efficiency score (0-100) based on multiple factors."""
        if not self.success or self.execution_time <= 0:
            return 0.0
        
        # Base score from throughput (rows/second)
        throughput_score = min(self.throughput / 1000, 1.0) * 40  # Max 40 points
        
        # Memory efficiency (lower is better)
        memory_score = max(0, 30 - (self.memory_peak / 100)) if self.memory_peak > 0 else 30  # Max 30 points
        
        # Execution time efficiency (under 1 second gets full points)
        time_score = max(0, 30 - (self.execution_time - 1) * 5) if self.execution_time > 1 else 30  # Max 30 points
        
        return min(100, throughput_score + memory_score + time_score)


@dataclass
class OptimizationSuggestion:
    """
    A single optimization suggestion.
    
    Attributes:
        category: Type of optimization (index, query, schema, etc.)
        priority: Priority level (high, medium, low)
        description: Human-readable description
        technical_details: Technical implementation details
        potential_improvement: Estimated improvement percentage
        implementation_effort: Effort level (low, medium, high)
    """
    category: str
    priority: str
    description: str
    technical_details: str = ""
    potential_improvement: float = 0.0
    implementation_effort: str = "medium"


@dataclass
class OptimizationSuggestions:
    """
    Collection of optimization suggestions for a query.
    
    Attributes:
        query_metrics: The metrics that triggered these suggestions
        recommendations: List of optimization suggestions
        overall_score: Overall performance score (0-100)
        priority_recommendations: High-priority recommendations only
    """
    query_metrics: QueryMetrics
    recommendations: List[OptimizationSuggestion] = field(default_factory=list)
    overall_score: float = 0.0
    
    @property
    def priority_recommendations(self) -> List[OptimizationSuggestion]:
        """Get only high-priority recommendations."""
        return [r for r in self.recommendations if r.priority == "high"]
    
    @property
    def quick_wins(self) -> List[OptimizationSuggestion]:
        """Get recommendations with low effort but high impact."""
        return [r for r in self.recommendations 
                if r.implementation_effort == "low" and r.potential_improvement >= 20]


class PerformanceMonitor:
    """
    Monitors query performance and provides optimization suggestions.
    
    Provides comprehensive performance monitoring including execution timing,
    resource usage tracking, and automated optimization recommendations.
    """
    
    def __init__(self, database_connection, enable_profiling: bool = True,
                 track_memory: bool = True, track_cpu: bool = True):
        """
        Initialize performance monitor.
        
        Args:
            database_connection: Connected database instance
            enable_profiling: Whether to enable detailed profiling
            track_memory: Whether to track memory usage
            track_cpu: Whether to track CPU usage
        """
        self.database = database_connection
        self.enable_profiling = enable_profiling
        self.track_memory = track_memory
        self.track_cpu = track_cpu
        
        # Historical metrics storage
        self.query_history: List[QueryMetrics] = []
        self.baseline_metrics: Dict[str, float] = {}
        
        # Performance thresholds
        self.slow_query_threshold = 5.0  # seconds
        self.high_memory_threshold = 1000  # MB
        self.low_throughput_threshold = 100  # rows/second
    
    def execute_with_profiling(self, view_definition: Dict[str, Any],
                             query_id: Optional[str] = None) -> QueryMetrics:
        """
        Execute a ViewDefinition with comprehensive performance profiling.
        
        Args:
            view_definition: FHIR ViewDefinition to execute
            query_id: Optional unique identifier for the query
            
        Returns:
            QueryMetrics with detailed performance information
            
        Example:
            >>> monitor = PerformanceMonitor(db)
            >>> metrics = monitor.execute_with_profiling(patient_demographics_view)
            >>> print(f"Query completed in {metrics.execution_time:.2f}s")
        """
        if not query_id:
            query_id = f"query_{int(time.time() * 1000)}"
        
        # Initialize metrics
        metrics = QueryMetrics(
            query_id=query_id,
            view_definition=view_definition,
            database_type=getattr(self.database, 'dialect_name', 'unknown')
        )
        
        # Get baseline resource usage
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        initial_cpu_time = process.cpu_times().user + process.cpu_times().system
        
        # Track memory usage during execution
        peak_memory = initial_memory
        memory_tracker = None
        
        if self.track_memory:
            def track_memory():
                nonlocal peak_memory
                while getattr(threading.current_thread(), "track_memory", True):
                    try:
                        current_memory = process.memory_info().rss / 1024 / 1024
                        peak_memory = max(peak_memory, current_memory)
                        time.sleep(0.1)  # Check every 100ms
                    except:
                        break
            
            memory_tracker = threading.Thread(target=track_memory)
            memory_tracker.track_memory = True
            memory_tracker.daemon = True
            memory_tracker.start()
        
        # Execute with timing
        start_time = time.time()
        
        try:
            # Time different phases if possible
            parsing_start = time.time()
            # The actual parsing/validation happens inside execute()
            
            sql_gen_start = time.time()
            result = self.database.execute(view_definition)
            end_time = time.time()
            
            # Calculate metrics
            metrics.execution_time = end_time - start_time
            metrics.parsing_time = sql_gen_start - parsing_start
            metrics.sql_generation_time = end_time - sql_gen_start
            metrics.database_time = metrics.execution_time - metrics.parsing_time
            
            # Get result information
            if hasattr(result, 'fetchall'):
                # QueryResult object
                rows = result.fetchall()
                metrics.row_count = len(rows)
            elif hasattr(result, '__len__'):
                # List or similar
                metrics.row_count = len(result)
            else:
                metrics.row_count = 0
                
            metrics.success = True
            
        except Exception as e:
            metrics.success = False
            metrics.error_message = str(e)
            metrics.execution_time = time.time() - start_time
            logger.error(f"Query {query_id} failed: {e}")
        
        finally:
            # Stop memory tracking
            if memory_tracker:
                memory_tracker.track_memory = False
                memory_tracker.join(timeout=1.0)
            
            # Calculate resource usage
            final_memory = process.memory_info().rss / 1024 / 1024
            final_cpu_time = process.cpu_times().user + process.cpu_times().system
            
            metrics.memory_peak = peak_memory
            metrics.memory_delta = final_memory - initial_memory
            metrics.cpu_time = final_cpu_time - initial_cpu_time
        
        # Store in history
        self.query_history.append(metrics)
        
        # Update baselines
        self._update_baselines(metrics)
        
        return metrics
    
    def analyze_performance(self, view_definition: Dict[str, Any],
                          metrics: Optional[QueryMetrics] = None) -> OptimizationSuggestions:
        """
        Analyze query performance and generate optimization suggestions.
        
        Args:
            view_definition: FHIR ViewDefinition to analyze
            metrics: Optional pre-computed metrics (will execute if not provided)
            
        Returns:
            OptimizationSuggestions with recommendations
            
        Example:
            >>> suggestions = monitor.analyze_performance(complex_view)
            >>> for rec in suggestions.priority_recommendations:
            ...     print(f"HIGH: {rec.description}")
        """
        if metrics is None:
            metrics = self.execute_with_profiling(view_definition)
        
        suggestions = OptimizationSuggestions(
            query_metrics=metrics,
            overall_score=metrics.efficiency_score
        )
        
        # Analyze different aspects and generate suggestions
        suggestions.recommendations.extend(self._analyze_execution_time(metrics, view_definition))
        suggestions.recommendations.extend(self._analyze_memory_usage(metrics, view_definition))
        suggestions.recommendations.extend(self._analyze_query_structure(view_definition))
        suggestions.recommendations.extend(self._analyze_historical_performance(metrics))
        
        return suggestions
    
    def _analyze_execution_time(self, metrics: QueryMetrics, 
                              view_definition: Dict[str, Any]) -> List[OptimizationSuggestion]:
        """Analyze execution time and suggest improvements."""
        suggestions = []
        
        if metrics.execution_time > self.slow_query_threshold:
            suggestions.append(OptimizationSuggestion(
                category="performance",
                priority="high",
                description=f"Query is slow ({metrics.execution_time:.2f}s). Consider optimization.",
                technical_details="Review WHERE clauses, add indexes, or limit result set size",
                potential_improvement=30.0,
                implementation_effort="medium"
            ))
        
        if metrics.throughput < self.low_throughput_threshold and metrics.row_count > 1000:
            suggestions.append(OptimizationSuggestion(
                category="throughput",
                priority="medium",
                description=f"Low throughput ({metrics.throughput:.1f} rows/sec). Database may be a bottleneck.",
                technical_details="Consider database tuning, connection pooling, or parallel execution",
                potential_improvement=50.0,
                implementation_effort="high"
            ))
        
        # Check for potential forEach optimization
        select_items = view_definition.get('select', [])
        for select_item in select_items:
            if 'forEach' in select_item and metrics.execution_time > 2.0:
                suggestions.append(OptimizationSuggestion(
                    category="query_structure",
                    priority="medium",
                    description="forEach clause detected in slow query. Consider restructuring.",
                    technical_details="Use JOINs or subqueries instead of forEach when possible",
                    potential_improvement=25.0,
                    implementation_effort="medium"
                ))
        
        return suggestions
    
    def _analyze_memory_usage(self, metrics: QueryMetrics,
                            view_definition: Dict[str, Any]) -> List[OptimizationSuggestion]:
        """Analyze memory usage patterns."""
        suggestions = []
        
        if metrics.memory_peak > self.high_memory_threshold:
            suggestions.append(OptimizationSuggestion(
                category="memory",
                priority="high",
                description=f"High memory usage ({metrics.memory_peak:.1f}MB). Risk of OOM errors.",
                technical_details="Add LIMIT clauses, process in batches, or use streaming",
                potential_improvement=40.0,
                implementation_effort="low"
            ))
        
        if metrics.memory_delta > 500:  # 500MB growth
            suggestions.append(OptimizationSuggestion(
                category="memory",
                priority="medium",
                description="Significant memory growth during query execution.",
                technical_details="Check for memory leaks or inefficient data structures",
                potential_improvement=20.0,
                implementation_effort="medium"
            ))
        
        return suggestions
    
    def _analyze_query_structure(self, view_definition: Dict[str, Any]) -> List[OptimizationSuggestion]:
        """Analyze ViewDefinition structure for optimization opportunities."""
        suggestions = []
        
        # Check for missing WHERE clauses
        select_items = view_definition.get('select', [])
        has_where_clause = any('where' in item for item in select_items)
        
        if not has_where_clause:
            suggestions.append(OptimizationSuggestion(
                category="query_structure",
                priority="medium",
                description="No WHERE clause found. Query may return excessive data.",
                technical_details="Add filtering conditions to reduce result set size",
                potential_improvement=60.0,
                implementation_effort="low"
            ))
        
        # Check for complex column expressions
        for select_item in select_items:
            columns = select_item.get('column', [])
            for col in columns:
                path = col.get('path', '')
                if any(func in path for func in ['join(', 'where(', 'select(']):
                    suggestions.append(OptimizationSuggestion(
                        category="query_structure",
                        priority="low",
                        description="Complex FHIRPath expressions may impact performance.",
                        technical_details="Consider simplifying expressions or pre-processing data",
                        potential_improvement=15.0,
                        implementation_effort="medium"
                    ))
                    break
        
        # Check for potential indexing opportunities
        resource_type = view_definition.get('resource', '')
        if resource_type in ['Patient', 'Observation', 'Encounter']:
            suggestions.append(OptimizationSuggestion(
                category="indexing",
                priority="low",
                description=f"Common resource type ({resource_type}) detected. Ensure proper indexing.",
                technical_details="Create indexes on frequently queried fields like id, subject.reference",
                potential_improvement=35.0,
                implementation_effort="low"
            ))
        
        return suggestions
    
    def _analyze_historical_performance(self, metrics: QueryMetrics) -> List[OptimizationSuggestion]:
        """Compare with historical performance data."""
        suggestions = []
        
        if len(self.query_history) < 5:
            return suggestions  # Need more data for comparison
        
        # Compare with similar queries
        similar_queries = [
            m for m in self.query_history[-20:]  # Last 20 queries
            if m.database_type == metrics.database_type and m.success
            and abs(m.row_count - metrics.row_count) / max(m.row_count, 1) < 0.5  # Similar size
        ]
        
        if len(similar_queries) >= 3:
            avg_time = statistics.mean(q.execution_time for q in similar_queries)
            if metrics.execution_time > avg_time * 1.5:
                suggestions.append(OptimizationSuggestion(
                    category="regression",
                    priority="high",
                    description=f"Performance regression detected. Query 50% slower than recent average.",
                    technical_details=f"Recent average: {avg_time:.2f}s, Current: {metrics.execution_time:.2f}s",
                    potential_improvement=33.0,
                    implementation_effort="medium"
                ))
        
        return suggestions
    
    def _update_baselines(self, metrics: QueryMetrics):
        """Update baseline performance metrics."""
        if not metrics.success:
            return
        
        # Update rolling averages
        key = f"{metrics.database_type}_{metrics.row_count // 1000}k"  # Group by size
        if key not in self.baseline_metrics:
            self.baseline_metrics[key] = metrics.execution_time
        else:
            # Exponential moving average
            self.baseline_metrics[key] = (0.8 * self.baseline_metrics[key] + 
                                        0.2 * metrics.execution_time)
    
    def get_performance_summary(self, time_window: Optional[timedelta] = None) -> Dict[str, Any]:
        """
        Get performance summary for recent queries.
        
        Args:
            time_window: Optional time window for analysis (default: last 24 hours)
            
        Returns:
            Dictionary with performance statistics
        """
        if time_window is None:
            time_window = timedelta(hours=24)
        
        cutoff_time = datetime.now() - time_window
        recent_queries = [
            q for q in self.query_history
            if datetime.fromisoformat(q.timestamp) >= cutoff_time
        ]
        
        if not recent_queries:
            return {"message": "No queries in specified time window"}
        
        successful_queries = [q for q in recent_queries if q.success]
        failed_queries = [q for q in recent_queries if not q.success]
        
        summary = {
            "time_window_hours": time_window.total_seconds() / 3600,
            "total_queries": len(recent_queries),
            "successful_queries": len(successful_queries),
            "failed_queries": len(failed_queries),
            "success_rate": len(successful_queries) / len(recent_queries) * 100,
        }
        
        if successful_queries:
            execution_times = [q.execution_time for q in successful_queries]
            memory_peaks = [q.memory_peak for q in successful_queries]
            row_counts = [q.row_count for q in successful_queries]
            
            summary.update({
                "avg_execution_time": statistics.mean(execution_times),
                "median_execution_time": statistics.median(execution_times),
                "max_execution_time": max(execution_times),
                "avg_memory_peak": statistics.mean(memory_peaks),
                "max_memory_peak": max(memory_peaks),
                "total_rows_processed": sum(row_counts),
                "avg_throughput": statistics.mean(q.throughput for q in successful_queries),
                "slow_queries": len([q for q in successful_queries if q.execution_time > self.slow_query_threshold])
            })
        
        return summary


class QueryProfiler:
    """
    Advanced query profiler for batch analysis and reporting.
    
    Provides detailed profiling capabilities for multiple queries with
    comparative analysis and trend detection.
    """
    
    def __init__(self, enable_cprofile: bool = False):
        """
        Initialize query profiler.
        
        Args:
            enable_cprofile: Whether to enable Python cProfile profiling
        """
        self.enable_cprofile = enable_cprofile and CPROFILE_AVAILABLE
        self.profiles: List[QueryMetrics] = []
        self.comparative_data: Dict[str, List[float]] = defaultdict(list)
    
    def profile_query(self, view_definition: Dict[str, Any], 
                     database_connection, iterations: int = 1) -> List[QueryMetrics]:
        """
        Profile a query with multiple iterations for statistical analysis.
        
        Args:
            view_definition: FHIR ViewDefinition to profile
            database_connection: Database connection to use
            iterations: Number of times to execute the query
            
        Returns:
            List of QueryMetrics for each iteration
        """
        monitor = PerformanceMonitor(database_connection)
        iteration_results = []
        
        for i in range(iterations):
            query_id = f"profile_{int(time.time() * 1000)}_{i}"
            
            if self.enable_cprofile:
                # Profile with cProfile
                pr = cProfile.Profile()
                pr.enable()
                
                metrics = monitor.execute_with_profiling(view_definition, query_id)
                
                pr.disable()
                # Could save cProfile stats if needed
                
            else:
                metrics = monitor.execute_with_profiling(view_definition, query_id)
            
            iteration_results.append(metrics)
            self.profiles.append(metrics)
            
            # Track comparative data
            if metrics.success:
                self.comparative_data['execution_time'].append(metrics.execution_time)
                self.comparative_data['memory_peak'].append(metrics.memory_peak)
                self.comparative_data['throughput'].append(metrics.throughput)
        
        return iteration_results
    
    def generate_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive profiling report.
        
        Returns:
            Dictionary with detailed profiling analysis
        """
        if not self.profiles:
            return {"error": "No profiling data available"}
        
        successful_profiles = [p for p in self.profiles if p.success]
        
        if not successful_profiles:
            return {"error": "No successful query executions to analyze"}
        
        # Basic statistics
        execution_times = [p.execution_time for p in successful_profiles]
        memory_peaks = [p.memory_peak for p in successful_profiles]
        throughputs = [p.throughput for p in successful_profiles]
        
        report = {
            "summary": {
                "total_queries_profiled": len(self.profiles),
                "successful_executions": len(successful_profiles),
                "failure_rate": (len(self.profiles) - len(successful_profiles)) / len(self.profiles) * 100,
            },
            "performance_statistics": {
                "execution_time": {
                    "mean": statistics.mean(execution_times),
                    "median": statistics.median(execution_times),
                    "stdev": statistics.stdev(execution_times) if len(execution_times) > 1 else 0,
                    "min": min(execution_times),
                    "max": max(execution_times)
                },
                "memory_usage": {
                    "mean_peak": statistics.mean(memory_peaks),
                    "median_peak": statistics.median(memory_peaks),
                    "max_peak": max(memory_peaks)
                },
                "throughput": {
                    "mean": statistics.mean(throughputs),
                    "median": statistics.median(throughputs),
                    "max": max(throughputs)
                }
            },
            "trends": self._analyze_trends(),
            "recommendations": self._generate_profile_recommendations()
        }
        
        return report
    
    def _analyze_trends(self) -> Dict[str, Any]:
        """Analyze performance trends over time."""
        if len(self.profiles) < 5:
            return {"message": "Insufficient data for trend analysis"}
        
        # Simple trend analysis using linear regression concepts
        successful = [p for p in self.profiles if p.success]
        if len(successful) < 3:
            return {"message": "Insufficient successful executions for trend analysis"}
        
        execution_times = [p.execution_time for p in successful]
        
        # Calculate if performance is improving or degrading
        first_half = execution_times[:len(execution_times)//2]
        second_half = execution_times[len(execution_times)//2:]
        
        first_avg = statistics.mean(first_half)
        second_avg = statistics.mean(second_half)
        
        trend_direction = "improving" if second_avg < first_avg else "degrading"
        trend_magnitude = abs(second_avg - first_avg) / first_avg * 100
        
        return {
            "execution_time_trend": {
                "direction": trend_direction,
                "magnitude_percent": trend_magnitude,
                "first_half_avg": first_avg,
                "second_half_avg": second_avg
            }
        }
    
    def _generate_profile_recommendations(self) -> List[str]:
        """Generate recommendations based on profiling data."""
        recommendations = []
        
        if not self.profiles:
            return recommendations
        
        successful = [p for p in self.profiles if p.success]
        
        if len(successful) == 0:
            recommendations.append("All queries failed - check query syntax and database connection")
            return recommendations
        
        # High execution time variance
        execution_times = [p.execution_time for p in successful]
        if len(execution_times) > 1:
            stdev = statistics.stdev(execution_times)
            mean_time = statistics.mean(execution_times)
            cv = stdev / mean_time  # Coefficient of variation
            
            if cv > 0.5:  # High variance
                recommendations.append("High execution time variance detected - investigate query consistency")
        
        # Memory usage patterns
        memory_peaks = [p.memory_peak for p in successful]
        max_memory = max(memory_peaks)
        avg_memory = statistics.mean(memory_peaks)
        
        if max_memory > 1000:  # 1GB
            recommendations.append("High memory usage detected - consider query optimization or data partitioning")
        
        if max_memory > avg_memory * 2:
            recommendations.append("Inconsistent memory usage - some executions use significantly more memory")
        
        # Failure patterns
        failure_rate = (len(self.profiles) - len(successful)) / len(self.profiles)
        if failure_rate > 0.1:  # 10% failure rate
            recommendations.append(f"High failure rate ({failure_rate*100:.1f}%) - investigate query reliability")
        
        return recommendations


def benchmark_query_performance(database_connection, view_definitions: List[Dict[str, Any]],
                               iterations: int = 3) -> Dict[str, Any]:
    """
    Benchmark multiple queries and compare their performance.
    
    Args:
        database_connection: Database connection to use
        view_definitions: List of ViewDefinitions to benchmark
        iterations: Number of iterations per query
        
    Returns:
        Dictionary with comparative benchmark results
        
    Example:
        >>> queries = [patient_view, observation_view, encounter_view]
        >>> results = benchmark_query_performance(db, queries, iterations=5)
        >>> print(results['ranking'])
    """
    profiler = QueryProfiler()
    benchmark_results = {}
    
    for i, view_def in enumerate(view_definitions):
        query_name = view_def.get('id', f'query_{i}')
        logger.info(f"Benchmarking {query_name} with {iterations} iterations")
        
        results = profiler.profile_query(view_def, database_connection, iterations)
        successful_results = [r for r in results if r.success]
        
        if successful_results:
            execution_times = [r.execution_time for r in successful_results]
            benchmark_results[query_name] = {
                'mean_time': statistics.mean(execution_times),
                'median_time': statistics.median(execution_times),
                'min_time': min(execution_times),
                'max_time': max(execution_times),
                'success_rate': len(successful_results) / len(results) * 100,
                'avg_throughput': statistics.mean(r.throughput for r in successful_results),
                'avg_memory': statistics.mean(r.memory_peak for r in successful_results)
            }
        else:
            benchmark_results[query_name] = {
                'error': 'All iterations failed',
                'success_rate': 0.0
            }
    
    # Create ranking
    successful_queries = {k: v for k, v in benchmark_results.items() if 'error' not in v}
    ranking = sorted(successful_queries.items(), key=lambda x: x[1]['mean_time'])
    
    return {
        'benchmark_results': benchmark_results,
        'ranking': [(name, data['mean_time']) for name, data in ranking],
        'fastest_query': ranking[0][0] if ranking else None,
        'slowest_query': ranking[-1][0] if ranking else None,
        'total_queries_tested': len(view_definitions),
        'iterations_per_query': iterations
    }