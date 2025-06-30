"""
Batch Processing Module

Provides parallel execution and progress monitoring for multiple FHIR ViewDefinitions.
Includes utilities for:
- Parallel query execution with thread/process pools
- Progress monitoring with real-time updates
- Error handling and recovery
- Result aggregation and analysis
"""

import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Union, Callable, Iterator
from dataclasses import dataclass
from datetime import datetime

# Optional imports for progress display
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    """
    Result of a batch processing operation.
    
    Attributes:
        view_definition: The ViewDefinition that was executed
        result: The query result (if successful)
        error: Error information (if failed)
        execution_time: Time taken to execute (in seconds)
        timestamp: When the execution completed
        index: Position in the original batch
    """
    view_definition: Dict[str, Any]
    result: Any = None
    error: Optional[str] = None
    execution_time: float = 0.0
    timestamp: str = ""
    index: int = 0
    
    @property
    def success(self) -> bool:
        """Whether this batch item executed successfully."""
        return self.error is None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'index': self.index,
            'success': self.success,
            'execution_time': self.execution_time,
            'timestamp': self.timestamp,
            'error': self.error,
            'view_definition': self.view_definition,
            'has_result': self.result is not None
        }


@dataclass
class BatchSummary:
    """
    Summary of batch processing results.
    
    Attributes:
        total_queries: Total number of queries in the batch
        successful_queries: Number of successful queries
        failed_queries: Number of failed queries
        total_time: Total execution time for the batch
        average_time: Average execution time per query
        throughput: Queries per second
        errors: List of error messages from failed queries
    """
    total_queries: int
    successful_queries: int
    failed_queries: int
    total_time: float
    average_time: float
    throughput: float
    errors: List[str]
    
    @property
    def success_rate(self) -> float:
        """Success rate as a percentage."""
        return (self.successful_queries / self.total_queries * 100) if self.total_queries > 0 else 0.0


class BatchProcessor:
    """
    Handles batch processing of FHIR ViewDefinitions with parallel execution and progress monitoring.
    
    Provides functionality for:
    - Parallel execution using thread pools
    - Progress monitoring with optional tqdm integration
    - Error handling and recovery
    - Performance analysis and reporting
    """
    
    def __init__(self, database_connection, max_workers: int = 4, 
                 show_progress: bool = True, timeout: Optional[float] = None):
        """
        Initialize batch processor.
        
        Args:
            database_connection: Connected database instance (ConnectedDatabase)
            max_workers: Maximum number of parallel workers
            show_progress: Whether to show progress bar
            timeout: Optional timeout for individual queries (seconds)
        """
        self.database = database_connection
        self.max_workers = max_workers
        self.show_progress = show_progress
        self.timeout = timeout
        
        # Track batch statistics
        self.total_batches_processed = 0
        self.total_queries_executed = 0
        self.total_processing_time = 0.0
    
    def execute_batch(self, view_definitions: List[Dict[str, Any]], 
                     parallel: bool = True) -> List[BatchResult]:
        """
        Execute multiple ViewDefinitions in batch.
        
        Args:
            view_definitions: List of FHIR ViewDefinition dictionaries
            parallel: Whether to execute in parallel (True) or sequentially (False)
            
        Returns:
            List of BatchResult objects with execution results
            
        Example:
            >>> processor = BatchProcessor(db)
            >>> results = processor.execute_batch([view1, view2, view3])
            >>> successful = [r for r in results if r.success]
        """
        if not view_definitions:
            return []
        
        start_time = time.time()
        batch_start = datetime.now()
        
        logger.info(f"Starting batch execution: {len(view_definitions)} queries, parallel={parallel}")
        
        if parallel and self.max_workers > 1:
            results = self._execute_parallel(view_definitions)
        else:
            results = self._execute_sequential(view_definitions)
        
        # Update statistics
        total_time = time.time() - start_time
        self.total_batches_processed += 1
        self.total_queries_executed += len(view_definitions)
        self.total_processing_time += total_time
        
        # Log summary
        successful = sum(1 for r in results if r.success)
        logger.info(f"Batch completed: {successful}/{len(view_definitions)} successful, {total_time:.2f}s total")
        
        return results
    
    def _execute_parallel(self, view_definitions: List[Dict[str, Any]]) -> List[BatchResult]:
        """Execute ViewDefinitions in parallel using ThreadPoolExecutor."""
        results = [None] * len(view_definitions)  # Pre-allocate to maintain order
        
        # Setup progress bar if available and requested
        progress = None
        if self.show_progress and TQDM_AVAILABLE:
            progress = tqdm(total=len(view_definitions), desc="Executing queries", 
                          unit="query", leave=True)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_index = {}
            for i, view_def in enumerate(view_definitions):
                future = executor.submit(self._execute_single, view_def, i)
                future_to_index[future] = i
            
            # Collect results as they complete
            for future in as_completed(future_to_index, timeout=self.timeout):
                index = future_to_index[future]
                try:
                    result = future.result()
                    results[index] = result
                except Exception as e:
                    # Create error result
                    results[index] = BatchResult(
                        view_definition=view_definitions[index],
                        error=str(e),
                        timestamp=datetime.now().isoformat(),
                        index=index
                    )
                
                # Update progress
                if progress:
                    progress.update(1)
        
        if progress:
            progress.close()
        
        return results
    
    def _execute_sequential(self, view_definitions: List[Dict[str, Any]]) -> List[BatchResult]:
        """Execute ViewDefinitions sequentially."""
        results = []
        
        # Setup progress bar if available and requested
        iterable = view_definitions
        if self.show_progress and TQDM_AVAILABLE:
            iterable = tqdm(view_definitions, desc="Executing queries", unit="query")
        
        for i, view_def in enumerate(iterable):
            result = self._execute_single(view_def, i)
            results.append(result)
        
        return results
    
    def _execute_single(self, view_definition: Dict[str, Any], index: int) -> BatchResult:
        """Execute a single ViewDefinition and return BatchResult."""
        start_time = time.time()
        timestamp = datetime.now().isoformat()
        
        try:
            # Execute the ViewDefinition
            result = self.database.execute(view_definition)
            execution_time = time.time() - start_time
            
            return BatchResult(
                view_definition=view_definition,
                result=result,
                execution_time=execution_time,
                timestamp=timestamp,
                index=index
            )
        
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Query {index} failed: {str(e)}")
            
            return BatchResult(
                view_definition=view_definition,
                error=str(e),
                execution_time=execution_time,
                timestamp=timestamp,
                index=index
            )
    
    def execute_and_export_batch(self, view_definitions: List[Dict[str, Any]], 
                                output_path: str, format: str = "excel",
                                sheet_names: Optional[List[str]] = None,
                                parallel: bool = True) -> BatchSummary:
        """
        Execute batch and immediately export results to specified format.
        
        Args:
            view_definitions: List of FHIR ViewDefinition dictionaries  
            output_path: Path for output file
            format: Output format ('excel', 'json', 'csv')
            sheet_names: Optional sheet names for Excel export
            parallel: Whether to execute in parallel
            
        Returns:
            BatchSummary with execution statistics
            
        Example:
            >>> summary = processor.execute_and_export_batch(
            ...     [view1, view2], "report.xlsx", format="excel"
            ... )
        """
        # Execute batch
        results = self.execute_batch(view_definitions, parallel=parallel)
        
        # Filter successful results for export
        successful_results = [r.result for r in results if r.success]
        
        if not successful_results:
            logger.warning("No successful results to export")
            return self.analyze_batch(results)
        
        # Export based on format
        try:
            if format.lower() == "excel":
                self.database.execute_to_excel(successful_results, output_path, sheet_names)
            elif format.lower() == "json":
                # Export each result to a separate JSON file or combine
                if len(successful_results) == 1:
                    self.database.execute_to_json(view_definitions[0], output_path)
                else:
                    # For multiple results, create combined JSON
                    combined_data = []
                    for i, result in enumerate(successful_results):
                        json_str = self.database.execute_to_json(view_definitions[i])
                        combined_data.append(json.loads(json_str))
                    
                    with open(output_path, 'w') as f:
                        json.dump(combined_data, f, indent=2)
            elif format.lower() == "csv":
                # For CSV, only export first successful result
                if successful_results:
                    self.database.execute_to_csv(view_definitions[0], output_path)
            else:
                raise ValueError(f"Unsupported export format: {format}")
            
            logger.info(f"Batch results exported to {output_path}")
        
        except Exception as e:
            logger.error(f"Failed to export batch results: {e}")
        
        return self.analyze_batch(results)
    
    def analyze_batch(self, results: List[BatchResult]) -> BatchSummary:
        """
        Analyze batch execution results and generate summary statistics.
        
        Args:
            results: List of BatchResult objects
            
        Returns:
            BatchSummary with detailed statistics
        """
        if not results:
            return BatchSummary(0, 0, 0, 0.0, 0.0, 0.0, [])
        
        total_queries = len(results)
        successful_queries = sum(1 for r in results if r.success)
        failed_queries = total_queries - successful_queries
        total_time = sum(r.execution_time for r in results)
        average_time = total_time / total_queries if total_queries > 0 else 0.0
        throughput = total_queries / total_time if total_time > 0 else 0.0
        errors = [r.error for r in results if r.error]
        
        return BatchSummary(
            total_queries=total_queries,
            successful_queries=successful_queries,
            failed_queries=failed_queries,
            total_time=total_time,
            average_time=average_time,
            throughput=throughput,
            errors=errors
        )
    
    def get_processor_stats(self) -> Dict[str, Any]:
        """
        Get overall processor statistics across all batches.
        
        Returns:
            Dictionary with processor performance statistics
        """
        avg_batch_time = (self.total_processing_time / self.total_batches_processed 
                         if self.total_batches_processed > 0 else 0.0)
        avg_query_time = (self.total_processing_time / self.total_queries_executed
                         if self.total_queries_executed > 0 else 0.0)
        
        return {
            'total_batches_processed': self.total_batches_processed,
            'total_queries_executed': self.total_queries_executed,
            'total_processing_time': self.total_processing_time,
            'average_batch_time': avg_batch_time,
            'average_query_time': avg_query_time,
            'max_workers': self.max_workers,
            'queries_per_second': (self.total_queries_executed / self.total_processing_time
                                 if self.total_processing_time > 0 else 0.0)
        }


def create_batch_processor(database_connection, max_workers: int = 4, 
                          show_progress: bool = True) -> BatchProcessor:
    """
    Factory function to create a BatchProcessor instance.
    
    Args:
        database_connection: Connected database instance
        max_workers: Maximum number of parallel workers
        show_progress: Whether to show progress indicators
        
    Returns:
        Configured BatchProcessor instance
        
    Example:
        >>> from fhir4ds.helpers import QuickConnect, create_batch_processor
        >>> db = QuickConnect.memory()
        >>> processor = create_batch_processor(db, max_workers=8)
        >>> results = processor.execute_batch(view_definitions)
    """
    return BatchProcessor(database_connection, max_workers, show_progress)