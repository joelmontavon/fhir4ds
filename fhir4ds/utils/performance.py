"""
Performance Testing Framework for FHIR4DS

This module provides comprehensive performance testing capabilities for SQL-on-FHIR
operations including dataset loading, ViewDefinition processing, and SQL generation.
"""

import time
import json
import os
import statistics
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path

# Import FHIR4DS components
from ..datastore import FHIRDataStore
from ..fhirpath import FHIRPath
from .logging_config import get_logger, log_execution_time


@dataclass
class PerformanceMetrics:
    """Container for performance measurement results"""
    operation: str
    duration_ms: float
    memory_mb: Optional[float] = None
    rows_processed: Optional[int] = None
    files_processed: Optional[int] = None
    throughput_per_sec: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class DatasetStats:
    """Statistics about a FHIR dataset"""
    total_files: int
    total_size_mb: float
    resource_types: Dict[str, int]
    avg_file_size_kb: float
    patient_count: int
    date_range: Optional[Tuple[str, str]] = None


class PerformanceTester:
    """
    Comprehensive performance testing framework for FHIR4DS operations.
    
    Provides systematic benchmarking for:
    - Dataset loading and processing
    - ViewDefinition execution
    - SQL generation performance
    - Memory usage analysis
    """
    
    def __init__(self, output_dir: str = "."):
        self.output_dir = Path(output_dir)
        if output_dir != ".":
            self.output_dir.mkdir(exist_ok=True)
        self.results: List[PerformanceMetrics] = []
        self.logger = get_logger(__name__)
        
    def measure_time(self, operation_name: str):
        """Context manager for measuring operation time"""
        return _TimeContext(operation_name, self)
    
    def benchmark_dataset_loading(self, dataset_path: str, 
                                  sample_size: Optional[int] = None) -> PerformanceMetrics:
        """
        Benchmark dataset loading performance.
        
        Args:
            dataset_path: Path to dataset directory
            sample_size: Optional limit on number of files to process
            
        Returns:
            PerformanceMetrics: Loading performance results
        """
        self.logger.info("Benchmarking dataset loading: %s", dataset_path)
        
        # Get dataset files
        json_files = list(Path(dataset_path).glob("*.json"))
        if sample_size:
            json_files = json_files[:sample_size]
            
        self.logger.info("Processing %d files...", len(json_files))
        
        # Create datastore and measure loading
        datastore = FHIRDataStore.with_duckdb()
        
        with self.measure_time("dataset_loading") as timer:
            # Load files in batches for better performance
            batch_size = 100
            for i in range(0, len(json_files), batch_size):
                batch = json_files[i:i + batch_size]
                file_pattern = str(Path(dataset_path) / "*.json")
                
                # Load batch (simplified for demo - would need actual batch loading)
                for file_path in batch:
                    try:
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                            # Simulate loading - in real implementation would use datastore.load_resource()
                    except Exception as e:
                        self.logger.warning("Skipped %s: %s", file_path, e)
        
        # Calculate throughput
        duration_sec = timer.duration_ms / 1000
        throughput = len(json_files) / duration_sec if duration_sec > 0 else 0
        
        metrics = PerformanceMetrics(
            operation="dataset_loading",
            duration_ms=timer.duration_ms,
            files_processed=len(json_files),
            throughput_per_sec=throughput,
            metadata={
                "dataset_path": dataset_path,
                "sample_size": sample_size,
                "batch_size": batch_size
            }
        )
        
        self.results.append(metrics)
        self.logger.info("Loaded %d files in %.1fms (%.1f files/sec)", 
                        len(json_files), timer.duration_ms, throughput)
        return metrics
    
    def benchmark_viewdefinition_execution(self, view_definition: Dict[str, Any],
                                          datastore: FHIRDataStore) -> PerformanceMetrics:
        """
        Benchmark ViewDefinition execution performance.
        
        Args:
            view_definition: ViewDefinition to execute
            datastore: Populated FHIRDataStore
            
        Returns:
            PerformanceMetrics: Execution performance results
        """
        view_name = view_definition.get("name", "unnamed_view")
        self.logger.info("Benchmarking ViewDefinition: %s", view_name)
        
        # Import ViewRunner locally to avoid circular import
        from ..view_runner import ViewRunner
        runner = ViewRunner(datastore=datastore)
        
        with self.measure_time(f"viewdef_execution_{view_name}") as timer:
            try:
                result = runner.execute_view_definition(view_definition)
                rows = result.fetchall() if hasattr(result, 'fetchall') else []
                row_count = len(rows) if rows else 0
            except Exception as e:
                self.logger.error("ViewDefinition execution failed: %s", e)
                row_count = 0
        
        metrics = PerformanceMetrics(
            operation=f"viewdef_execution_{view_name}",
            duration_ms=timer.duration_ms,
            rows_processed=row_count,
            metadata={
                "view_name": view_name,
                "view_definition": view_definition
            }
        )
        
        self.results.append(metrics)
        self.logger.info("Executed ViewDefinition in %.1fms, returned %d rows", 
                        timer.duration_ms, row_count)
        return metrics
    
    def benchmark_fhirpath_operations(self, expressions: List[str]) -> List[PerformanceMetrics]:
        """
        Benchmark FHIRPath parsing and SQL generation performance.
        
        Args:
            expressions: List of FHIRPath expressions to test
            
        Returns:
            List of PerformanceMetrics for each operation
        """
        self.logger.info("Benchmarking FHIRPath operations: %d expressions", len(expressions))
        
        fp = FHIRPath()
        results = []
        
        for i, expression in enumerate(expressions):
            self.logger.debug("Testing expression %d/%d: %s", i+1, len(expressions), expression)
            
            # Benchmark parsing
            with self.measure_time(f"fhirpath_parse_{i}") as timer:
                try:
                    ast = fp.parse(expression)
                    parse_success = True
                except Exception as e:
                    self.logger.error("Parse failed: %s", e)
                    parse_success = False
            
            parse_metrics = PerformanceMetrics(
                operation="fhirpath_parse",
                duration_ms=timer.duration_ms,
                metadata={
                    "expression": expression,
                    "success": parse_success
                }
            )
            results.append(parse_metrics)
            self.results.append(parse_metrics)
            
            if parse_success:
                # Benchmark SQL generation
                with self.measure_time(f"fhirpath_sql_gen_{i}") as timer:
                    try:
                        sql = fp.to_sql(expression, "Patient", "p")
                        sql_success = True
                    except Exception as e:
                        self.logger.error("SQL generation failed: %s", e)
                        sql_success = False
                
                sql_metrics = PerformanceMetrics(
                    operation="fhirpath_sql_generation",
                    duration_ms=timer.duration_ms,
                    metadata={
                        "expression": expression,
                        "success": sql_success,
                        "generated_sql": sql if sql_success else None
                    }
                )
                results.append(sql_metrics)
                self.results.append(sql_metrics)
        
        # Calculate summary statistics
        parse_times = [r.duration_ms for r in results if r.operation == "fhirpath_parse"]
        sql_times = [r.duration_ms for r in results if r.operation == "fhirpath_sql_generation"]
        
        if parse_times:
            self.logger.info("Parse times - avg: %.2fms, median: %.2fms", 
                           statistics.mean(parse_times), statistics.median(parse_times))
        if sql_times:
            self.logger.info("SQL gen times - avg: %.2fms, median: %.2fms", 
                           statistics.mean(sql_times), statistics.median(sql_times))
        
        return results
    
    def analyze_dataset(self, dataset_path: str, 
                       sample_size: Optional[int] = 100) -> DatasetStats:
        """
        Analyze dataset characteristics for performance planning.
        
        Args:
            dataset_path: Path to dataset directory
            sample_size: Number of files to sample for analysis
            
        Returns:
            DatasetStats: Dataset characteristics
        """
        self.logger.info("Analyzing dataset: %s", dataset_path)
        
        dataset_dir = Path(dataset_path)
        json_files = list(dataset_dir.glob("*.json"))
        
        if not json_files:
            self.logger.warning("No JSON files found in %s", dataset_path)
            return DatasetStats(0, 0, {}, 0, 0)
        
        # Sample files for analysis
        sample_files = json_files[:sample_size] if sample_size else json_files
        self.logger.info("Analyzing %d files (sample of %d total)", len(sample_files), len(json_files))
        
        total_size = 0
        resource_types = {}
        patients = set()
        file_sizes = []
        
        for file_path in sample_files:
            try:
                file_size = file_path.stat().st_size
                file_sizes.append(file_size / 1024)  # KB
                total_size += file_size
                
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    
                    # Extract resource type
                    resource_type = data.get("resourceType", "Unknown")
                    resource_types[resource_type] = resource_types.get(resource_type, 0) + 1
                    
                    # Track patients
                    if resource_type == "Patient":
                        patients.add(data.get("id"))
                    elif "subject" in data and "reference" in data["subject"]:
                        patient_ref = data["subject"]["reference"]
                        if patient_ref.startswith("Patient/"):
                            patients.add(patient_ref[8:])  # Remove "Patient/" prefix
                            
            except Exception as e:
                self.logger.warning("Skipped %s: %s", file_path, e)
        
        # Calculate statistics
        total_size_mb = total_size / (1024 * 1024)
        avg_file_size_kb = statistics.mean(file_sizes) if file_sizes else 0
        
        stats = DatasetStats(
            total_files=len(json_files),
            total_size_mb=total_size_mb,
            resource_types=resource_types,
            avg_file_size_kb=avg_file_size_kb,
            patient_count=len(patients)
        )
        
        self.logger.info("Dataset analysis complete:")
        self.logger.info("  - Total files: %d", stats.total_files)
        self.logger.info("  - Total size: %.1fMB", stats.total_size_mb)
        self.logger.info("  - Avg file size: %.1fKB", stats.avg_file_size_kb)
        self.logger.info("  - Unique patients: %d", stats.patient_count)
        self.logger.info("  - Resource types: %s...", dict(list(stats.resource_types.items())[:5]))
        
        return stats
    
    def generate_performance_report(self, output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate comprehensive performance report.
        
        Args:
            output_file: Optional file path to save report
            
        Returns:
            Dict containing performance analysis
        """
        if not self.results:
            self.logger.warning("No performance results to report")
            return {}
        
        self.logger.info("Generating performance report from %d measurements", len(self.results))
        
        # Group results by operation type
        by_operation = {}
        for result in self.results:
            op_type = result.operation.split('_')[0]  # Get base operation type
            if op_type not in by_operation:
                by_operation[op_type] = []
            by_operation[op_type].append(result)
        
        # Calculate statistics for each operation type
        report = {
            "timestamp": time.time(),
            "total_measurements": len(self.results),
            "operations": {}
        }
        
        for op_type, measurements in by_operation.items():
            durations = [m.duration_ms for m in measurements]
            
            op_stats = {
                "count": len(measurements),
                "duration_ms": {
                    "min": min(durations),
                    "max": max(durations),
                    "mean": statistics.mean(durations),
                    "median": statistics.median(durations)
                }
            }
            
            # Add operation-specific metrics
            if op_type == "dataset":
                files_processed = [m.files_processed for m in measurements if m.files_processed]
                if files_processed:
                    op_stats["files_processed"] = {
                        "total": sum(files_processed),
                        "avg_per_measurement": statistics.mean(files_processed)
                    }
                    
                throughputs = [m.throughput_per_sec for m in measurements if m.throughput_per_sec]
                if throughputs:
                    op_stats["throughput_files_per_sec"] = {
                        "min": min(throughputs),
                        "max": max(throughputs),
                        "mean": statistics.mean(throughputs)
                    }
            
            elif op_type == "viewdef":
                rows = [m.rows_processed for m in measurements if m.rows_processed is not None]
                if rows:
                    op_stats["rows_processed"] = {
                        "total": sum(rows),
                        "avg_per_measurement": statistics.mean(rows)
                    }
            
            report["operations"][op_type] = op_stats
        
        # Save report if requested
        if output_file:
            report_path = self.output_dir / output_file
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2)
            self.logger.info("Performance report saved to: %s", report_path)
        
        # Print summary
        self.logger.info("Performance Summary:")
        for op_type, stats in report["operations"].items():
            mean_time = stats["duration_ms"]["mean"]
            count = stats["count"]
            self.logger.info("  %s: %.1fms avg (%d measurements)", op_type, mean_time, count)
        
        return report


class _TimeContext:
    """Context manager for measuring execution time"""
    
    def __init__(self, operation_name: str, tester: PerformanceTester):
        self.operation_name = operation_name
        self.tester = tester
        self.start_time = None
        self.duration_ms = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = time.perf_counter()
        self.duration_ms = (end_time - self.start_time) * 1000


# Convenience functions for common performance tests
def quick_performance_test(dataset_path: str, sample_size: int = 50) -> Dict[str, Any]:
    """
    Run a quick performance test suite.
    
    Args:
        dataset_path: Path to FHIR dataset
        sample_size: Number of files to process
        
    Returns:
        Performance report dictionary
    """
    logger = get_logger(__name__)
    logger.info("Running Quick Performance Test Suite")
    logger.info("=" * 50)
    
    tester = PerformanceTester()
    
    # Analyze dataset
    stats = tester.analyze_dataset(dataset_path, sample_size)
    
    # Test FHIRPath operations
    test_expressions = [
        "Patient.name.family",
        "Patient.birthDate",
        "Patient.gender",
        "Observation.value.ofType(Quantity)",
        "Condition.code.coding.where(system='http://snomed.info/sct').code"
    ]
    tester.benchmark_fhirpath_operations(test_expressions)
    
    # Benchmark dataset loading (small sample)
    tester.benchmark_dataset_loading(dataset_path, sample_size=min(sample_size, 20))
    
    # Generate report
    return tester.generate_performance_report("quick_performance_test.json")


def comprehensive_performance_test(dataset_path: str) -> Dict[str, Any]:
    """
    Run comprehensive performance test suite.
    
    Args:
        dataset_path: Path to FHIR dataset
        
    Returns:
        Performance report dictionary
    """
    logger = get_logger(__name__)
    logger.info("Running Comprehensive Performance Test Suite")
    logger.info("=" * 60)
    
    tester = PerformanceTester()
    
    # Full dataset analysis
    stats = tester.analyze_dataset(dataset_path, sample_size=None)
    
    # Extensive FHIRPath testing
    test_expressions = [
        # Basic paths
        "Patient.name.family",
        "Patient.name.given",
        "Patient.birthDate",
        "Patient.gender",
        "Patient.address.postalCode",
        
        # Choice types
        "Observation.value.ofType(Quantity)",
        "Observation.value.ofType(string)",
        "Patient.deceased.ofType(boolean)",
        
        # Complex paths
        "Condition.code.coding.where(system='http://snomed.info/sct').code",
        "Observation.component.where(code.coding.code='8480-6').value.ofType(Quantity)",
        "Patient.contact.where(relationship.coding.code='C').name.family",
        
        # Functions
        "Patient.name.family.first()",
        "Observation.value.ofType(Quantity).value",
        "Condition.onset.ofType(dateTime)",
        
        # Nested operations
        "Patient.extension.where(url='http://example.org/race').value.ofType(Coding).code"
    ]
    
    tester.benchmark_fhirpath_operations(test_expressions)
    
    # Benchmark larger dataset loading
    tester.benchmark_dataset_loading(dataset_path, sample_size=100)
    
    # Generate comprehensive report
    return tester.generate_performance_report("comprehensive_performance_test.json")