# Task: Performance Validation Framework

**Task ID**: SP-002-005
**Sprint**: SP-002
**Task Name**: Performance Validation Framework and Benchmarking
**Assignee**: Junior Developer + QA
**Created**: 26-01-2025
**Last Updated**: 26-01-2025
**Dependencies**: SP-002-002 (Core Function Library), SP-002-003 (Literal Support)

---

## Task Overview

### Description
Establish a comprehensive performance validation framework to measure, monitor, and ensure the FHIRPath parser meets performance targets. This includes systematic benchmarking, automated performance regression testing, memory usage profiling, and validation of the <10ms parsing target for complex expressions.

### Category
- [ ] Feature Implementation
- [ ] Testing Infrastructure
- [ ] Architecture Enhancement
- [x] Performance Optimization
- [ ] Bug Fix
- [ ] Documentation
- [ ] Process Improvement

### Priority
- [x] Critical (Blocker for sprint goals)
- [ ] High (Important for sprint success)
- [ ] Medium (Valuable but not essential)
- [ ] Low (Stretch goal)

---

## Requirements

### Functional Requirements

#### Performance Benchmarking Framework
1. **Expression Categories**: Benchmark different types of expressions
   - Simple path navigation: `Patient.name`
   - Function calls: `Patient.name.first()`
   - Complex queries: `Patient.telecom.where(system = 'phone').value`
   - Nested operations: `Patient.name.where(use = 'official').given.first()`

2. **Performance Metrics**: Measure comprehensive performance characteristics
   - Parsing time (milliseconds)
   - Memory usage (peak and average)
   - AST node count and depth
   - Token count and processing time

3. **Automated Benchmarking**: Systematic performance measurement
   - Automated benchmark execution
   - Performance trend tracking over time
   - Regression detection and alerting
   - CI/CD integration for continuous monitoring

#### Performance Targets Validation
4. **Target Verification**: Validate specific performance requirements
   - Simple expressions: <1ms parsing time
   - Complex expressions: <10ms parsing time
   - Memory usage: <1MB per expression
   - Test suite execution: <60 seconds for 934 test cases

5. **Load Testing**: Validate performance under load
   - Concurrent parsing operations
   - Large expression parsing
   - Repeated parsing (cache effectiveness)
   - Memory leak detection

#### Performance Regression Testing
6. **Automated Regression Detection**: Monitor performance over time
   - Daily performance baseline updates
   - Regression threshold alerting
   - Performance comparison reports
   - Integration with development workflow

### Non-Functional Requirements
- **Accuracy**: Benchmark results must be accurate and reproducible
- **Automation**: Full automation of performance testing and reporting
- **Integration**: Seamless CI/CD integration without significant pipeline impact
- **Monitoring**: Real-time performance monitoring and alerting

---

## Technical Specifications

### Performance Benchmarking Framework
```python
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable
import time
import psutil
import statistics
from contextlib import contextmanager

@dataclass
class PerformanceMetrics:
    """Performance measurement results"""
    parsing_time_ms: float
    memory_usage_mb: float
    peak_memory_mb: float
    ast_node_count: int
    ast_depth: int
    token_count: int
    success: bool
    error_message: Optional[str] = None

@dataclass
class BenchmarkCase:
    """Individual benchmark test case"""
    name: str
    expression: str
    category: str
    expected_time_ms: float
    description: str

class PerformanceBenchmark:
    """Core performance benchmarking framework"""

    def __init__(self, parser_factory: Callable):
        self.parser_factory = parser_factory
        self.benchmark_cases: List[BenchmarkCase] = []
        self.baseline_metrics: Dict[str, PerformanceMetrics] = {}

    def add_benchmark_case(self, case: BenchmarkCase):
        """Add benchmark case to test suite"""
        self.benchmark_cases.append(case)

    def run_benchmark(self, case: BenchmarkCase, iterations: int = 10) -> PerformanceMetrics:
        """Run benchmark for single case with multiple iterations"""
        results = []

        for _ in range(iterations):
            result = self._measure_single_parse(case.expression)
            if result.success:
                results.append(result)

        if not results:
            return PerformanceMetrics(0, 0, 0, 0, 0, 0, False, "All iterations failed")

        # Calculate average metrics
        return PerformanceMetrics(
            parsing_time_ms=statistics.mean(r.parsing_time_ms for r in results),
            memory_usage_mb=statistics.mean(r.memory_usage_mb for r in results),
            peak_memory_mb=max(r.peak_memory_mb for r in results),
            ast_node_count=results[0].ast_node_count,
            ast_depth=results[0].ast_depth,
            token_count=results[0].token_count,
            success=True
        )

    @contextmanager
    def _memory_monitor(self):
        """Context manager for memory usage monitoring"""
        process = psutil.Process()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        peak_memory = start_memory

        yield lambda: max(peak_memory, process.memory_info().rss / 1024 / 1024)

        end_memory = process.memory_info().rss / 1024 / 1024
        return end_memory - start_memory, peak_memory

    def _measure_single_parse(self, expression: str) -> PerformanceMetrics:
        """Measure performance of single parse operation"""
        try:
            with self._memory_monitor() as get_peak_memory:
                start_time = time.perf_counter()

                # Create fresh parser instance
                from fhir4ds.parser.lexer import FHIRPathLexer
                from fhir4ds.parser.parser import Parser

                lexer = FHIRPathLexer(expression)
                tokens = list(lexer.tokenize())
                parser = Parser(tokens)
                ast = parser.parse()

                end_time = time.perf_counter()
                peak_memory = get_peak_memory()

            # Calculate metrics
            parsing_time_ms = (end_time - start_time) * 1000
            ast_node_count = self._count_ast_nodes(ast)
            ast_depth = self._calculate_ast_depth(ast)
            token_count = len(tokens)

            return PerformanceMetrics(
                parsing_time_ms=parsing_time_ms,
                memory_usage_mb=peak_memory,
                peak_memory_mb=peak_memory,
                ast_node_count=ast_node_count,
                ast_depth=ast_depth,
                token_count=token_count,
                success=True
            )

        except Exception as e:
            return PerformanceMetrics(0, 0, 0, 0, 0, 0, False, str(e))
```

### Performance Test Suite
```python
class PerformanceTestSuite:
    """Comprehensive performance test suite"""

    def __init__(self):
        self.benchmark = PerformanceBenchmark(self._create_parser)
        self._setup_benchmark_cases()

    def _setup_benchmark_cases(self):
        """Setup standard benchmark test cases"""

        # Simple expressions
        self.benchmark.add_benchmark_case(BenchmarkCase(
            name="simple_path",
            expression="Patient.name",
            category="simple",
            expected_time_ms=1.0,
            description="Simple path navigation"
        ))

        # Function calls
        self.benchmark.add_benchmark_case(BenchmarkCase(
            name="function_call",
            expression="Patient.name.first()",
            category="function",
            expected_time_ms=5.0,
            description="Simple function call"
        ))

        # Complex expressions
        self.benchmark.add_benchmark_case(BenchmarkCase(
            name="complex_query",
            expression="Patient.telecom.where(system = 'phone').value",
            category="complex",
            expected_time_ms=10.0,
            description="Complex query with filtering"
        ))

        # Nested operations
        self.benchmark.add_benchmark_case(BenchmarkCase(
            name="nested_operations",
            expression="Patient.name.where(use = 'official').given.first()",
            category="complex",
            expected_time_ms=10.0,
            description="Nested operations with filtering and functions"
        ))

        # Large expressions
        self.benchmark.add_benchmark_case(BenchmarkCase(
            name="large_expression",
            expression="Patient.name.where(use = 'official' or use = 'usual').given.where(length() > 2).first()",
            category="large",
            expected_time_ms=15.0,
            description="Large complex expression"
        ))

    def run_full_suite(self) -> Dict[str, PerformanceMetrics]:
        """Run complete performance test suite"""
        results = {}

        for case in self.benchmark.benchmark_cases:
            print(f"Running benchmark: {case.name}")
            result = self.benchmark.run_benchmark(case)
            results[case.name] = result

            # Validate against targets
            if result.success and result.parsing_time_ms > case.expected_time_ms:
                print(f"WARNING: {case.name} exceeded target time: {result.parsing_time_ms:.2f}ms > {case.expected_time_ms}ms")

        return results

    def generate_performance_report(self, results: Dict[str, PerformanceMetrics]) -> str:
        """Generate detailed performance report"""
        report = "# FHIRPath Parser Performance Report\n\n"

        for case_name, metrics in results.items():
            case = next(c for c in self.benchmark.benchmark_cases if c.name == case_name)

            report += f"## {case.description}\n"
            report += f"**Expression**: `{case.expression}`\n"
            report += f"**Category**: {case.category}\n\n"

            if metrics.success:
                report += f"- **Parsing Time**: {metrics.parsing_time_ms:.2f}ms (target: <{case.expected_time_ms}ms)\n"
                report += f"- **Memory Usage**: {metrics.memory_usage_mb:.2f}MB\n"
                report += f"- **AST Nodes**: {metrics.ast_node_count}\n"
                report += f"- **AST Depth**: {metrics.ast_depth}\n"
                report += f"- **Tokens**: {metrics.token_count}\n"

                if metrics.parsing_time_ms <= case.expected_time_ms:
                    report += "- **Status**: ✅ PASS\n"
                else:
                    report += "- **Status**: ❌ FAIL (exceeded target time)\n"
            else:
                report += f"- **Status**: ❌ ERROR - {metrics.error_message}\n"

            report += "\n"

        return report
```

### Automated Performance Monitoring
```python
class PerformanceMonitor:
    """Automated performance monitoring and regression detection"""

    def __init__(self, baseline_file: str = "performance_baseline.json"):
        self.baseline_file = baseline_file
        self.baseline_metrics: Dict[str, PerformanceMetrics] = {}
        self._load_baseline()

    def _load_baseline(self):
        """Load performance baseline from file"""
        import json
        try:
            with open(self.baseline_file, 'r') as f:
                data = json.load(f)
                for key, value in data.items():
                    self.baseline_metrics[key] = PerformanceMetrics(**value)
        except FileNotFoundError:
            print(f"No baseline file found at {self.baseline_file}, creating new baseline")

    def update_baseline(self, results: Dict[str, PerformanceMetrics]):
        """Update performance baseline with new results"""
        import json

        # Convert to serializable format
        serializable_results = {}
        for key, metrics in results.items():
            serializable_results[key] = {
                'parsing_time_ms': metrics.parsing_time_ms,
                'memory_usage_mb': metrics.memory_usage_mb,
                'peak_memory_mb': metrics.peak_memory_mb,
                'ast_node_count': metrics.ast_node_count,
                'ast_depth': metrics.ast_depth,
                'token_count': metrics.token_count,
                'success': metrics.success,
                'error_message': metrics.error_message
            }

        with open(self.baseline_file, 'w') as f:
            json.dump(serializable_results, f, indent=2)

        self.baseline_metrics = results

    def detect_regressions(self, current_results: Dict[str, PerformanceMetrics],
                          threshold_percent: float = 20.0) -> List[str]:
        """Detect performance regressions compared to baseline"""
        regressions = []

        for test_name, current_metrics in current_results.items():
            if test_name not in self.baseline_metrics:
                continue

            baseline_metrics = self.baseline_metrics[test_name]

            if not current_metrics.success or not baseline_metrics.success:
                continue

            # Check parsing time regression
            time_increase = ((current_metrics.parsing_time_ms - baseline_metrics.parsing_time_ms)
                           / baseline_metrics.parsing_time_ms * 100)

            if time_increase > threshold_percent:
                regressions.append(
                    f"{test_name}: {time_increase:.1f}% slower "
                    f"({current_metrics.parsing_time_ms:.2f}ms vs {baseline_metrics.parsing_time_ms:.2f}ms)"
                )

        return regressions
```

---

## Implementation Plan

### Day 1: Benchmarking Framework
- **Hour 1-4**: Implement core performance measurement framework
- **Hour 5-8**: Create benchmark test case definitions and runner
- **Hour 9-12**: Implement memory monitoring and AST metrics calculation
- **Hour 13-16**: Test benchmark framework with current parser

### Day 2: Performance Test Suite
- **Hour 1-4**: Create comprehensive benchmark test cases covering all expression types
- **Hour 5-8**: Implement automated test suite execution and reporting
- **Hour 9-12**: Add performance target validation and regression detection
- **Hour 13-16**: Run full performance analysis of current parser

### Day 3: CI Integration and Monitoring
- **Hour 1-4**: Integrate performance testing with CI/CD pipeline
- **Hour 5-8**: Implement automated baseline tracking and regression alerts
- **Hour 9-12**: Create performance monitoring dashboard and reports
- **Hour 13-16**: Validate complete performance framework and generate final report

---

## Acceptance Criteria

### Primary Acceptance Criteria
1. **✅ Performance Targets Validated**:
   - Simple expressions parse in <1ms
   - Complex expressions parse in <10ms
   - Memory usage within acceptable bounds
   - Test suite execution within 60 seconds

2. **✅ Automated Benchmarking**:
   - Complete benchmark suite operational
   - Automated performance measurement and reporting
   - CI/CD integration with performance monitoring

3. **✅ Regression Detection**:
   - Automated regression detection functional
   - Performance baseline tracking implemented
   - Alerts for performance degradation

4. **✅ Comprehensive Metrics**:
   - Parsing time measurement accurate
   - Memory usage profiling operational
   - AST complexity metrics calculated
   - Performance trends tracked over time

### Quality Gates
- **Performance targets met**: All benchmark cases meet their performance targets
- **Framework reliability**: Consistent and reproducible benchmark results
- **CI integration**: Automated performance testing integrated without pipeline impact
- **Documentation complete**: Performance characteristics documented

---

## Testing Strategy

### Framework Validation
1. **Benchmark Accuracy**: Verify benchmark measurements are accurate and consistent
2. **Memory Monitoring**: Validate memory usage measurements are correct
3. **Regression Detection**: Test regression detection with artificially degraded performance
4. **CI Integration**: Verify automated execution in CI environment

### Performance Validation
1. **Target Verification**: Confirm all performance targets are met
2. **Load Testing**: Test performance under various load conditions
3. **Edge Cases**: Test performance with edge case expressions
4. **Memory Leaks**: Verify no memory leaks in repeated parsing operations

---

## Deliverables

### Code Deliverables
1. **Performance Framework**: `tests/performance/benchmark_framework.py`
2. **Test Suite**: `tests/performance/test_suite.py`
3. **Performance Monitor**: `tests/performance/monitor.py`
4. **CI Integration**: `.github/workflows/performance.yml` (or equivalent)
5. **Reporting Tools**: `tests/performance/reporting.py`

### Documentation Deliverables
1. **Performance Report**: Comprehensive analysis of parser performance
2. **Benchmarking Guide**: How to run and interpret performance tests
3. **Performance Targets**: Documented performance requirements and validation
4. **Monitoring Guide**: How to monitor performance over time

### Testing Deliverables
1. **Performance Test Suite**: Complete suite of performance benchmarks
2. **Baseline Results**: Initial performance baseline for regression tracking
3. **CI Configuration**: Automated performance testing in CI/CD pipeline
4. **Performance Dashboard**: Real-time performance monitoring (if applicable)

---

## Success Metrics

### Quantitative Metrics
- **Performance Targets**: 100% of benchmark cases meet performance targets
- **Regression Detection**: 0 undetected performance regressions
- **CI Integration**: <5 minute additional CI time for performance testing
- **Framework Reliability**: <1% variance in repeated benchmark runs

### Qualitative Metrics
- **Framework Usability**: Easy to run benchmarks and interpret results
- **Documentation Quality**: Clear performance characteristics and monitoring guides
- **Developer Experience**: Performance feedback integrated into development workflow
- **Monitoring Effectiveness**: Early detection and alerting for performance issues

---

## Dependencies and Blockers

### Dependencies
1. **SP-002-002**: Core function library (to benchmark function performance)
2. **SP-002-003**: Literal support (to benchmark all expression types)
3. **CI/CD Infrastructure**: Available pipeline for integration

### Potential Blockers
1. **Performance Targets**: Current parser may not meet performance targets
2. **CI Integration**: CI configuration may require system administration access
3. **Memory Monitoring**: Accurate memory measurement may be platform-dependent

---

## Risk Mitigation

### Technical Risks
1. **Performance Targets Not Met**:
   - **Mitigation**: Identify performance bottlenecks and optimize critical paths
   - **Contingency**: Adjust targets if optimization efforts insufficient

2. **Framework Complexity**:
   - **Mitigation**: Start with simple benchmarks, add complexity incrementally
   - **Contingency**: Reduce scope to essential performance validation

3. **CI Integration Issues**:
   - **Mitigation**: Test CI integration early and incrementally
   - **Contingency**: Provide manual performance testing procedures

---

**Task establishes comprehensive performance validation ensuring parser meets production requirements and maintains performance over time.**