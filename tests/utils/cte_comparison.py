"""
CTE System Comparison Utility

This module provides tools for comparing the old vs new CTE system behavior
during the migration process. It helps validate that the new CTEBuilder
architecture produces equivalent results to the legacy system.

Key Features:
- Side-by-side comparison of SQL generation
- Performance metrics (SQL length, CTE count)
- Logical equivalence checking
- Batch testing for regression validation
- Detailed analysis reporting
"""

import re
import time
from typing import Dict, Any, List, Tuple, Optional, Union
from dataclasses import dataclass
from pathlib import Path
import json


@dataclass
class ComparisonResult:
    """Results from comparing old vs new CTE system."""
    expression: str
    sql_old: str
    sql_new: str
    old_length: int
    new_length: int
    old_cte_count: int
    new_cte_count: int
    old_generation_time: float
    new_generation_time: float
    equivalent: bool
    equivalence_confidence: float
    size_reduction_ratio: float
    performance_improvement: float
    issues: List[str]
    analysis: Dict[str, Any]


class CTESystemComparison:
    """
    Utility for comparing old vs new CTE system behavior.
    
    This class provides comprehensive comparison capabilities between
    the legacy CTE system and the new CTEBuilder architecture. It's
    designed to validate migration safety and measure improvements.
    """
    
    def __init__(self, table_name: str = "fhir_resources", json_column: str = "resource"):
        """
        Initialize comparison utility.
        
        Args:
            table_name: Default table name for SQL generation
            json_column: Default JSON column name
        """
        self.table_name = table_name
        self.json_column = json_column
        
        # Initialize dialects
        try:
            from fhir4ds.dialects.duckdb import DuckDBDialect
            self.dialect = DuckDBDialect()
        except ImportError:
            self.dialect = None
            
        # Metrics tracking
        self.comparison_history: List[ComparisonResult] = []
        
        # Analysis patterns for equivalence checking
        self.sql_patterns = {
            'select_count': r'\bSELECT\b',
            'from_count': r'\bFROM\b',
            'where_count': r'\bWHERE\b',
            'case_count': r'\bCASE\b',
            'json_extract_count': r'\bjson_extract\b',
            'with_count': r'\bWITH\b',
            'cte_definitions': r'\w+\s+AS\s*\(',
            'subquery_count': r'\(\s*SELECT\b'
        }
    
    def compare_systems(self, fhirpath_expr: str, 
                       resource_type_context: Optional[str] = None,
                       detailed_analysis: bool = True) -> ComparisonResult:
        """
        Compare old and new CTE system output for given expression.
        
        Args:
            fhirpath_expr: FHIRPath expression to test
            resource_type_context: Optional resource type context
            detailed_analysis: Whether to perform detailed SQL analysis
            
        Returns:
            ComparisonResult with comprehensive comparison data
        """
        issues = []
        
        try:
            # Test old system
            sql_old, time_old = self._generate_with_old_system(
                fhirpath_expr, resource_type_context
            )
        except Exception as e:
            issues.append(f"Old system error: {str(e)}")
            sql_old, time_old = "", 0.0
        
        try:
            # Test new system
            sql_new, time_new = self._generate_with_new_system(
                fhirpath_expr, resource_type_context
            )
        except Exception as e:
            issues.append(f"New system error: {str(e)}")
            sql_new, time_new = "", 0.0
        
        # Calculate metrics
        old_length = len(sql_old)
        new_length = len(sql_new)
        old_cte_count = sql_old.count('WITH') if sql_old else 0
        new_cte_count = sql_new.count('WITH') if sql_new else 0
        
        # Calculate ratios
        size_reduction_ratio = (
            (old_length - new_length) / max(old_length, 1) 
            if old_length > 0 else 0.0
        )
        
        performance_improvement = (
            (time_old - time_new) / max(time_old, 0.001) 
            if time_old > 0 else 0.0
        )
        
        # Check equivalence
        equivalent, confidence = self._check_equivalence(sql_old, sql_new)
        
        # Detailed analysis
        analysis = {}
        if detailed_analysis:
            analysis = self._perform_detailed_analysis(sql_old, sql_new)
        
        # Create result
        result = ComparisonResult(
            expression=fhirpath_expr,
            sql_old=sql_old,
            sql_new=sql_new,
            old_length=old_length,
            new_length=new_length,
            old_cte_count=old_cte_count,
            new_cte_count=new_cte_count,
            old_generation_time=time_old,
            new_generation_time=time_new,
            equivalent=equivalent,
            equivalence_confidence=confidence,
            size_reduction_ratio=size_reduction_ratio,
            performance_improvement=performance_improvement,
            issues=issues,
            analysis=analysis
        )
        
        # Store in history
        self.comparison_history.append(result)
        
        return result
    
    def _generate_with_old_system(self, expression: str, 
                                 context: Optional[str] = None) -> Tuple[str, float]:
        """
        Generate SQL using the old/legacy CTE system.
        
        Args:
            expression: FHIRPath expression
            context: Optional resource type context
            
        Returns:
            Tuple of (generated_sql, generation_time)
        """
        try:
            from fhir4ds.fhirpath.core.translator import FHIRPathToSQL
            
            # Create translator with old system (default behavior)
            translator = FHIRPathToSQL(
                table_name=self.table_name, 
                json_column=self.json_column,
                dialect=self.dialect
            )
            
            # Measure generation time
            start_time = time.perf_counter()
            sql = translator.translate(expression, resource_type_context=context)
            end_time = time.perf_counter()
            
            generation_time = end_time - start_time
            
            return sql, generation_time
            
        except Exception as e:
            raise RuntimeError(f"Old system generation failed: {str(e)}")
    
    def _generate_with_new_system(self, expression: str, 
                                 context: Optional[str] = None) -> Tuple[str, float]:
        """
        Generate SQL using the new CTEBuilder system.
        
        Args:
            expression: FHIRPath expression
            context: Optional resource type context
            
        Returns:
            Tuple of (generated_sql, generation_time)
        """
        try:
            from fhir4ds.fhirpath.core.translator import FHIRPathToSQL
            
            # Create translator with new CTE system
            translator = FHIRPathToSQL(
                table_name=self.table_name,
                json_column=self.json_column, 
                dialect=self.dialect
            )
            
            # Enable new CTEBuilder system
            translator.enable_new_cte_system()
            
            # Measure generation time
            start_time = time.perf_counter()
            sql = translator.translate(expression, resource_type_context=context)
            end_time = time.perf_counter()
            
            generation_time = end_time - start_time
            
            return sql, generation_time
            
        except Exception as e:
            raise RuntimeError(f"New system generation failed: {str(e)}")
    
    def _check_equivalence(self, sql1: str, sql2: str) -> Tuple[bool, float]:
        """
        Check if two SQL queries are logically equivalent.
        
        This performs structural analysis to determine if the queries
        are likely to produce the same results. It's not perfect but
        provides a good confidence measure.
        
        Args:
            sql1: First SQL query
            sql2: Second SQL query
            
        Returns:
            Tuple of (is_equivalent, confidence_score)
            where confidence_score is 0.0-1.0
        """
        if not sql1 or not sql2:
            return False, 0.0
        
        # Normalize whitespace
        norm1 = self._normalize_sql(sql1)
        norm2 = self._normalize_sql(sql2)
        
        # Exact match
        if norm1 == norm2:
            return True, 1.0
        
        # Structural analysis
        confidence_factors = []
        
        # Compare pattern counts
        for pattern_name, pattern in self.sql_patterns.items():
            count1 = len(re.findall(pattern, norm1, re.IGNORECASE))
            count2 = len(re.findall(pattern, norm2, re.IGNORECASE))
            
            if count1 == count2:
                confidence_factors.append(1.0)
            elif count1 == 0 and count2 == 0:
                confidence_factors.append(1.0)
            else:
                # Partial credit based on similarity
                max_count = max(count1, count2)
                min_count = min(count1, count2)
                similarity = min_count / max_count if max_count > 0 else 0.0
                confidence_factors.append(similarity)
        
        # Key phrase analysis
        key_phrases = [
            'json_extract', 'json_type', 'json_array_length',
            'CASE WHEN', 'WHERE', 'FROM', 'SELECT'
        ]
        
        phrase_matches = 0
        for phrase in key_phrases:
            in_sql1 = phrase.lower() in norm1.lower()
            in_sql2 = phrase.lower() in norm2.lower()
            if in_sql1 == in_sql2:
                phrase_matches += 1
        
        phrase_confidence = phrase_matches / len(key_phrases)
        confidence_factors.append(phrase_confidence)
        
        # Calculate overall confidence
        overall_confidence = sum(confidence_factors) / len(confidence_factors)
        
        # Determine equivalence (threshold can be adjusted)
        equivalent = overall_confidence >= 0.8
        
        return equivalent, overall_confidence
    
    def _normalize_sql(self, sql: str) -> str:
        """
        Normalize SQL for comparison.
        
        Args:
            sql: SQL string to normalize
            
        Returns:
            Normalized SQL string
        """
        if not sql:
            return ""
        
        # Remove extra whitespace
        normalized = ' '.join(sql.split())
        
        # Standardize case for keywords (but preserve identifiers)
        keywords = [
            'SELECT', 'FROM', 'WHERE', 'WITH', 'AS', 'CASE', 'WHEN', 
            'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT', 'NULL', 'IS',
            'LIKE', 'IN', 'EXISTS', 'UNION', 'ORDER', 'BY', 'GROUP',
            'HAVING', 'LIMIT', 'OFFSET'
        ]
        
        for keyword in keywords:
            # Use word boundaries to avoid replacing parts of identifiers
            pattern = r'\b' + keyword.lower() + r'\b'
            normalized = re.sub(pattern, keyword, normalized, flags=re.IGNORECASE)
        
        return normalized
    
    def _perform_detailed_analysis(self, sql1: str, sql2: str) -> Dict[str, Any]:
        """
        Perform detailed analysis of SQL differences.
        
        Args:
            sql1: First SQL query
            sql2: Second SQL query
            
        Returns:
            Dictionary with detailed analysis results
        """
        analysis = {
            'length_comparison': {
                'sql1_length': len(sql1),
                'sql2_length': len(sql2),
                'difference': len(sql1) - len(sql2),
                'reduction_percentage': (len(sql1) - len(sql2)) / max(len(sql1), 1) * 100
            },
            'pattern_analysis': {},
            'complexity_metrics': {},
            'structure_differences': []
        }
        
        # Pattern analysis
        for pattern_name, pattern in self.sql_patterns.items():
            count1 = len(re.findall(pattern, sql1, re.IGNORECASE))
            count2 = len(re.findall(pattern, sql2, re.IGNORECASE))
            
            analysis['pattern_analysis'][pattern_name] = {
                'sql1_count': count1,
                'sql2_count': count2,
                'difference': count1 - count2
            }
        
        # Complexity metrics
        analysis['complexity_metrics'] = {
            'sql1_complexity': self._calculate_sql_complexity(sql1),
            'sql2_complexity': self._calculate_sql_complexity(sql2)
        }
        
        # Structure differences
        structure_diffs = self._find_structure_differences(sql1, sql2)
        analysis['structure_differences'] = structure_diffs
        
        return analysis
    
    def _calculate_sql_complexity(self, sql: str) -> Dict[str, int]:
        """
        Calculate complexity metrics for SQL.
        
        Args:
            sql: SQL string to analyze
            
        Returns:
            Dictionary with complexity metrics
        """
        if not sql:
            return {'total_score': 0}
        
        metrics = {
            'length': len(sql),
            'select_statements': len(re.findall(r'\bSELECT\b', sql, re.IGNORECASE)),
            'subqueries': len(re.findall(r'\(\s*SELECT\b', sql, re.IGNORECASE)),
            'case_statements': len(re.findall(r'\bCASE\b', sql, re.IGNORECASE)),
            'json_operations': len(re.findall(r'\bjson_\w+', sql, re.IGNORECASE)),
            'with_clauses': len(re.findall(r'\bWITH\b', sql, re.IGNORECASE)),
            'parentheses_depth': self._calculate_max_nesting_depth(sql)
        }
        
        # Calculate composite complexity score
        metrics['total_score'] = (
            metrics['length'] // 100 +
            metrics['select_statements'] * 2 +
            metrics['subqueries'] * 3 +
            metrics['case_statements'] * 2 +
            metrics['json_operations'] * 1 +
            metrics['with_clauses'] * 1 +
            metrics['parentheses_depth'] * 2
        )
        
        return metrics
    
    def _calculate_max_nesting_depth(self, sql: str) -> int:
        """Calculate maximum parentheses nesting depth."""
        max_depth = 0
        current_depth = 0
        
        for char in sql:
            if char == '(':
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char == ')':
                current_depth = max(0, current_depth - 1)
        
        return max_depth
    
    def _find_structure_differences(self, sql1: str, sql2: str) -> List[str]:
        """
        Find structural differences between two SQL queries.
        
        Args:
            sql1: First SQL query
            sql2: Second SQL query
            
        Returns:
            List of difference descriptions
        """
        differences = []
        
        # Compare CTE usage
        cte1_count = sql1.count('WITH')
        cte2_count = sql2.count('WITH')
        if cte1_count != cte2_count:
            differences.append(f"CTE count differs: {cte1_count} vs {cte2_count}")
        
        # Compare subquery patterns
        subq1 = len(re.findall(r'\(\s*SELECT\b', sql1, re.IGNORECASE))
        subq2 = len(re.findall(r'\(\s*SELECT\b', sql2, re.IGNORECASE))
        if subq1 != subq2:
            differences.append(f"Subquery count differs: {subq1} vs {subq2}")
        
        # Compare JSON operations
        json1 = len(re.findall(r'\bjson_\w+', sql1, re.IGNORECASE))
        json2 = len(re.findall(r'\bjson_\w+', sql2, re.IGNORECASE))
        if json1 != json2:
            differences.append(f"JSON operation count differs: {json1} vs {json2}")
        
        return differences
    
    def batch_compare(self, expressions: List[str], 
                     contexts: Optional[List[Optional[str]]] = None,
                     output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Compare multiple expressions and generate summary report.
        
        Args:
            expressions: List of FHIRPath expressions to test
            contexts: Optional list of resource type contexts
            output_file: Optional file to save detailed results
            
        Returns:
            Summary report dictionary
        """
        if contexts is None:
            contexts = [None] * len(expressions)
        elif len(contexts) != len(expressions):
            raise ValueError("contexts list must match expressions list length")
        
        results = []
        
        # Run comparisons
        for expr, context in zip(expressions, contexts):
            try:
                result = self.compare_systems(expr, context)
                results.append(result)
            except Exception as e:
                # Continue with other expressions even if one fails
                print(f"Failed to compare expression '{expr}': {e}")
        
        # Generate summary
        summary = self._generate_batch_summary(results)
        
        # Save detailed results if requested
        if output_file:
            self._save_results(results, output_file)
        
        return summary
    
    def _generate_batch_summary(self, results: List[ComparisonResult]) -> Dict[str, Any]:
        """Generate summary statistics from batch comparison results."""
        if not results:
            return {'error': 'No successful comparisons'}
        
        # Basic statistics
        total_comparisons = len(results)
        equivalent_count = sum(1 for r in results if r.equivalent)
        avg_confidence = sum(r.equivalence_confidence for r in results) / total_comparisons
        
        # Performance statistics
        avg_old_time = sum(r.old_generation_time for r in results) / total_comparisons
        avg_new_time = sum(r.new_generation_time for r in results) / total_comparisons
        avg_size_reduction = sum(r.size_reduction_ratio for r in results) / total_comparisons
        
        # Issue analysis
        all_issues = [issue for r in results for issue in r.issues]
        issue_types = {}
        for issue in all_issues:
            issue_type = issue.split(':')[0] if ':' in issue else 'Other'
            issue_types[issue_type] = issue_types.get(issue_type, 0) + 1
        
        return {
            'total_comparisons': total_comparisons,
            'equivalent_count': equivalent_count,
            'equivalence_rate': equivalent_count / total_comparisons,
            'average_confidence': avg_confidence,
            'performance': {
                'avg_old_generation_time': avg_old_time,
                'avg_new_generation_time': avg_new_time,
                'avg_performance_improvement': (avg_old_time - avg_new_time) / max(avg_old_time, 0.001),
                'avg_size_reduction': avg_size_reduction
            },
            'issues': {
                'total_issues': len(all_issues),
                'issue_types': issue_types,
                'expressions_with_issues': len([r for r in results if r.issues])
            },
            'cte_usage': {
                'expressions_using_old_ctes': len([r for r in results if r.old_cte_count > 0]),
                'expressions_using_new_ctes': len([r for r in results if r.new_cte_count > 0]),
                'avg_old_cte_count': sum(r.old_cte_count for r in results) / total_comparisons,
                'avg_new_cte_count': sum(r.new_cte_count for r in results) / total_comparisons
            }
        }
    
    def _save_results(self, results: List[ComparisonResult], output_file: str) -> None:
        """Save detailed comparison results to file."""
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert results to serializable format
        serializable_results = []
        for result in results:
            serializable_results.append({
                'expression': result.expression,
                'sql_old': result.sql_old,
                'sql_new': result.sql_new,
                'metrics': {
                    'old_length': result.old_length,
                    'new_length': result.new_length,
                    'old_cte_count': result.old_cte_count,
                    'new_cte_count': result.new_cte_count,
                    'old_generation_time': result.old_generation_time,
                    'new_generation_time': result.new_generation_time,
                    'size_reduction_ratio': result.size_reduction_ratio,
                    'performance_improvement': result.performance_improvement
                },
                'equivalence': {
                    'equivalent': result.equivalent,
                    'confidence': result.equivalence_confidence
                },
                'issues': result.issues,
                'analysis': result.analysis
            })
        
        with open(output_path, 'w') as f:
            json.dump(serializable_results, f, indent=2)
    
    def get_summary_report(self) -> str:
        """
        Generate a human-readable summary report of all comparisons.
        
        Returns:
            Formatted summary report string
        """
        if not self.comparison_history:
            return "No comparisons performed yet."
        
        summary = self._generate_batch_summary(self.comparison_history)
        
        report = f"""
CTE System Comparison Summary Report
=====================================

Total Comparisons: {summary['total_comparisons']}
Equivalent Results: {summary['equivalent_count']} ({summary['equivalence_rate']:.1%})
Average Confidence: {summary['average_confidence']:.3f}

Performance Metrics:
- Average Old Generation Time: {summary['performance']['avg_old_generation_time']:.4f}s
- Average New Generation Time: {summary['performance']['avg_new_generation_time']:.4f}s
- Performance Improvement: {summary['performance']['avg_performance_improvement']:.1%}
- Average Size Reduction: {summary['performance']['avg_size_reduction']:.1%}

CTE Usage:
- Expressions Using Old CTEs: {summary['cte_usage']['expressions_using_old_ctes']}
- Expressions Using New CTEs: {summary['cte_usage']['expressions_using_new_ctes']}
- Average Old CTE Count: {summary['cte_usage']['avg_old_cte_count']:.1f}
- Average New CTE Count: {summary['cte_usage']['avg_new_cte_count']:.1f}

Issues:
- Total Issues: {summary['issues']['total_issues']}
- Expressions with Issues: {summary['issues']['expressions_with_issues']}
- Issue Types: {summary['issues']['issue_types']}
"""
        
        return report.strip()


# Convenience functions for common use cases

def compare_expression(expression: str, context: Optional[str] = None) -> ComparisonResult:
    """
    Quick comparison of a single expression.
    
    Args:
        expression: FHIRPath expression to compare
        context: Optional resource type context
        
    Returns:
        ComparisonResult
    """
    comparator = CTESystemComparison()
    return comparator.compare_systems(expression, context)


def batch_test_expressions(expressions: List[str], 
                          output_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Quick batch testing of multiple expressions.
    
    Args:
        expressions: List of FHIRPath expressions
        output_file: Optional output file for detailed results
        
    Returns:
        Summary report dictionary
    """
    comparator = CTESystemComparison()
    return comparator.batch_compare(expressions, output_file=output_file)


# Test expressions for validation
SAMPLE_EXPRESSIONS = [
    "name.family",
    "name.where(use='official').family.first()",
    "telecom.where(system='email').value",
    "birthDate > '2000-01-01'",
    "address.where(use='home').city",
    "name.given.join(' ')",
    "telecom.where(system='phone' and use='mobile').value.first()",
    "extension.where(url='custom-field').value",
    "contact.name.where(use='official').select(family + ', ' + given.first()).first()",
    "identifier.where(system='ssn').value"
]


if __name__ == "__main__":
    # Example usage
    print("Testing CTE System Comparison...")
    
    comparator = CTESystemComparison()
    
    # Test a few sample expressions
    test_expressions = SAMPLE_EXPRESSIONS[:3]
    
    for expr in test_expressions:
        print(f"\nTesting: {expr}")
        try:
            result = comparator.compare_systems(expr)
            print(f"  Equivalent: {result.equivalent}")
            print(f"  Confidence: {result.equivalence_confidence:.3f}")
            print(f"  Size reduction: {result.size_reduction_ratio:.1%}")
        except Exception as e:
            print(f"  Error: {e}")
    
    # Generate summary report
    print("\n" + comparator.get_summary_report())