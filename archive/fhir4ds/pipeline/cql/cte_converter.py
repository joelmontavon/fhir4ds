"""
CQL to CTE Converter for Unified Pipeline Architecture

This module extracts and adapts the proven CTE conversion logic from the original
cte_pipeline system, integrating it with the unified pipeline architecture.

Key Features:
- Enhanced dependency detection for CQL define statements
- Resource type detection for FHIR queries
- CQL pattern analysis for filter condition extraction
- Integration with unified execution context and dialect system
"""

from typing import Dict, List, Optional, Set, Any, Tuple
import re
import logging
import inspect
from dataclasses import dataclass

# Import unified pipeline components
from ..core.cte_integration import CTEFragment, UnifiedExecutionContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CQLDefineMetadata:
    """Metadata for a CQL define statement."""
    name: str
    expression: str
    dependencies: List[str]
    resource_type: str
    estimated_complexity: int = 0


class EnhancedDependencyDetector:
    """
    Enhanced dependency detection for CQL define statements.

    Extracted and adapted from cte_pipeline/core/cql_to_cte_converter.py
    to work with the unified pipeline architecture.
    """

    def __init__(self):
        """Initialize the enhanced dependency detector."""
        # Common CQL patterns that indicate dependencies
        self.dependency_patterns = [
            # Direct define references: "Office Visits", 'Emergency Visits'
            (r'"([^"]+)"', 'direct_quote'),
            (r"'([^']+)'", 'direct_single_quote'),

            # Function calls with define references: exists "Some Define"
            (r'exists\s+["\']([^"\']+)["\']', 'exists_reference'),

            # Union/intersect operations: "Define A" union "Define B"
            (r'["\']([^"\']+)["\']\s+union\s+["\']([^"\']+)["\']', 'union_operation'),
            (r'["\']([^"\']+)["\']\s+intersect\s+["\']([^"\']+)["\']', 'intersect_operation'),

            # With/such that clauses: "Define A" with "Define B" such that...
            (r'["\']([^"\']+)["\']\s+with\s+["\']([^"\']+)["\']', 'with_clause'),
        ]

    def detect_dependencies(self, cql_expression: str, available_defines: Set[str]) -> List[str]:
        """
        Detect dependencies from a single CQL expression.

        Args:
            cql_expression: CQL expression to analyze
            available_defines: Set of available define names

        Returns:
            List of define names this expression depends on
        """
        dependencies = set()
        cql_clean = cql_expression.strip()

        # Apply dependency detection patterns
        for pattern, pattern_type in self.dependency_patterns:
            matches = re.findall(pattern, cql_clean, re.IGNORECASE)

            if pattern_type in ['union_operation', 'intersect_operation', 'with_clause']:
                # These patterns return tuples for multiple captures
                for match_tuple in matches:
                    if isinstance(match_tuple, tuple):
                        for match in match_tuple:
                            if match in available_defines:
                                dependencies.add(match)
            else:
                # Single capture patterns
                for match in matches:
                    if match in available_defines:
                        dependencies.add(match)

        logger.debug(f"Found dependencies {list(dependencies)} in CQL: {cql_expression[:50]}...")
        return list(dependencies)


class ResourceTypeDetector:
    """
    Enhanced resource type detector for CQL expressions.

    Adapted from cte_pipeline to work with unified pipeline dialect system.
    """

    # Resource type patterns with improved detection
    RESOURCE_TYPE_PATTERNS = {
        'Patient': {
            'resource_syntax': ['[Patient]', '[Patient:'],
            'indicators': ['patient', 'AgeInYears()', 'birthDate', 'gender'],
            'priority': 10
        },
        'Condition': {
            'resource_syntax': ['[Condition]', '[Condition:'],
            'indicators': ['asthma', 'diabetes', 'hypertension', 'diagnosis'],
            'priority': 8
        },
        'MedicationDispense': {
            'resource_syntax': ['[MedicationDispense]', '[MedicationDispense:'],
            'indicators': ['medication', 'dispense', 'medicationCodeableConcept'],
            'priority': 7
        },
        'Encounter': {
            'resource_syntax': ['[Encounter]', '[Encounter:'],
            'indicators': ['encounter', 'visit', 'admission', 'hospitalization'],
            'priority': 7
        },
        'Observation': {
            'resource_syntax': ['[Observation]', '[Observation:'],
            'indicators': ['observation', 'vital', 'lab', 'result'],
            'priority': 6
        }
    }

    def detect_from_cql(self, cql_expr: str, define_name: str = "") -> str:
        """
        Detect FHIR resource type from CQL expression and context.

        Args:
            cql_expr: CQL expression to analyze
            define_name: Name of the define statement (for additional context)

        Returns:
            Detected FHIR resource type
        """
        cql_expr_lower = cql_expr.lower()

        # First check for explicit resource syntax
        scores = {}
        for resource_type, pattern_info in self.RESOURCE_TYPE_PATTERNS.items():
            score = 0

            # Check explicit syntax patterns
            for syntax in pattern_info['resource_syntax']:
                if syntax.lower() in cql_expr_lower:
                    score += pattern_info['priority'] * 2  # High weight for explicit syntax

            # Check indicator patterns
            for indicator in pattern_info['indicators']:
                if indicator.lower() in cql_expr_lower:
                    score += pattern_info['priority']

            if score > 0:
                scores[resource_type] = score

        # Return highest scoring resource type
        if scores:
            detected_type = max(scores.items(), key=lambda x: x[1])[0]
            logger.debug(f"Detected resource type '{detected_type}' from explicit syntax in CQL: '{cql_expr[:50]}...'")
            return detected_type

        # Fallback: analyze define name and context
        try:
            define_lower = define_name.lower()

            # Check define name patterns directly
            if any(term in define_lower for term in ['asthma', 'copd', 'diabetes', 'hypertension']):
                return 'Condition'
            elif any(term in define_lower for term in ['medication', 'dispense', 'drug', 'therapy']):
                return 'MedicationDispense'
            elif any(term in define_lower for term in ['encounter', 'visit', 'admission']):
                return 'Encounter'
            elif any(term in define_lower for term in ['observation', 'vital', 'lab']):
                return 'Observation'

        except Exception as e:
            logger.debug(f"Define name analysis failed: {e}")

        return 'Patient'  # Safe default


class CQLPatternAnalyzer:
    """
    Analyzes CQL expressions to extract filter conditions and patterns.

    Adapted from cte_pipeline to work with unified pipeline dialect system.
    """

    def __init__(self, context: UnifiedExecutionContext):
        """Initialize pattern analyzer with unified execution context."""
        self.context = context
        self.dialect = context.dialect

    def extract_filter_conditions(self, cql_expr: str, resource_type: str) -> List[str]:
        """
        Extract WHERE conditions from CQL expression.

        Converts CQL filter expressions into SQL conditions using dialect-specific
        SQL generation patterns.

        Args:
            cql_expr: CQL expression to analyze
            resource_type: FHIR resource type being queried

        Returns:
            List of SQL WHERE conditions
        """
        conditions = []

        # Always add resource type filter
        resource_type_condition = self._get_resource_type_condition(resource_type)
        conditions.append(resource_type_condition)

        # Extract age filters
        age_conditions = self._extract_age_conditions(cql_expr)
        conditions.extend(age_conditions)

        # Extract code/text filters
        code_conditions = self._extract_code_conditions(cql_expr, resource_type)
        conditions.extend(code_conditions)

        # Extract date range filters
        date_conditions = self._extract_date_conditions(cql_expr)
        conditions.extend(date_conditions)

        # Extract boolean filters
        boolean_conditions = self._extract_boolean_conditions(cql_expr)
        conditions.extend(boolean_conditions)

        logger.debug(f"Extracted {len(conditions)} conditions from CQL: {cql_expr[:50]}...")
        return conditions

    def _get_resource_type_condition(self, resource_type: str) -> str:
        """Generate resource type filter condition using unified dialect."""
        # Use dialect to generate resource type condition
        if hasattr(self.dialect, 'generate_resource_type_condition'):
            return self.dialect.generate_resource_type_condition(resource_type)
        else:
            # Fallback for basic resource type filtering
            return f"json_extract_string(resource, '$.resourceType') = '{resource_type}'"

    def _extract_age_conditions(self, cql_expr: str) -> List[str]:
        """
        Extract age-related conditions from CQL.

        Handles patterns like:
        - AgeInYears() >= 18
        - AgeInYears() between 5 and 64
        """
        conditions = []

        # Age comparison patterns
        age_patterns = [
            r'AgeInYears\(\)\s*>=\s*(\d+)',
            r'AgeInYears\(\)\s*>\s*(\d+)',
            r'AgeInYears\(\)\s*<=\s*(\d+)',
            r'AgeInYears\(\)\s*<\s*(\d+)',
            r'AgeInYears\(\)\s*=\s*(\d+)',
        ]

        for pattern in age_patterns:
            matches = re.findall(pattern, cql_expr, re.IGNORECASE)
            for age_value in matches:
                # Use dialect to generate age calculation
                if hasattr(self.dialect, 'generate_age_calculation'):
                    age_condition = self.dialect.generate_age_calculation(
                        operator=self._extract_operator_from_pattern(pattern),
                        age_value=int(age_value)
                    )
                    conditions.append(age_condition)
                else:
                    # Basic age calculation fallback
                    operator = self._extract_operator_from_pattern(pattern)
                    conditions.append(f"extract(year from current_date) - extract(year from cast(json_extract_string(resource, '$.birthDate') as date)) {operator} {age_value}")

        return conditions

    def _extract_operator_from_pattern(self, pattern: str) -> str:
        """Extract SQL operator from regex pattern."""
        if '>=' in pattern:
            return '>='
        elif '>' in pattern:
            return '>'
        elif '<=' in pattern:
            return '<='
        elif '<' in pattern:
            return '<'
        elif '=' in pattern:
            return '='
        return '='

    def _extract_code_conditions(self, cql_expr: str, resource_type: str) -> List[str]:
        """Extract code-based filter conditions."""
        conditions = []

        # Look for code patterns like: code in "ICD10-CM"
        code_patterns = [
            r'code\s+in\s+["\']([^"\']+)["\']',
            r'coding\s+in\s+["\']([^"\']+)["\']',
        ]

        for pattern in code_patterns:
            matches = re.findall(pattern, cql_expr, re.IGNORECASE)
            for code_system in matches:
                # Use dialect to generate code condition
                if hasattr(self.dialect, 'generate_code_condition'):
                    code_condition = self.dialect.generate_code_condition(code_system, resource_type)
                    conditions.append(code_condition)

        return conditions

    def _extract_date_conditions(self, cql_expr: str) -> List[str]:
        """Extract date range filter conditions."""
        conditions = []

        # Date range patterns
        date_patterns = [
            r'during\s+["\']([^"\']+)["\']',
            r'after\s+["\']([^"\']+)["\']',
            r'before\s+["\']([^"\']+)["\']',
        ]

        for pattern in date_patterns:
            matches = re.findall(pattern, cql_expr, re.IGNORECASE)
            for date_value in matches:
                # Use dialect to generate date condition
                if hasattr(self.dialect, 'generate_date_condition'):
                    date_condition = self.dialect.generate_date_condition(
                        operator=self._extract_date_operator_from_pattern(pattern),
                        date_value=date_value
                    )
                    conditions.append(date_condition)

        return conditions

    def _extract_date_operator_from_pattern(self, pattern: str) -> str:
        """Extract date operator from regex pattern."""
        if 'during' in pattern.lower():
            return 'during'
        elif 'after' in pattern.lower():
            return 'after'
        elif 'before' in pattern.lower():
            return 'before'
        return 'during'

    def _extract_boolean_conditions(self, cql_expr: str) -> List[str]:
        """Extract boolean filter conditions."""
        conditions = []

        # Boolean patterns
        if 'is not null' in cql_expr.lower():
            conditions.append("resource IS NOT NULL")

        return conditions


class CQLToCTEConverter:
    """
    Main converter class that transforms CQL definitions into CTE fragments.

    This integrates the proven CTE conversion logic with the unified pipeline
    architecture, preserving the performance benefits while enabling integration.
    """

    def __init__(self, context: UnifiedExecutionContext):
        """Initialize converter with unified execution context."""
        self.context = context
        self.dependency_detector = EnhancedDependencyDetector()
        self.resource_detector = ResourceTypeDetector()
        self.pattern_analyzer = CQLPatternAnalyzer(context)

    def convert_defines_to_cte_fragments(self, defines: Dict[str, str]) -> List[CTEFragment]:
        """
        Convert CQL define statements to CTE fragments.

        Args:
            defines: Dictionary mapping define names to CQL expressions

        Returns:
            List of CTE fragments with resolved dependencies
        """
        metadata = self._analyze_defines(defines)
        ordered_metadata = self._resolve_dependencies(metadata)
        fragments = self._generate_cte_fragments(ordered_metadata)

        logger.info(f"Converted {len(defines)} CQL defines to {len(fragments)} CTE fragments")
        return fragments

    def _analyze_defines(self, defines: Dict[str, str]) -> List[CQLDefineMetadata]:
        """Analyze CQL defines to extract metadata."""
        metadata = []
        available_defines = set(defines.keys())

        for define_name, cql_expression in defines.items():
            # Detect dependencies
            dependencies = self.dependency_detector.detect_dependencies(
                cql_expression, available_defines
            )

            # Detect resource type
            resource_type = self.resource_detector.detect_from_cql(
                cql_expression, define_name
            )

            # Estimate complexity
            complexity = self._estimate_complexity(cql_expression)

            metadata.append(CQLDefineMetadata(
                name=define_name,
                expression=cql_expression,
                dependencies=dependencies,
                resource_type=resource_type,
                estimated_complexity=complexity
            ))

        return metadata

    def _estimate_complexity(self, cql_expression: str) -> int:
        """Estimate complexity of CQL expression."""
        complexity = 0

        # Base complexity
        complexity += len(cql_expression) // 100

        # Additional complexity for patterns
        if 'union' in cql_expression.lower():
            complexity += 3
        if 'intersect' in cql_expression.lower():
            complexity += 3
        if 'exists' in cql_expression.lower():
            complexity += 2
        if 'such that' in cql_expression.lower():
            complexity += 2

        return min(complexity, 10)  # Cap at 10

    def _resolve_dependencies(self, metadata: List[CQLDefineMetadata]) -> List[CQLDefineMetadata]:
        """Resolve dependencies and order metadata topologically."""
        # Create dependency graph
        graph = {meta.name: meta.dependencies for meta in metadata}
        metadata_map = {meta.name: meta for meta in metadata}

        # Topological sort
        ordered_names = self._topological_sort(graph)

        return [metadata_map[name] for name in ordered_names if name in metadata_map]

    def _topological_sort(self, graph: Dict[str, List[str]]) -> List[str]:
        """Perform topological sort on dependency graph - dependencies first."""
        in_degree = {node: 0 for node in graph}
        for node in graph:
            for neighbor in graph[node]:
                if neighbor in in_degree:
                    in_degree[neighbor] += 1

        queue = [node for node in in_degree if in_degree[node] == 0]
        result = []

        while queue:
            node = queue.pop(0)
            result.append(node)

            for neighbor in graph.get(node, []):
                if neighbor in in_degree:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

        # Reverse to put dependencies first (base definitions before dependent ones)
        return list(reversed(result))

    def _generate_cte_fragments(self, ordered_metadata: List[CQLDefineMetadata]) -> List[CTEFragment]:
        """Generate CTE fragments from ordered metadata."""
        fragments = []

        for meta in ordered_metadata:
            # Extract filter conditions
            conditions = self.pattern_analyzer.extract_filter_conditions(
                meta.expression, meta.resource_type
            )

            # Generate SQL for this CTE
            cte_sql = self._build_cte_sql(meta, conditions)

            # Create CTE fragment
            fragment = CTEFragment(
                name=self._sanitize_cte_name(meta.name),
                sql=cte_sql,
                dependencies=[self._sanitize_cte_name(dep) for dep in meta.dependencies],
                optimization_level=self.context.optimization_level
            )

            fragments.append(fragment)

        return fragments

    def _sanitize_cte_name(self, name: str) -> str:
        """Sanitize CQL define name to valid SQL identifier."""
        # Replace spaces and special characters with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Ensure it starts with a letter or underscore
        if sanitized and sanitized[0].isdigit():
            sanitized = f"define_{sanitized}"
        return sanitized.lower()

    def _build_cte_sql(self, meta: CQLDefineMetadata, conditions: List[str]) -> str:
        """Build SQL for a single CTE from metadata and conditions."""
        # Basic structure for CTE SQL
        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Use resource type to determine base query
        if meta.resource_type == 'Patient':
            base_query = f"""
                SELECT DISTINCT
                    json_extract_string(resource, '$.id') as patient_id,
                    resource
                FROM fhir_resources
                WHERE {where_clause}
            """
        else:
            base_query = f"""
                SELECT DISTINCT
                    json_extract_string(resource, '$.subject.reference') as patient_ref,
                    json_extract_string(resource, '$.id') as resource_id,
                    resource
                FROM fhir_resources
                WHERE {where_clause}
            """

        return base_query.strip()


def create_cql_to_cte_converter(context: UnifiedExecutionContext) -> CQLToCTEConverter:
    """
    Factory function to create CQL to CTE converter.

    Args:
        context: Unified execution context

    Returns:
        Configured CQL to CTE converter
    """
    return CQLToCTEConverter(context)