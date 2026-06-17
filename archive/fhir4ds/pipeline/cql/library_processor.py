"""
CQL Library Processor for Unified Pipeline Architecture

This module provides CQL library processing capabilities that integrate with
the unified pipeline system, enabling efficient execution of complete CQL
libraries through the optimized pipeline architecture.

Key Features:
- CQL library parsing and processing
- Define statement extraction and dependency analysis
- Integration with CTE optimization for monolithic execution
- Backward compatibility with existing CQL workflows
"""

from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass
import re
import logging

from ..core.cte_integration import UnifiedExecutionContext, CTEFragment
from .cte_converter import create_cql_to_cte_converter, CQLDefineMetadata

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CQLLibraryMetadata:
    """Metadata for a CQL library."""
    library_id: str
    version: str
    defines: Dict[str, str]
    parameters: Dict[str, Any]
    dependencies: List[str]
    estimated_complexity: int = 0


class CQLLibraryParser:
    """
    Parser for CQL library content.

    Extracts define statements, parameters, and metadata from CQL library text,
    preparing it for pipeline execution.
    """

    def __init__(self):
        self.define_pattern = re.compile(
            r'define\s+"([^"]+)"\s*:\s*(.+?)(?=\s*define\s+"|$)',
            re.IGNORECASE | re.DOTALL
        )
        self.parameter_pattern = re.compile(
            r'parameter\s+"([^"]+)"\s*:\s*(.+?)(?=\s*(?:parameter|define|library)\s+"|$)',
            re.IGNORECASE | re.DOTALL
        )
        self.library_pattern = re.compile(
            r'library\s+([^\s]+)(?:\s+version\s+[\'"]([^\'"]+)[\'"])?',
            re.IGNORECASE
        )

    def parse_library(self, cql_content: str, library_id: Optional[str] = None) -> CQLLibraryMetadata:
        """
        Parse CQL library content into structured metadata.

        Args:
            cql_content: CQL library text content
            library_id: Optional library identifier override

        Returns:
            Parsed CQL library metadata
        """
        # Extract library information
        library_info = self._extract_library_info(cql_content, library_id)

        # Extract define statements
        defines = self._extract_defines(cql_content)

        # Extract parameters
        parameters = self._extract_parameters(cql_content)

        # Analyze dependencies
        dependencies = self._analyze_dependencies(defines)

        # Estimate complexity
        complexity = self._estimate_complexity(defines)

        metadata = CQLLibraryMetadata(
            library_id=library_info['id'],
            version=library_info['version'],
            defines=defines,
            parameters=parameters,
            dependencies=dependencies,
            estimated_complexity=complexity
        )

        logger.info(f"Parsed CQL library '{metadata.library_id}' v{metadata.version}: "
                   f"{len(defines)} defines, {len(parameters)} parameters")

        return metadata

    def _extract_library_info(self, cql_content: str, library_id: Optional[str] = None) -> Dict[str, str]:
        """Extract library ID and version information."""
        library_match = self.library_pattern.search(cql_content)

        if library_match:
            extracted_id = library_match.group(1)
            extracted_version = library_match.group(2) or "1.0"
        else:
            extracted_id = library_id or "UnknownLibrary"
            extracted_version = "1.0"

        return {
            'id': library_id or extracted_id,
            'version': extracted_version
        }

    def _extract_defines(self, cql_content: str) -> Dict[str, str]:
        """Extract define statements from CQL content."""
        defines = {}

        for match in self.define_pattern.finditer(cql_content):
            define_name = match.group(1).strip()
            define_expression = match.group(2).strip()

            # Clean up the expression (remove trailing semicolons, etc.)
            define_expression = re.sub(r';\s*$', '', define_expression)

            defines[define_name] = define_expression

        logger.debug(f"Extracted {len(defines)} define statements")
        return defines

    def _extract_parameters(self, cql_content: str) -> Dict[str, Any]:
        """Extract parameter statements from CQL content."""
        parameters = {}

        for match in self.parameter_pattern.finditer(cql_content):
            param_name = match.group(1).strip()
            param_definition = match.group(2).strip()

            # Basic parameter parsing - could be enhanced
            parameters[param_name] = {
                'definition': param_definition,
                'type': self._infer_parameter_type(param_definition)
            }

        logger.debug(f"Extracted {len(parameters)} parameters")
        return parameters

    def _infer_parameter_type(self, param_definition: str) -> str:
        """Infer parameter type from definition."""
        definition_lower = param_definition.lower()

        if 'interval' in definition_lower:
            return 'Interval'
        elif 'datetime' in definition_lower:
            return 'DateTime'
        elif 'date' in definition_lower:
            return 'Date'
        elif 'boolean' in definition_lower:
            return 'Boolean'
        elif 'integer' in definition_lower:
            return 'Integer'
        elif 'decimal' in definition_lower:
            return 'Decimal'
        elif 'string' in definition_lower:
            return 'String'
        else:
            return 'Unknown'

    def _analyze_dependencies(self, defines: Dict[str, str]) -> List[str]:
        """Analyze inter-define dependencies within the library."""
        all_dependencies = set()
        define_names = set(defines.keys())

        for define_name, define_expression in defines.items():
            # Look for references to other defines within this library
            for other_define in define_names:
                if other_define != define_name:
                    # Check if this define references the other
                    if f'"{other_define}"' in define_expression or f"'{other_define}'" in define_expression:
                        all_dependencies.add(other_define)

        return list(all_dependencies)

    def _estimate_complexity(self, defines: Dict[str, str]) -> int:
        """Estimate overall library complexity."""
        total_complexity = 0

        for define_expression in defines.values():
            # Base complexity on expression length
            complexity = len(define_expression) // 100

            # Additional complexity for certain patterns
            if 'union' in define_expression.lower():
                complexity += 3
            if 'intersect' in define_expression.lower():
                complexity += 3
            if 'exists' in define_expression.lower():
                complexity += 2
            if 'such that' in define_expression.lower():
                complexity += 2

            total_complexity += complexity

        return min(total_complexity, 50)  # Cap at 50


class CQLLibraryProcessor:
    """
    Main processor for CQL library execution through unified pipeline.

    This class coordinates the execution of CQL libraries by:
    1. Parsing CQL library content
    2. Converting defines to CTE fragments
    3. Building monolithic queries
    4. Executing through the unified pipeline
    """

    def __init__(self, context: UnifiedExecutionContext):
        """Initialize processor with unified execution context."""
        self.context = context
        self.parser = CQLLibraryParser()
        self.cte_converter = create_cql_to_cte_converter(context)

    def process_library(self, cql_content: str, library_id: Optional[str] = None) -> 'CQLLibraryExecutionResult':
        """
        Process CQL library for execution through unified pipeline.

        Args:
            cql_content: CQL library text content
            library_id: Optional library identifier

        Returns:
            Library execution result with CTE fragments and metadata
        """
        # Parse library content
        library_metadata = self.parser.parse_library(cql_content, library_id)

        # Convert defines to CTE fragments
        cte_fragments = self.cte_converter.convert_defines_to_cte_fragments(
            library_metadata.defines
        )

        # Build execution result
        result = CQLLibraryExecutionResult(
            library_metadata=library_metadata,
            cte_fragments=cte_fragments,
            monolithic_sql=self._build_monolithic_sql(cte_fragments),
            execution_context=self.context
        )

        logger.info(f"Processed CQL library '{library_metadata.library_id}': "
                   f"{len(cte_fragments)} CTE fragments generated")

        return result

    def _build_monolithic_sql(self, cte_fragments: List[CTEFragment]) -> str:
        """Build monolithic SQL from CTE fragments."""
        if not cte_fragments:
            return ""

        # Build WITH clause with all CTEs
        cte_clauses = []
        for fragment in cte_fragments:
            cte_clause = f"{fragment.name} AS (\n{fragment.sql}\n)"
            cte_clauses.append(cte_clause)

        with_clause = "WITH " + ",\n".join(cte_clauses)

        # Main query that selects from all CTEs (for debugging/inspection)
        # In practice, this would be customized based on the specific use case
        main_query = f"""
            SELECT
                '{fragment.name}' as define_name,
                COUNT(*) as result_count
            FROM {fragment.name}
        """ if cte_fragments else "SELECT 1"

        # For multiple CTEs, union all results
        if len(cte_fragments) > 1:
            union_queries = []
            for fragment in cte_fragments:
                union_queries.append(f"""
                    SELECT
                        '{fragment.name}' as define_name,
                        COUNT(*) as result_count
                    FROM {fragment.name}
                """)
            main_query = " UNION ALL ".join(union_queries)

        return f"{with_clause}\n{main_query}"


@dataclass(frozen=True)
class CQLLibraryExecutionResult:
    """Result of CQL library processing."""
    library_metadata: CQLLibraryMetadata
    cte_fragments: List[CTEFragment]
    monolithic_sql: str
    execution_context: UnifiedExecutionContext

    def get_define_sql(self, define_name: str) -> Optional[str]:
        """Get SQL for a specific define."""
        sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '_', define_name).lower()
        for fragment in self.cte_fragments:
            if fragment.name == sanitized_name:
                return fragment.sql
        return None

    def get_execution_summary(self) -> Dict[str, Any]:
        """Get summary of execution preparation."""
        return {
            'library_id': self.library_metadata.library_id,
            'library_version': self.library_metadata.version,
            'defines_count': len(self.library_metadata.defines),
            'cte_fragments_count': len(self.cte_fragments),
            'estimated_complexity': self.library_metadata.estimated_complexity,
            'optimization_enabled': self.execution_context.cte_optimization_enabled,
            'context_id': self.execution_context.get_context_id()
        }


def create_cql_library_processor(context: UnifiedExecutionContext) -> CQLLibraryProcessor:
    """
    Factory function to create CQL library processor.

    Args:
        context: Unified execution context

    Returns:
        Configured CQL library processor
    """
    return CQLLibraryProcessor(context)