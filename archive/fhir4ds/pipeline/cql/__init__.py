"""
CQL Pipeline Extensions for Unified Pipeline Architecture

This module provides CQL-specific extensions to the unified pipeline system,
integrating the proven CTE conversion logic and performance optimizations
from the original cte_pipeline while maintaining compatibility with the
comprehensive FHIRPath pipeline foundation.

Key Components:
- CQL to CTE conversion logic
- CQL define dependency resolution
- CQL library processing and workflow integration
- Performance optimization for CQL workflows
- Legacy compatibility layer

The integration preserves the performance benefits that achieved:
- 79.9% CQL Framework success rate
- 100% Quality Measures success rate
- 5-10x performance improvements through monolithic CTE queries

Usage:
    # High-level workflow execution
    from fhir4ds.pipeline.cql import create_cql_workflow_engine

    engine = create_cql_workflow_engine(dialect)
    result = engine.execute_cql_library(cql_content, "MyLibrary")

    # Low-level CTE conversion
    from fhir4ds.pipeline.cql import create_cql_to_cte_converter

    converter = create_cql_to_cte_converter(context)
    fragments = converter.convert_defines_to_cte_fragments(defines)
"""

from .cte_converter import (
    CQLToCTEConverter,
    EnhancedDependencyDetector,
    ResourceTypeDetector,
    CQLPatternAnalyzer,
    CQLDefineMetadata,
    create_cql_to_cte_converter
)

from .library_processor import (
    CQLLibraryProcessor,
    CQLLibraryParser,
    CQLLibraryMetadata,
    CQLLibraryExecutionResult,
    create_cql_library_processor
)

from .workflow_integration import (
    CQLWorkflowEngine,
    CQLExecutionResult,
    LegacyCompatibilityLayer,
    create_cql_workflow_engine,
    create_legacy_compatibility_layer
)

__all__ = [
    # CTE Conversion
    'CQLToCTEConverter',
    'EnhancedDependencyDetector',
    'ResourceTypeDetector',
    'CQLPatternAnalyzer',
    'CQLDefineMetadata',
    'create_cql_to_cte_converter',

    # Library Processing
    'CQLLibraryProcessor',
    'CQLLibraryParser',
    'CQLLibraryMetadata',
    'CQLLibraryExecutionResult',
    'create_cql_library_processor',

    # Workflow Integration
    'CQLWorkflowEngine',
    'CQLExecutionResult',
    'LegacyCompatibilityLayer',
    'create_cql_workflow_engine',
    'create_legacy_compatibility_layer'
]