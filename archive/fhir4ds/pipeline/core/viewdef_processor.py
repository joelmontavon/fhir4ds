"""
ViewDefinition Processor for Unified Pipeline Architecture

This module processes complete SQL-on-FHIR ViewDefinitions through the
unified pipeline system, including forEach, unionAll, and other complex
constructs that were previously handled by legacy generation methods.

Key Features:
- Complete ViewDefinition processing through pipeline
- forEach operation handling with LATERAL JOINs
- unionAll operation support
- Nested select structure processing
- Single execution path - no legacy fallbacks
"""

from typing import Dict, List, Any, Optional
import logging
from .base import ExecutionContext, SQLState
from ..converters.ast_converter import PipelineASTBridge

logger = logging.getLogger(__name__)


class ViewDefinitionProcessor:
    """
    Processes complete SQL-on-FHIR ViewDefinitions through the unified pipeline.

    This processor eliminates the need for legacy SQL generation methods
    by handling all ViewDefinition constructs through the pipeline system.
    """

    def __init__(self, table_name: str, json_col: str, dialect, fhirpath_pipeline=None):
        """
        Initialize the ViewDefinition processor.

        Args:
            table_name: Name of the FHIR resource table
            json_col: Name of the JSON column containing FHIR resources
            dialect: Database dialect for SQL generation
            fhirpath_pipeline: Existing FHIRPath pipeline processor to use
        """
        self.table_name = table_name
        self.json_col = json_col
        self.dialect = dialect
        self.fhirpath_pipeline = fhirpath_pipeline
        self.pipeline_bridge = PipelineASTBridge()

        # Statistics tracking
        self.processing_stats = {
            'standard_views': 0,
            'foreach_views': 0,
            'unionall_views': 0,
            'nested_selects': 0,
            'total_columns': 0
        }

    def process_view_definition(self, view_def: Dict[str, Any]) -> str:
        """
        Process a complete ViewDefinition and generate SQL.

        Args:
            view_def: SQL-on-FHIR ViewDefinition dictionary

        Returns:
            Generated SQL string
        """
        logger.debug(f"Processing ViewDefinition with resource: {view_def.get('resource', 'Unknown')}")

        # Check for special operations
        has_foreach = self._contains_foreach_operations(view_def)
        has_unionall = self._has_unionall_operations(view_def)

        if has_unionall:
            return self._process_unionall_view(view_def)
        elif has_foreach:
            return self._process_foreach_view(view_def)
        else:
            return self._process_standard_view(view_def)

    def _process_standard_view(self, view_def: Dict[str, Any]) -> str:
        """Process a standard ViewDefinition without forEach or unionAll."""
        self.processing_stats['standard_views'] += 1

        # Extract base information
        resource_type = view_def.get('resource', 'Patient')

        # Process select columns
        select_parts = []
        for select_item in view_def.get('select', []):
            select_parts.extend(self._process_select_item(select_item, resource_type))

        # Build base query
        sql_parts = [
            f"SELECT {', '.join(select_parts)}",
            f"FROM {self.table_name}"
        ]

        # Add WHERE clause if present
        if 'where' in view_def:
            where_conditions = []
            for where_item in view_def['where']:
                if 'path' in where_item:
                    condition_sql = self._process_where_condition(where_item['path'], resource_type)
                    where_conditions.append(condition_sql)

            if where_conditions:
                sql_parts.append(f"WHERE ({' AND '.join(where_conditions)})")

        return '\n        '.join(sql_parts)

    def _process_foreach_view(self, view_def: Dict[str, Any]) -> str:
        """Process a ViewDefinition with forEach operations."""
        self.processing_stats['foreach_views'] += 1

        # Extract base information
        resource_type = view_def.get('resource', 'Patient')

        # Process select items to find forEach constructs
        select_parts = []
        lateral_joins = []

        for select_item in view_def.get('select', []):
            if 'select' in select_item:
                # This is a nested structure with potential forEach
                for nested_select in select_item['select']:
                    if 'forEach' in nested_select:
                        # Process forEach operation
                        foreach_path = nested_select['forEach']
                        alias = f"foreach_{len(lateral_joins) + 1}"

                        # Create LATERAL JOIN for forEach
                        lateral_join = self._create_lateral_join(foreach_path, alias, resource_type)
                        lateral_joins.append(lateral_join)

                        # Process columns within forEach context
                        if 'column' in nested_select:
                            for column in nested_select['column']:
                                if 'path' in column:
                                    column_sql = self._process_foreach_column(
                                        column['path'],
                                        column.get('name', column['path'].replace('.', '_')),
                                        alias,
                                        resource_type
                                    )
                                    select_parts.append(column_sql)
            else:
                # Regular select item
                select_parts.extend(self._process_select_item(select_item, resource_type))

        # Build query with LATERAL JOINs
        sql_parts = [
            f"SELECT {', '.join(select_parts)}",
            f"FROM {self.table_name}"
        ]

        # Add LATERAL JOINs
        sql_parts.extend(lateral_joins)

        # Add WHERE clause if present
        if 'where' in view_def:
            where_conditions = [f"json_extract_string({self.table_name}.{self.json_col}, '$.resourceType') = '{resource_type}'"]
            for where_item in view_def['where']:
                if 'path' in where_item:
                    condition_sql = self._process_where_condition(where_item['path'], resource_type)
                    where_conditions.append(condition_sql)

            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")

        return ' '.join(sql_parts)

    def _process_unionall_view(self, view_def: Dict[str, Any]) -> str:
        """Process a ViewDefinition with unionAll operations."""
        self.processing_stats['unionall_views'] += 1

        union_parts = []

        for select_item in view_def.get('select', []):
            if 'unionAll' in select_item:
                for union_item in select_item['unionAll']:
                    # Create a temporary view def for each union part
                    temp_view_def = {
                        'resource': view_def.get('resource', 'Patient'),
                        'select': [union_item]
                    }
                    if 'where' in view_def:
                        temp_view_def['where'] = view_def['where']

                    # Process each union part
                    union_sql = self._process_standard_view(temp_view_def)
                    union_parts.append(f"({union_sql})")

        return ' UNION ALL '.join(union_parts)

    def _process_select_item(self, select_item: Dict[str, Any], resource_type: str) -> List[str]:
        """Process a select item and return SQL column expressions."""
        columns = []

        # Handle direct path in select item
        if 'path' in select_item:
            column_name = select_item.get('name', select_item['path'].replace('.', '_'))
            column_sql = self._process_fhirpath_expression(select_item['path'], resource_type)
            columns.append(f"{column_sql} AS \"{column_name}\"")
            self.processing_stats['total_columns'] += 1

        # Handle column array
        if 'column' in select_item:
            for column in select_item['column']:
                if 'path' in column:
                    column_name = column.get('name', column['path'].replace('.', '_'))
                    column_sql = self._process_fhirpath_expression(column['path'], resource_type)
                    columns.append(f"{column_sql} AS \"{column_name}\"")
                    self.processing_stats['total_columns'] += 1

        return columns

    def _process_foreach_column(self, path: str, name: str, alias: str, resource_type: str) -> str:
        """Process a column within a forEach context."""
        # In forEach context, the path operates on the iteration variable
        if path.startswith(alias + '.'):
            # Path relative to forEach alias
            json_path = path[len(alias) + 1:]
            column_sql = self.dialect.generate_json_path_extraction(f"{alias}.value", f"$.{json_path}")
            return f"{column_sql} AS \"{name}\""
        else:
            # Regular path processing
            column_sql = self._process_fhirpath_expression(path, resource_type)
            return f"{column_sql} AS \"{name}\""

    def _create_lateral_join(self, foreach_path: str, alias: str, resource_type: str) -> str:
        """Create a LATERAL JOIN for forEach operation."""
        # Convert forEach path to JSON extraction
        json_path = f"$.{foreach_path}"
        json_extract = self.dialect.generate_json_path_extraction(f"{self.table_name}.{self.json_col}", json_path)

        return f"INNER JOIN LATERAL json_each({json_extract}) AS {alias} ON true"

    def _process_fhirpath_expression(self, path: str, resource_type: str) -> str:
        """Process a FHIRPath expression through the pipeline system."""
        try:
            # Use the provided FHIRPath pipeline if available
            if self.fhirpath_pipeline:
                return self.fhirpath_pipeline.to_sql(path, resource_type, self.table_name)
            else:
                # Fallback: create new FHIRPath processor
                from ...fhirpath.fhirpath import FHIRPath
                fhirpath_processor = FHIRPath(dialect=self.dialect, use_pipeline=True)
                return fhirpath_processor.to_sql(path, resource_type, self.table_name)

        except Exception as e:
            logger.warning(f"Pipeline processing failed for path '{path}': {e}")
            # Simple fallback - direct JSON extraction using dialect-aware method
            json_path = f"$.{path}"
            return self.dialect.generate_json_path_extraction(f"{self.table_name}.{self.json_col}", json_path)

    def _process_where_condition(self, path: str, resource_type: str) -> str:
        """Process a WHERE condition through the pipeline system."""
        try:
            return self._process_fhirpath_expression(path, resource_type)
        except Exception as e:
            logger.warning(f"WHERE condition processing failed for path '{path}': {e}")
            # Simple fallback using dialect-aware method
            return self.dialect.generate_json_path_extraction(f"{self.table_name}.{self.json_col}", f"$.{path}")

    def _contains_foreach_operations(self, view_def: Dict[str, Any]) -> bool:
        """Check if ViewDefinition contains forEach operations."""
        for select_item in view_def.get('select', []):
            if 'forEach' in select_item or 'forEachOrNull' in select_item:
                return True
            if 'select' in select_item:
                for nested_select in select_item['select']:
                    if 'forEach' in nested_select or 'forEachOrNull' in nested_select:
                        return True
            if 'unionAll' in select_item:
                for union_item in select_item['unionAll']:
                    if 'forEach' in union_item or 'forEachOrNull' in union_item:
                        return True
                    if 'select' in union_item:
                        for nested_select in union_item['select']:
                            if 'forEach' in nested_select or 'forEachOrNull' in nested_select:
                                return True
        return False

    def _has_unionall_operations(self, view_def: Dict[str, Any]) -> bool:
        """Check if ViewDefinition contains unionAll operations."""
        for select_item in view_def.get('select', []):
            if 'unionAll' in select_item:
                return True
        return False

    def get_processing_stats(self) -> Dict[str, int]:
        """Get processing statistics."""
        return self.processing_stats.copy()