"""
ValueSet CTE Generator

This module generates Common Table Expressions (CTEs) from FHIR ValueSet resources,
enabling efficient terminology-based queries in CQL-to-SQL translation.

The ValueSet CTEs contain all clinical codes from expanded ValueSets, allowing
SQL queries to reference these codes via JOINs instead of inline literals.
"""

from typing import Dict, List, Any, Optional
import logging
from ..core.cte_fragment import CTEFragment

logger = logging.getLogger(__name__)


class ValueSetCTEGenerator:
    """
    Generator for creating ValueSet CTEs from FHIR ValueSet resources.

    This class converts FHIR ValueSet resources (typically retrieved from VSAC)
    into CTE fragments that can be included in monolithic SQL queries for
    efficient terminology-based filtering.

    Architecture integration:
    - Uses CTEFragment structure for consistency with CTE Pipeline
    - Supports both DuckDB and PostgreSQL dialects
    - Generates standard SQL that integrates with existing query builder
    """

    def __init__(self, dialect: str = "duckdb"):
        """
        Initialize ValueSet CTE generator.

        Args:
            dialect: Target SQL dialect ("duckdb" or "postgresql")
        """
        self.dialect = dialect
        self.logger = logger

    def generate_valueset_cte(self, valueset_resource: Dict[str, Any], preferred_name: Optional[str] = None) -> CTEFragment:
        """
        Generate CTE fragment from FHIR ValueSet resource with expansion.

        Args:
            valueset_resource: FHIR ValueSet resource with expansion.contains[]
            preferred_name: Optional preferred name for the CTE (overrides resource name)

        Returns:
            CTEFragment containing ValueSet codes as a CTE

        Raises:
            ValueError: If ValueSet resource is invalid or missing expansion
        """
        # Validate ValueSet resource
        self._validate_valueset_resource(valueset_resource)

        # Extract ValueSet metadata - use preferred name if provided
        if preferred_name:
            valueset_name = preferred_name
        else:
            valueset_name = valueset_resource.get('name', valueset_resource.get('title', 'unknown_valueset'))
        valueset_id = valueset_resource.get('id', valueset_name.lower().replace(' ', '_'))

        # Extract codes from ValueSet expansion
        codes = self.extract_codes_from_valueset(valueset_resource)

        if not codes:
            self.logger.warning(f"ValueSet '{valueset_name}' has no codes in expansion")

        # Generate CTE name (sanitize for SQL)
        cte_name = self._generate_cte_name(valueset_name)

        # Build ValueSet CTE SQL
        cte_sql = self._build_valueset_cte_sql(cte_name, codes)

        # Create CTEFragment
        cte_fragment = CTEFragment(
            name=cte_name,
            resource_type="ValueSet",  # Special type for ValueSet CTEs
            patient_id_extraction="NULL",  # ValueSet CTEs don't have patient IDs
            select_fields=["code", "system", "display"],
            from_clause=f"({cte_sql}) AS {cte_name}",
            where_conditions=[],  # No WHERE conditions for ValueSet CTEs
            dependencies=[],  # ValueSet CTEs have no dependencies
            source_cql_expression=f"ValueSet: {valueset_name}",
            complexity_score=1,  # Low complexity
            define_name=f"valueset_{cte_name}",
            result_type="codes"
        )

        self.logger.info(f"Generated ValueSet CTE '{cte_name}' with {len(codes)} codes")
        return cte_fragment

    def extract_codes_from_valueset(self, valueset_resource: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Extract all codes from ValueSet.expansion.contains[].

        Args:
            valueset_resource: FHIR ValueSet resource with expansion

        Returns:
            List of code dictionaries with 'code', 'system', 'display'
        """
        expansion = valueset_resource.get('expansion', {})
        contains = expansion.get('contains', [])

        codes = []
        for item in contains:
            if isinstance(item, dict) and 'code' in item:
                codes.append({
                    'code': item.get('code', ''),
                    'system': item.get('system', ''),
                    'display': item.get('display', '')
                })

        return codes

    def _build_valueset_cte_sql(self, cte_name: str, codes: List[Dict[str, str]]) -> str:
        """
        Build dialect-specific SQL for ValueSet CTE.

        Args:
            cte_name: Name for the CTE
            codes: List of code dictionaries

        Returns:
            SQL string for the ValueSet CTE
        """
        if not codes:
            # Return empty CTE with correct structure
            return f"""
            SELECT
                CAST(NULL AS TEXT) AS code,
                CAST(NULL AS TEXT) AS system,
                CAST(NULL AS TEXT) AS display
            WHERE FALSE"""

        # Generate VALUES clauses for each code
        values_clauses = []
        for code in codes:
            # Escape single quotes in SQL strings
            code_val = self._escape_sql_string(code['code'])
            system_val = self._escape_sql_string(code['system'])
            display_val = self._escape_sql_string(code['display'])

            values_clauses.append(f"('{code_val}', '{system_val}', '{display_val}')")

        # Build the complete CTE SQL
        values_sql = ",\n            ".join(values_clauses)

        return f"""
        SELECT
            code,
            system,
            display
        FROM (
            VALUES
            {values_sql}
        ) AS codes(code, system, display)"""

    def _validate_valueset_resource(self, valueset_resource: Dict[str, Any]) -> None:
        """
        Validate that ValueSet resource has required structure.

        Args:
            valueset_resource: FHIR ValueSet resource to validate

        Raises:
            ValueError: If resource is invalid
        """
        if not isinstance(valueset_resource, dict):
            raise ValueError("ValueSet resource must be a dictionary")

        if valueset_resource.get('resourceType') != 'ValueSet':
            raise ValueError(f"Resource type must be 'ValueSet', got '{valueset_resource.get('resourceType')}'")

        if 'expansion' not in valueset_resource:
            raise ValueError("ValueSet resource must have 'expansion' section")

        if 'contains' not in valueset_resource.get('expansion', {}):
            raise ValueError("ValueSet expansion must have 'contains' array")

    def _generate_cte_name(self, valueset_name: str) -> str:
        """
        Generate valid SQL identifier for CTE name from ValueSet name.

        Args:
            valueset_name: Original ValueSet name

        Returns:
            Sanitized CTE name safe for SQL
        """
        # Convert to lowercase and replace non-alphanumeric with underscores
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', valueset_name.lower())

        # Ensure it starts with a letter
        if not sanitized[0].isalpha():
            sanitized = f"vs_{sanitized}"

        # Add suffix to indicate it's a ValueSet CTE (only if not already present)
        if not sanitized.endswith('_vs'):
            sanitized = f"{sanitized}_vs"
        return sanitized

    def _escape_sql_string(self, value: str) -> str:
        """
        Escape single quotes in SQL string values.

        Args:
            value: String value to escape

        Returns:
            Escaped string safe for SQL literals
        """
        if not isinstance(value, str):
            return str(value)

        # Replace single quotes with two single quotes
        return value.replace("'", "''")

    def get_valueset_cte_reference_sql(self, cte_name: str, resource_field_path: str) -> str:
        """
        Generate SQL for referencing ValueSet CTE in WHERE clauses.

        Args:
            cte_name: Name of the ValueSet CTE
            resource_field_path: JSON path to the code field in FHIR resource

        Returns:
            SQL condition for matching against ValueSet codes
        """
        if self.dialect == "postgresql":
            return f"""
            EXISTS (
                SELECT 1 FROM {cte_name} vs
                WHERE jsonb_extract_path_text(resource, '{resource_field_path}') = vs.code
            )"""
        else:  # DuckDB
            return f"""
            EXISTS (
                SELECT 1 FROM {cte_name} vs
                WHERE json_extract_string(resource, '$.{resource_field_path}') = vs.code
            )"""