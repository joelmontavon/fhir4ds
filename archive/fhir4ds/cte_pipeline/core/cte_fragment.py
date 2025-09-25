"""
CTE Fragment Data Model

This module defines the core data structures for representing Common Table Expression
fragments that can be combined into monolithic SQL queries.

A CTEFragment represents a single CTE that corresponds to one CQL define statement,
containing all the information needed to generate optimized SQL for both DuckDB and PostgreSQL.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CTEFragment:
    """
    Represents a single CTE that can be combined with others into a monolithic query.
    
    This replaces the individual query approach by allowing multiple define statements
    to be expressed as CTEs within a single SQL query, enabling database-level
    optimization and eliminating multiple round trips.
    
    Integration with existing architecture:
    - Uses existing resource type detection from functions.py
    - Leverages established JSON extraction patterns from dialects
    - Builds on proven patient ID extraction logic from workflow_engine.py
    """
    
    # Core CTE identity
    name: str                           # CTE alias name (e.g., "patient_pop_001")
    resource_type: str                  # FHIR resource type from existing detection logic
    patient_id_extraction: str          # SQL expression for extracting patient ID
    
    # CTE structure
    select_fields: List[str]            # Fields to select in CTE
    from_clause: str                    # Base table/subquery reference
    where_conditions: List[str]         # WHERE clause conditions
    dependencies: List[str] = field(default_factory=list)  # Other CTEs this depends on
    
    # Integration with existing architecture
    source_cql_expression: str = ""     # Original CQL expression for debugging
    complexity_score: int = 0           # Complexity estimate for optimization decisions
    
    # Metadata
    define_name: str = ""               # Original CQL define name
    result_type: str = "boolean"        # Expected result type (boolean, integer, etc.)
    
    def to_sql(self, dialect: str) -> str:
        """
        Generate SQL for this CTE fragment using existing dialect patterns.
        
        Leverages the same JSON extraction patterns used throughout the existing
        pipeline infrastructure for consistent cross-database compatibility.
        
        Args:
            dialect: Database dialect ('DUCKDB' or 'POSTGRESQL')
            
        Returns:
            SQL string for this CTE fragment
        """
        # Generate SELECT clause
        if self.select_fields:
            select_clause = ', '.join(self.select_fields)
        else:
            # Default: patient ID and boolean result
            patient_id_sql = self.get_patient_id_sql(dialect)
            select_clause = f"{patient_id_sql} as patient_id, true as result"
        
        # Generate WHERE clause
        where_clause = ""
        if self.where_conditions:
            where_clause = f"WHERE {' AND '.join(self.where_conditions)}"
        
        # Build complete CTE
        cte_sql = f"""
        {self.name} AS (
            SELECT {select_clause}
            FROM {self.from_clause}
            {where_clause}
        )"""
        
        logger.debug(f"Generated CTE SQL for {self.name}: {len(cte_sql)} characters")
        return cte_sql.strip()
    
    def get_patient_id_sql(self, dialect: str) -> str:
        """
        Get patient ID extraction SQL using existing patterns from workflow_engine.py.
        
        This method reuses the proven patient ID extraction logic that handles
        different FHIR resource types correctly across both database dialects.
        
        Args:
            dialect: Database dialect ('DUCKDB' or 'POSTGRESQL')
            
        Returns:
            SQL expression for extracting patient ID
        """
        dialect_upper = dialect.upper()
        
        if self.resource_type == 'Patient':
            # Direct patient ID extraction
            if dialect_upper == 'DUCKDB':
                return "json_extract_string(resource, '$.id')"
            else:  # PostgreSQL
                return "jsonb_extract_path_text(resource, 'id')"
        else:
            # Subject reference extraction (existing pattern for all other resources)
            if dialect_upper == 'DUCKDB':
                return """CASE 
                    WHEN json_extract_string(resource, '$.subject.reference') LIKE 'Patient/%'
                    THEN REPLACE(json_extract_string(resource, '$.subject.reference'), 'Patient/', '')
                    ELSE NULL
                END"""
            else:  # PostgreSQL
                return """CASE 
                    WHEN jsonb_extract_path_text(resource, 'subject', 'reference') LIKE 'Patient/%'
                    THEN REPLACE(jsonb_extract_path_text(resource, 'subject', 'reference'), 'Patient/', '')
                    ELSE NULL
                END"""
    
    def get_resource_type_filter_sql(self, dialect: str) -> str:
        """
        Generate resource type filter SQL using existing dialect patterns.
        
        Args:
            dialect: Database dialect ('DUCKDB' or 'POSTGRESQL')
            
        Returns:
            SQL condition for filtering by resource type
        """
        dialect_upper = dialect.upper()
        
        if dialect_upper == 'DUCKDB':
            return f"json_extract_string(resource, '$.resourceType') = '{self.resource_type}'"
        else:  # PostgreSQL
            return f"jsonb_extract_path_text(resource, 'resourceType') = '{self.resource_type}'"
    
    def estimate_complexity(self) -> int:
        """
        Estimate the complexity of this CTE fragment for optimization decisions.
        
        Returns:
            Complexity score (higher = more complex)
        """
        complexity = 0
        
        # Base complexity
        complexity += len(self.where_conditions) * 2
        complexity += len(self.dependencies) * 3
        complexity += len(self.select_fields)
        
        # Resource type complexity
        if self.resource_type != 'Patient':
            complexity += 2  # Subject reference extraction is more complex
        
        # Update stored complexity score
        object.__setattr__(self, 'complexity_score', complexity)
        return complexity
    
    def validate(self) -> List[str]:
        """
        Validate this CTE fragment for correctness.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        if not self.name:
            errors.append("CTE name is required")
        
        if not self.resource_type:
            errors.append("Resource type is required")
        
        if not self.from_clause:
            errors.append("FROM clause is required")
        
        # Validate SQL identifier naming
        if self.name and not self.name.replace('_', '').replace('-', '').isalnum():
            errors.append(f"CTE name '{self.name}' contains invalid characters")
        
        # Validate known resource types
        known_resource_types = {
            'Patient', 'Condition', 'MedicationDispense', 'Encounter', 
            'Observation', 'Procedure', 'DiagnosticReport', 'Immunization'
        }
        if self.resource_type and self.resource_type not in known_resource_types:
            logger.warning(f"Unknown resource type: {self.resource_type}")
        
        return errors
    
    @classmethod
    def create_patient_population_cte(cls, dialect: str) -> 'CTEFragment':
        """
        Create a standard patient population CTE using existing optimization patterns.
        
        This creates the base patient population CTE that other fragments can join against,
        following the same patterns used in existing population optimization code.
        
        Args:
            dialect: Database dialect ('DUCKDB' or 'POSTGRESQL')
            
        Returns:
            CTEFragment for patient population
        """
        dialect_upper = dialect.upper()
        
        if dialect_upper == 'DUCKDB':
            select_fields = [
                "json_extract_string(resource, '$.id') as patient_id",
                "json_extract_string(resource, '$.birthDate') as birth_date",
                "json_extract_string(resource, '$.gender') as gender",
                "EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM CAST(json_extract_string(resource, '$.birthDate') AS DATE)) as age"
            ]
            where_conditions = ["json_extract_string(resource, '$.resourceType') = 'Patient'"]
        else:  # PostgreSQL
            select_fields = [
                "jsonb_extract_path_text(resource, 'id') as patient_id",
                "jsonb_extract_path_text(resource, 'birthDate') as birth_date",
                "jsonb_extract_path_text(resource, 'gender') as gender",
                "EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM (jsonb_extract_path_text(resource, 'birthDate'))::DATE) as age"
            ]
            where_conditions = ["jsonb_extract_path_text(resource, 'resourceType') = 'Patient'"]
        
        return cls(
            name="patient_population",
            resource_type="Patient",
            patient_id_extraction="patient_id",  # Already extracted as field
            select_fields=select_fields,
            from_clause="fhir_resources",
            where_conditions=where_conditions,
            define_name="PatientPopulation",
            result_type="population"
        )
    
    @classmethod  
    def create_from_cql_define(cls, define_name: str, cql_expression: str, 
                              resource_type: str, dialect: str) -> 'CTEFragment':
        """
        Create a CTE fragment from a CQL define statement.
        
        This is a factory method that converts CQL define statements into CTE fragments
        using the established patterns for resource type detection and SQL generation.
        
        Args:
            define_name: Name of the CQL define
            cql_expression: The CQL expression
            resource_type: FHIR resource type
            dialect: Database dialect
            
        Returns:
            CTEFragment representing the CQL define as a CTE
        """
        # Normalize define name to valid SQL identifier
        cte_name = define_name.lower().replace(' ', '_').replace('-', '_')
        cte_name = ''.join(c for c in cte_name if c.isalnum() or c == '_')
        
        # Generate patient ID extraction
        fragment = cls(
            name=cte_name,
            resource_type=resource_type,
            patient_id_extraction="",  # Will be set by get_patient_id_sql
            select_fields=[],  # Will be populated based on CQL analysis
            from_clause="fhir_resources",
            where_conditions=[],  # Will be populated based on CQL analysis
            source_cql_expression=cql_expression,
            define_name=define_name
        )
        
        # Set patient ID extraction
        object.__setattr__(fragment, 'patient_id_extraction', fragment.get_patient_id_sql(dialect))
        
        # Add resource type filter
        resource_filter = fragment.get_resource_type_filter_sql(dialect)
        object.__setattr__(fragment, 'where_conditions', [resource_filter])
        
        # Set default select fields
        patient_id_sql = fragment.get_patient_id_sql(dialect)
        select_fields = [f"{patient_id_sql} as patient_id", "true as result"]
        object.__setattr__(fragment, 'select_fields', select_fields)
        
        return fragment