"""
CQL Context Management - Context evaluation for Patient vs Population contexts.

This module provides context-aware evaluation capabilities for CQL expressions,
supporting different evaluation contexts like Patient, Population, Practitioner, etc.
"""

import logging
from typing import Dict, Any, Optional, List, Union
from enum import Enum

logger = logging.getLogger(__name__)

class CQLContextType(Enum):
    """CQL evaluation context types."""
    PATIENT = "Patient"
    POPULATION = "Population" 
    PRACTITIONER = "Practitioner"
    ENCOUNTER = "Encounter"
    EPISODE = "Episode"
    UNFILTERED = "Unfiltered"

class CQLContext:
    """
    CQL evaluation context manager.
    
    Manages context-specific evaluation behavior, resource filtering,
    and population-level vs patient-level operations.
    """
    
    def __init__(self, context_type: Union[str, CQLContextType] = CQLContextType.PATIENT, dialect=None):
        """
        Initialize CQL context.
        
        Args:
            context_type: The evaluation context type
            dialect: Database dialect for SQL generation compatibility
        """
        if isinstance(context_type, str):
            try:
                self.context_type = CQLContextType(context_type)
            except ValueError:
                logger.warning(f"Unknown context type '{context_type}', defaulting to Patient")
                self.context_type = CQLContextType.PATIENT
        else:
            self.context_type = context_type
            
        self.current_patient_id = None
        self.population_filters = {}
        self.context_variables = {}
        self.resource_filters = {}
        self.dialect = dialect  # For cross-dialect compatibility
        
        logger.debug(f"Initialized CQL context: {self.context_type.value}")
    
    def set_patient_context(self, patient_id: str):
        """
        Set specific patient for patient-level evaluation.
        
        Args:
            patient_id: Patient identifier
        """
        self.current_patient_id = patient_id
        self.context_type = CQLContextType.PATIENT
        logger.debug(f"Set patient context: {patient_id}")
    
    def set_population_context(self, filters: Optional[Dict[str, Any]] = None):
        """
        Set population-level evaluation with optional filters.
        
        Args:
            filters: Population-level filters
        """
        self.context_type = CQLContextType.POPULATION
        self.population_filters = filters or {}
        self.current_patient_id = None
        logger.debug(f"Set population context with {len(self.population_filters)} filters")
    
    def reset_to_population_analytics(self):
        """
        Reset context to population-first analytics mode.
        
        This clears any single-patient override and returns to 
        population-first processing mode.
        """
        self.current_patient_id = None
        self.population_filters = {}
        if self.context_type not in [CQLContextType.POPULATION, CQLContextType.PATIENT, 
                                   CQLContextType.PRACTITIONER, CQLContextType.ENCOUNTER]:
            self.context_type = CQLContextType.POPULATION
        logger.debug(f"Reset to population analytics mode: {self.get_context_mode()}")
    
    def generate_context_sql_filter(self, table_alias: str = "resource") -> str:
        """
        Generate SQL WHERE clause for current context.
        
        Args:
            table_alias: Table alias for the resource table
            
        Returns:
            SQL WHERE clause fragment
        """
        if self.context_type == CQLContextType.PATIENT and self.current_patient_id:
            # Patient-specific filtering (single patient override)
            subject_ref_extract = self._get_dialect_compatible_json_extract("'$.subject.reference'", table_alias)
            return f"{subject_ref_extract} = 'Patient/{self.current_patient_id}'"
        
        elif self.context_type == CQLContextType.POPULATION:
            # Population-level - potentially no filtering or demographic filters
            filters = []
            
            # Add population-level filters
            for key, value in self.population_filters.items():
                if key == 'ageRange':
                    # Age-based filtering with dialect compatibility
                    min_age, max_age = value
                    birth_date_extract = self._get_dialect_compatible_json_extract("'$.birthDate'", table_alias)
                    age_calculation = self._get_dialect_compatible_date_diff(birth_date_extract)
                    age_filter = f"CAST({age_calculation} AS INTEGER) BETWEEN {min_age} AND {max_age}"
                    filters.append(age_filter)
                elif key == 'gender':
                    gender_extract = self._get_dialect_compatible_json_extract("'$.gender'", table_alias)
                    filters.append(f"{gender_extract} = '{value}'")
                elif key == 'resourceType':
                    resource_type_extract = self._get_dialect_compatible_json_extract("'$.resourceType'", table_alias)
                    filters.append(f"{resource_type_extract} = '{value}'")
            
            return ' AND '.join(filters) if filters else "1=1"
        
        else:
            # No context filtering
            return "1=1"

    def generate_context_group_by(self, table_alias: str = "resource") -> str:
        """
        Generate GROUP BY clause for population-first processing.
        
        Args:
            table_alias: Table alias for the resource table
            
        Returns:
            GROUP BY clause fragment or empty string
        """
        # Single-patient override: No grouping when specific patient ID is set
        if self.current_patient_id:
            return ""
        
        # Population-first processing: Add grouping based on context type
        if self.context_type == CQLContextType.PATIENT:
            # Population-level patient context - group by patient
            return self._get_dialect_compatible_json_extract("'$.subject.reference'", table_alias)
        
        elif self.context_type == CQLContextType.PRACTITIONER:
            # Practitioner-level context - group by practitioner
            return self._get_dialect_compatible_json_extract("'$.performer[0].reference'", table_alias)
            
        elif self.context_type == CQLContextType.ENCOUNTER:
            # Encounter-level context - group by encounter
            return self._get_dialect_compatible_json_extract("'$.encounter.reference'", table_alias)
            
        elif self.context_type == CQLContextType.POPULATION:
            # For population context, default to patient-level grouping
            return self._get_dialect_compatible_json_extract("'$.subject.reference'", table_alias)
        
        else:
            # No grouping for unfiltered contexts
            return ""

    def add_context_columns(self, base_select: str, table_alias: str = "resource") -> str:
        """
        Add context-aware columns to SELECT clause for population analytics.
        
        Args:
            base_select: Base SELECT clause
            table_alias: Table alias for the resource table
            
        Returns:
            Enhanced SELECT clause with context columns
        """
        # Single-patient override: No additional columns when specific patient ID is set
        if self.current_patient_id:
            return base_select
        
        # Population-first processing: Add context identifier columns
        if self.context_type == CQLContextType.PATIENT:
            # Population-level patient analysis - add patient_id
            if "patient_id" not in base_select.lower():
                patient_extract = self._get_dialect_compatible_json_extract("'$.subject.reference'", table_alias)
                return f"{base_select}, {patient_extract} as patient_id"
                
        elif self.context_type == CQLContextType.PRACTITIONER:
            # Practitioner-level analysis - add practitioner_id
            if "practitioner_id" not in base_select.lower():
                practitioner_extract = self._get_dialect_compatible_json_extract("'$.performer[0].reference'", table_alias)
                return f"{base_select}, {practitioner_extract} as practitioner_id"
                
        elif self.context_type == CQLContextType.ENCOUNTER:
            # Encounter-level analysis - add encounter_id
            if "encounter_id" not in base_select.lower():
                encounter_extract = self._get_dialect_compatible_json_extract("'$.encounter.reference'", table_alias)
                return f"{base_select}, {encounter_extract} as encounter_id"
                
        elif self.context_type == CQLContextType.POPULATION:
            # Population context - add patient_id for analytics
            if "patient_id" not in base_select.lower():
                patient_extract = self._get_dialect_compatible_json_extract("'$.subject.reference'", table_alias)
                return f"{base_select}, {patient_extract} as patient_id"
        
        return base_select
    
    def is_patient_context(self) -> bool:
        """Check if current context is patient-level."""
        return self.context_type == CQLContextType.PATIENT
    
    def is_population_context(self) -> bool:
        """Check if current context is population-level."""
        return self.context_type == CQLContextType.POPULATION
    
    def is_single_patient_mode(self) -> bool:
        """Check if we're in single-patient override mode."""
        return self.current_patient_id is not None
    
    def is_population_analytics_mode(self) -> bool:
        """Check if we're in population-first analytics mode."""
        return not self.is_single_patient_mode()
    
    def get_context_mode(self) -> str:
        """Get human-readable description of current context mode."""
        if self.is_single_patient_mode():
            return f"Single Patient ({self.current_patient_id})"
        elif self.context_type == CQLContextType.POPULATION:
            filter_count = len(self.population_filters)
            return f"Population Analytics ({filter_count} filters)"
        elif self.context_type == CQLContextType.PATIENT:
            return "Population-Level Patient Analytics"
        elif self.context_type == CQLContextType.PRACTITIONER:
            return "Practitioner-Level Analytics"
        elif self.context_type == CQLContextType.ENCOUNTER:
            return "Encounter-Level Analytics"
        else:
            return f"Unfiltered ({self.context_type.value})"
    
    def get_context_resource_type(self) -> str:
        """Get the primary resource type for current context."""
        return self.context_type.value
    
    def generate_vectorized_sql_wrapper(self, base_query: str, table_name: str = "fhir_resources") -> str:
        """
        Generate vectorized SQL wrapper for population-first processing.
        
        This method wraps queries with advanced SQL patterns like window functions,
        efficient aggregations, and population-level optimizations.
        
        Args:
            base_query: Base SQL query to wrap
            table_name: FHIR resources table name
            
        Returns:
            Vectorized SQL query optimized for population analytics
        """
        # Single-patient mode: no vectorization needed
        if self.is_single_patient_mode():
            return base_query
        
        # Population mode: apply vectorization
        return self._apply_vectorization_patterns(base_query, table_name)
    
    def _apply_vectorization_patterns(self, base_query: str, table_name: str) -> str:
        """
        Apply vectorization patterns to SQL for population analytics.
        
        Args:
            base_query: Base SQL query
            table_name: FHIR resources table name
            
        Returns:
            Vectorized SQL with population optimizations
        """
        base_query_upper = base_query.upper()
        
        # Check if query already has advanced features
        has_window_function = "OVER(" in base_query_upper or "PARTITION BY" in base_query_upper
        has_cte = "WITH " in base_query_upper
        
        if has_window_function or has_cte:
            # Query already has advanced features, apply context normally
            return self.apply_context_to_query(base_query, table_name)
        
        # Apply population-first vectorization patterns
        vectorized_query = self._wrap_with_population_cte(base_query, table_name)
        return vectorized_query
    
    def _wrap_with_population_cte(self, base_query: str, table_name: str) -> str:
        """
        Wrap query with population-optimized CTE pattern.
        
        This creates a Common Table Expression that:
        1. Pre-filters data based on context
        2. Adds population analytics columns
        3. Enables efficient aggregation and window functions
        
        Args:
            base_query: Base SQL query
            table_name: FHIR resources table name
            
        Returns:
            CTE-wrapped query optimized for population processing
        """
        # Generate context filter
        context_filter = self.generate_context_sql_filter(table_name)
        group_by_clause = self.generate_context_group_by(table_name)
        
        # Extract key parts of base query
        base_query_upper = base_query.upper()
        
        # Determine population analytics columns based on context
        analytics_columns = self._get_population_analytics_columns(table_name)
        
        if group_by_clause and analytics_columns:
            # Create population-optimized CTE
            cte_query = f"""
WITH population_base AS (
    SELECT 
        *,
        {analytics_columns}
    FROM {table_name}
    {f'WHERE {context_filter}' if context_filter != '1=1' else ''}
),
population_aggregated AS (
    {base_query.replace(f'FROM {table_name}', 'FROM population_base')}
    {f'GROUP BY {group_by_clause}' if 'GROUP BY' not in base_query_upper else f', {group_by_clause}'}
)
SELECT * FROM population_aggregated
            """.strip()
            
            return cte_query
        else:
            # Simple context application
            return self.apply_context_to_query(base_query, table_name)
    
    def _get_population_analytics_columns(self, table_name: str) -> str:
        """
        Get population analytics columns based on context type.
        
        Args:
            table_name: FHIR resources table name
            
        Returns:
            SQL column definitions for population analytics
        """
        columns = []
        
        if self.context_type == CQLContextType.PATIENT:
            patient_extract = self._get_dialect_compatible_json_extract("'$.subject.reference'", table_name)
            resource_type_extract = self._get_dialect_compatible_json_extract("'$.resourceType'", table_name)
            columns.append(f"{patient_extract} as patient_id")
            columns.append(f"{resource_type_extract} as resource_type")
            
        elif self.context_type == CQLContextType.PRACTITIONER:
            practitioner_extract = self._get_dialect_compatible_json_extract("'$.performer[0].reference'", table_name)
            patient_extract = self._get_dialect_compatible_json_extract("'$.subject.reference'", table_name)
            resource_type_extract = self._get_dialect_compatible_json_extract("'$.resourceType'", table_name)
            columns.append(f"{practitioner_extract} as practitioner_id")
            columns.append(f"{patient_extract} as patient_id")
            columns.append(f"{resource_type_extract} as resource_type")
            
        elif self.context_type == CQLContextType.ENCOUNTER:
            encounter_extract = self._get_dialect_compatible_json_extract("'$.encounter.reference'", table_name)
            patient_extract = self._get_dialect_compatible_json_extract("'$.subject.reference'", table_name)
            resource_type_extract = self._get_dialect_compatible_json_extract("'$.resourceType'", table_name)
            columns.append(f"{encounter_extract} as encounter_id")
            columns.append(f"{patient_extract} as patient_id")
            columns.append(f"{resource_type_extract} as resource_type")
            
        elif self.context_type == CQLContextType.POPULATION:
            patient_extract = self._get_dialect_compatible_json_extract("'$.subject.reference'", table_name)
            resource_type_extract = self._get_dialect_compatible_json_extract("'$.resourceType'", table_name)
            gender_extract = self._get_dialect_compatible_json_extract("'$.gender'", table_name)
            birth_date_extract = self._get_dialect_compatible_json_extract("'$.birthDate'", table_name)
            columns.append(f"{patient_extract} as patient_id")
            columns.append(f"{resource_type_extract} as resource_type")
            # Add demographic columns for population analytics
            columns.append(f"{gender_extract} as gender")
            columns.append(f"{birth_date_extract} as birth_date")
        
        return ',\n        '.join(columns) if columns else ""
    
    def generate_window_function_sql(self, metric_expression: str, table_name: str = "fhir_resources") -> str:
        """
        Generate SQL with window functions for population analytics.
        
        This method creates efficient SQL patterns using window functions for:
        - Patient-level calculations within population
        - Ranking and percentile calculations
        - Moving averages and trends
        
        Args:
            metric_expression: The metric to calculate
            table_name: FHIR resources table name
            
        Returns:
            SQL with window functions for advanced analytics
        """
        if self.is_single_patient_mode():
            # Single-patient mode: no window functions needed
            return f"SELECT {metric_expression} FROM {table_name} WHERE {self.generate_context_sql_filter(table_name)}"
        
        # Population mode: use window functions
        analytics_columns = self._get_population_analytics_columns(table_name)
        context_filter = self.generate_context_sql_filter(table_name)
        
        window_sql = f"""
WITH population_metrics AS (
    SELECT 
        {analytics_columns},
        {metric_expression} as metric_value,
        ROW_NUMBER() OVER (PARTITION BY json_extract_string({table_name}, '$.subject.reference') ORDER BY json_extract_string({table_name}, '$.date')) as record_sequence,
        COUNT(*) OVER (PARTITION BY json_extract_string({table_name}, '$.subject.reference')) as patient_record_count,
        AVG({metric_expression}) OVER (PARTITION BY json_extract_string({table_name}, '$.subject.reference')) as patient_avg,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {metric_expression}) OVER () as population_median
    FROM {table_name}
    {f'WHERE {context_filter}' if context_filter != '1=1' else ''}
)
SELECT 
    patient_id,
    resource_type,
    metric_value,
    patient_record_count,
    patient_avg,
    population_median,
    CASE 
        WHEN metric_value > population_median THEN 'Above Median'
        WHEN metric_value < population_median THEN 'Below Median' 
        ELSE 'At Median'
    END as population_comparison
FROM population_metrics
ORDER BY patient_id, record_sequence
        """.strip()
        
        return window_sql
    
    def _get_dialect_compatible_json_extract(self, path: str, table_alias: str = "resource") -> str:
        """
        Get dialect-compatible JSON extraction SQL.
        
        Args:
            path: JSON path to extract
            table_alias: Table alias
            
        Returns:
            Dialect-appropriate JSON extraction SQL
        """
        if self.dialect and hasattr(self.dialect, 'name'):
            if self.dialect.name == "POSTGRESQL":
                # PostgreSQL uses ->> for text extraction
                return f"({table_alias}->>{path})"
            else:
                # DuckDB and default
                return f"json_extract_string({table_alias}, '{path}')"
        else:
            # Default to DuckDB syntax
            return f"json_extract_string({table_alias}, '{path}')"
    
    def _get_dialect_compatible_percentile(self, expression: str, percentile: float) -> str:
        """
        Get dialect-compatible percentile calculation.
        
        Args:
            expression: Expression to calculate percentile for
            percentile: Percentile value (0.0 to 1.0)
            
        Returns:
            Dialect-appropriate percentile SQL
        """
        if self.dialect and hasattr(self.dialect, 'name'):
            if self.dialect.name == "POSTGRESQL":
                # PostgreSQL uses PERCENTILE_CONT
                return f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {expression}) OVER ()"
            else:
                # DuckDB uses PERCENTILE_CONT
                return f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {expression}) OVER ()"
        else:
            # Default percentile function
            return f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {expression}) OVER ()"
    
    def _get_dialect_compatible_date_diff(self, start_date: str, end_date: str = "CURRENT_DATE") -> str:
        """
        Get dialect-compatible date difference calculation.
        
        Args:
            start_date: Start date expression
            end_date: End date expression (defaults to CURRENT_DATE)
            
        Returns:
            Dialect-appropriate date difference SQL
        """
        if self.dialect and hasattr(self.dialect, 'name'):
            if self.dialect.name == "POSTGRESQL":
                # PostgreSQL uses EXTRACT or date subtraction
                return f"EXTRACT(DAYS FROM {end_date} - DATE({start_date})) / 365.25"
            else:
                # DuckDB uses DATEDIFF or date subtraction
                return f"CAST(({end_date} - DATE({start_date})) / 365.25 AS DOUBLE)"
        else:
            # Default to DuckDB syntax
            return f"CAST(({end_date} - DATE({start_date})) / 365.25 AS DOUBLE)"
    
    def apply_context_to_query(self, base_query: str, table_name: str = "fhir_resources") -> str:
        """
        Apply context filtering and population-first processing to a base query.
        
        Args:
            base_query: Base SQL query
            table_name: FHIR resources table name
            
        Returns:
            Context-enhanced SQL query with population-first optimization
        """
        modified_query = base_query
        base_query_upper = base_query.upper()
        
        # Step 1: Add context filtering (WHERE clause)
        context_filter = self.generate_context_sql_filter(table_name)
        
        if context_filter and context_filter != "1=1":
            if "WHERE" in base_query_upper:
                # Existing WHERE clause - add AND condition
                modified_query = modified_query.replace(
                    " WHERE ", 
                    f" WHERE ({context_filter}) AND ("
                ) + ")"
            else:
                # No WHERE clause - add one
                modified_query = modified_query + f" WHERE {context_filter}"
        
        # Step 2: For population processing, add context columns and GROUP BY
        group_by_clause = self.generate_context_group_by(table_name)
        
        if group_by_clause:
            # Add context-aware columns to SELECT
            if "SELECT" in base_query_upper:
                # Find the SELECT clause and enhance it
                select_start = base_query_upper.find("SELECT")
                from_start = base_query_upper.find("FROM")
                
                if select_start >= 0 and from_start > select_start:
                    # Extract SELECT clause
                    select_clause = modified_query[select_start + 6:from_start].strip()
                    
                    # Add context columns
                    enhanced_select = self.add_context_columns(select_clause, table_name)
                    
                    # Reconstruct query with enhanced SELECT
                    before_select = modified_query[:select_start + 6]
                    after_from = modified_query[from_start:]
                    modified_query = f"{before_select} {enhanced_select} {after_from}"
            
            # Add GROUP BY clause if not already present
            if "GROUP BY" not in base_query_upper:
                modified_query = modified_query + f" GROUP BY {group_by_clause}"
            else:
                # If GROUP BY exists, append our grouping
                modified_query = modified_query + f", {group_by_clause}"
        
        return modified_query
    
    def get_context_variables(self) -> Dict[str, Any]:
        """Get context-specific variables."""
        variables = {
            'context': self.context_type.value,
            'contextType': self.context_type.value
        }
        
        if self.current_patient_id:
            variables['patientId'] = self.current_patient_id
            
        variables.update(self.context_variables)
        return variables
    
    def set_context_variable(self, name: str, value: Any):
        """Set a context-specific variable."""
        self.context_variables[name] = value
        logger.debug(f"Set context variable {name} = {value}")
    
    def clear_context_variables(self):
        """Clear all context variables."""
        self.context_variables.clear()

class CQLContextManager:
    """
    Context stack manager for nested context operations.
    
    Supports pushing/popping contexts for complex CQL evaluations.
    """
    
    def __init__(self):
        self.context_stack: List[CQLContext] = []
        self.current_context = CQLContext()
    
    def push_context(self, context: CQLContext):
        """Push a new context onto the stack."""
        self.context_stack.append(self.current_context)
        self.current_context = context
        logger.debug(f"Pushed context: {context.context_type.value}")
    
    def pop_context(self) -> Optional[CQLContext]:
        """Pop the current context and restore previous."""
        if self.context_stack:
            old_context = self.current_context
            self.current_context = self.context_stack.pop()
            logger.debug(f"Popped context: {old_context.context_type.value}")
            return old_context
        return None
    
    def get_current_context(self) -> CQLContext:
        """Get the current active context."""
        return self.current_context
    
    def with_context(self, context: CQLContext):
        """Context manager for temporary context switching."""
        return TemporaryContextScope(self, context)

class TemporaryContextScope:
    """Context manager for temporary context switching."""
    
    def __init__(self, manager: CQLContextManager, context: CQLContext):
        self.manager = manager
        self.context = context
    
    def __enter__(self):
        self.manager.push_context(self.context)
        return self.context
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.manager.pop_context()

class CQLEvaluationContext:
    """
    Enhanced evaluation context with library and parameter support.
    
    Manages libraries, parameters, and definitions in context-aware manner.
    """
    
    def __init__(self, context: CQLContext):
        self.context = context
        self.libraries = {}
        self.parameters = {}
        self.local_definitions = {}
        self.imported_definitions = {}
    
    def add_library(self, name: str, library_data: Dict[str, Any]):
        """Add a library to the evaluation context."""
        self.libraries[name] = library_data
        
        # Import public definitions
        if 'definitions' in library_data:
            for def_name, def_data in library_data['definitions'].items():
                if def_data.get('access_level') == 'PUBLIC':
                    qualified_name = f"{name}.{def_name}"
                    self.imported_definitions[qualified_name] = def_data
    
    def set_parameter(self, name: str, value: Any):
        """Set a parameter value."""
        self.parameters[name] = value
    
    def get_parameter(self, name: str) -> Any:
        """Get a parameter value."""
        return self.parameters.get(name)
    
    def add_definition(self, name: str, expression: Any, access_level: str = "PRIVATE"):
        """Add a local definition."""
        self.local_definitions[name] = {
            'expression': expression,
            'access_level': access_level
        }
    
    def get_definition(self, name: str) -> Optional[Any]:
        """Get a definition (local first, then imported)."""
        # Check local definitions first
        if name in self.local_definitions:
            return self.local_definitions[name]['expression']
        
        # Check imported definitions
        if name in self.imported_definitions:
            return self.imported_definitions[name]['expression']
        
        return None
    
    def evaluate_in_context(self, expression: Any) -> Any:
        """Evaluate expression in current context."""
        # This would be the main evaluation entry point
        # For now, return the expression as-is
        return expression