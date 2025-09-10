"""
PostgreSQL dialect implementation for FHIR4DS.

This module provides PostgreSQL-specific functionality for FHIR data storage,
optimized for JSONB operations and performance.
"""

import json
import logging
from typing import Dict, List, Any, Optional

from .base import DatabaseDialect

# Optional import for PostgreSQL
try:
    import psycopg2
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False

logger = logging.getLogger(__name__)


class PostgreSQLDialect(DatabaseDialect):
    """PostgreSQL implementation of the database dialect"""
    
    def __init__(self, conn_str: str):
        super().__init__()  # Initialize base class
        
        if not POSTGRESQL_AVAILABLE:
            raise ImportError("psycopg2 is required but not installed. Install with: pip install psycopg2-binary")
        
        # PostgreSQL-specific settings
        self.name = "POSTGRESQL"
        self.supports_jsonb = True
        self.supports_json_functions = True
        self.json_type = "JSONB"
        # Function mappings for PostgreSQL - now properly integrated with dialect-aware generation
        self.json_extract_function = "jsonb_extract_path"
        self.json_extract_string_function = "jsonb_extract_path_text"
        self.json_array_function = "jsonb_build_array"
        self.json_object_function = "jsonb_build_object"
        self.json_type_function = "jsonb_typeof"
        self.json_array_length_function = "jsonb_array_length"
        self.json_each_function = "jsonb_array_elements"  # Default to array elements for FHIR
        self.array_agg_function = "array_agg"
        self.string_agg_function = "string_agg"
        self.regex_function = "substring"
        self.cast_syntax = "::"
        self.quote_char = '"'
        
        # PostgreSQL-specific JSONB functions
        self.jsonb_path_query_function = "jsonb_path_query"
        self.jsonb_path_query_first_function = "jsonb_path_query_first"
        self.jsonb_path_exists_function = "jsonb_path_exists"
        self.jsonb_array_elements_function = "jsonb_array_elements"
        self.jsonb_array_elements_text_function = "jsonb_array_elements_text"
        
        self.connection = psycopg2.connect(conn_str)
        self.connection.autocommit = True  # Enable autocommit to avoid transaction issues
        logger.info("Initialized PostgreSQL dialect")
    
    def get_connection(self) -> Any:
        return self.connection
    
    def execute_sql(self, sql: str, view_def: Optional[Dict] = None) -> 'QueryResult':
        """Execute SQL and return wrapped results"""
        logger.debug(f"Executing PostgreSQL SQL: {sql}")
        
        # Import locally to avoid circular imports
        from .. import datastore
        from ..datastore import QueryResult
        return QueryResult(self, sql, view_def)
    
    def execute_query(self, sql: str) -> Any:
        """Execute a query and return raw results"""
        logger.debug(f"Executing PostgreSQL SQL: {sql}")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            # Store the cursor description for later retrieval
            self._last_cursor_description = cursor.description
            return cursor.fetchall()
        except Exception as e:
            # With autocommit=True, no need to rollback
            logger.error(f"PostgreSQL execution failed: {e}\nSQL: {sql}")
            raise e
    
    def get_query_description(self, connection: Any) -> Any:
        """Get column descriptions from last executed query"""
        return getattr(self, '_last_cursor_description', None)
    
    def create_fhir_table(self, table_name: str, json_col: str) -> None:
        """Create FHIR resources table optimized for PostgreSQL"""
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                {json_col} JSONB
            )
        """)
        # Add GIN index for better JSON query performance
        cursor.execute(f"CREATE INDEX idx_{table_name}_{json_col}_gin ON {table_name} USING GIN ({json_col})")
        
        # Create optimized indexes for common FHIR query patterns
        self.create_optimized_indexes(table_name, json_col)
        
        # No need to commit with autocommit=True
        logger.info(f"Created FHIR table: {table_name} with JSONB optimization and enhanced indexing")
    
    def bulk_load_json(self, file_path: str, table_name: str, json_col: str) -> int:
        """Enhanced bulk load JSON using PostgreSQL COPY for better performance"""
        try:
            return self._bulk_load_json_copy(file_path, table_name, json_col)
        except Exception as e:
            logger.warning(f"COPY bulk load failed, falling back to individual inserts: {e}")
            return 0  # Indicates to use fallback method
    
    def insert_resource(self, resource: Dict[str, Any], table_name: str, json_col: str) -> None:
        """Insert a single FHIR resource using JSONB"""
        cursor = self.connection.cursor()
        cursor.execute(
            f"INSERT INTO {table_name} ({json_col}) VALUES (%s)",
            (json.dumps(resource),)
        )
        # No need to commit with autocommit=True
    
    def get_resource_counts(self, table_name: str, json_col: str) -> Dict[str, int]:
        """Get resource counts using JSONB operators"""
        cursor = self.connection.cursor()
        cursor.execute(f"""
            SELECT {json_col}->>'resourceType' as resource_type, COUNT(*) as count 
            FROM {table_name} 
            GROUP BY {json_col}->>'resourceType'
            ORDER BY count DESC
        """)
        return {row[0]: row[1] for row in cursor.fetchall()}
    
    # Dialect-specific SQL generation methods for PostgreSQL
    
    def extract_json_field(self, column: str, path: str) -> str:
        """Extract a JSON field as text using PostgreSQL's jsonb_extract_path_text function"""
        if path.startswith('$.'):
            field_path = path[2:]  # Remove $.
            
            # Handle array indexing like name[0].family -> jsonb_extract_path_text(column, 'name', '0', 'family')
            if '[' in field_path:
                import re
                # Convert array indexing: name[0] -> name,0
                processed_path = re.sub(r'(\w+)\[(\d+)\]', r'\1,\2', field_path)
                parts = processed_path.replace('.', ',').split(',')
                path_args = ', '.join([f"'{part}'" for part in parts])
                return f"jsonb_extract_path_text({column}, {path_args})"
            elif '.' in field_path:
                # Nested path: $.name.family -> jsonb_extract_path_text(column, 'name', 'family')
                parts = field_path.split('.')
                path_args = ', '.join([f"'{part}'" for part in parts])
                return f"jsonb_extract_path_text({column}, {path_args})"
            else:
                # Simple field: $.id -> jsonb_extract_path_text(column, 'id')
                return f"jsonb_extract_path_text({column}, '{field_path}')"
        else:
            # Complex JSONPath - use jsonb_path_query_first as fallback
            return f"jsonb_path_query_first({column}, '{path}') #>> '{{}}'"
    
    def extract_json_object(self, column: str, path: str) -> str:
        """Extract a JSON object using PostgreSQL's jsonb_extract_path function"""
        if path.startswith('$.'):
            field_path = path[2:]
            
            # Handle array indexing like telecom[0] -> jsonb_extract_path(column, 'telecom', '0')
            if '[' in field_path:
                import re
                # Convert array indexing: telecom[0] -> telecom,0
                processed_path = re.sub(r'(\w+)\[(\d+)\]', r'\1,\2', field_path)
                parts = processed_path.replace('.', ',').split(',')
                path_args = ', '.join([f"'{part}'" for part in parts])
                return f"jsonb_extract_path({column}, {path_args})"
            elif '.' in field_path:
                # Check if this is an array field that needs array iteration instead of simple extraction
                parts = field_path.split('.')
                first_part = parts[0]
                
                # Use centralized FHIR schema instead of hardcoded array fields
                from ..schema import fhir_schema
                
                if fhir_schema.is_array_field(first_part) and len(parts) > 1:
                    # Use array iteration: name.family -> jsonb_path_query_array to get ALL values
                    # This is essential for collection functions like combine() that need all array elements
                    array_path = f"$.{'.'.join(parts[:1])}[*].{'.'.join(parts[1:])}"
                    return f"jsonb_path_query_array({column}, '{array_path}')"
                else:
                    # Regular nested path: $.id.value -> jsonb_extract_path(column, 'id', 'value')
                    path_args = ', '.join([f"'{part}'" for part in parts])
                    return f"jsonb_extract_path({column}, {path_args})"
            else:
                # Simple field: $.telecom -> jsonb_extract_path(column, 'telecom')
                return f"jsonb_extract_path({column}, '{field_path}')"
        else:
            # Complex JSONPath - use jsonb_path_query_array for array results or jsonb_path_query_first for single results
            if '[*]' in path:
                return f"jsonb_path_query_array({column}, '{path}')"
            else:
                return f"jsonb_path_query_first({column}, '{path}')"
    
    def iterate_json_array(self, column: str, path: str) -> str:
        """Iterate over JSON array elements using PostgreSQL JSONB functions"""
        if path.startswith('$.'):
            field_path = path[2:]
            
            if '.' in field_path:
                # Nested path like $.name -> jsonb_array_elements(jsonb_extract_path(column, 'name')) WITH ORDINALITY
                parts = field_path.split('.')
                path_args = ', '.join([f"'{part}'" for part in parts])
                return f"jsonb_array_elements(jsonb_extract_path({column}, {path_args})) WITH ORDINALITY"
            else:
                # Simple path like $.telecom -> jsonb_array_elements(jsonb_extract_path(column, 'telecom')) WITH ORDINALITY
                return f"jsonb_array_elements(jsonb_extract_path({column}, '{field_path}')) WITH ORDINALITY"
        else:
            # Complex path or no path - if path is '$', it's an array iteration
            if path == '$':
                return f"jsonb_array_elements({column}) WITH ORDINALITY"
            else:
                return f"jsonb_each({column})"
    
    def check_json_exists(self, column: str, path: str) -> str:
        """Check if JSON path exists using PostgreSQL JSONB operators"""
        if path.startswith('$.'):
            field_path = path[2:]
            if '.' not in field_path and '[' not in field_path:
                # Simple path: use ? operator
                return f"({column} ? '{field_path}')"
            else:
                # Complex path: use jsonb_path_exists
                return f"jsonb_path_exists({column}, '{path}')"
        else:
            return f"jsonb_path_exists({column}, '{path}')"
    
    def get_json_type(self, column: str) -> str:
        """Get JSON value type using PostgreSQL's jsonb_typeof with uppercase for case consistency"""
        return f"upper(jsonb_typeof({column}))"
    
    def get_json_array_length(self, column: str, path: str = None) -> str:
        """Get JSON array length using PostgreSQL's jsonb_array_length"""
        if path:
            json_obj = self.extract_json_object(column, path)
            return f"jsonb_array_length({json_obj})"
        else:
            return f"jsonb_array_length({column})"
    
    def get_json_type_constant(self, json_type: str) -> str:
        """Get the correct type constant for comparison with get_json_type()"""
        # Since we use upper(jsonb_typeof()), type constants should be uppercase
        return json_type.upper()
    
    def aggregate_to_json_array(self, expression: str) -> str:
        """Aggregate values into a JSON array using PostgreSQL's jsonb_agg"""
        return f"jsonb_agg({expression})"
    
    def json_array_agg_function(self, expression: str) -> str:
        """JSON array aggregation function alias - same as aggregate_to_json_array"""
        return f"jsonb_agg({expression})"
    
    def coalesce_empty_array(self, expression: str) -> str:
        """COALESCE with empty array using PostgreSQL JSONB syntax"""
        return f"COALESCE({expression}, '[]'::jsonb)"
    
    def get_array_iteration_columns(self) -> tuple:
        """Get column names for array iteration - PostgreSQL uses 'value' and 'ordinality'"""
        return ('value', 'ordinality')
    
    def get_object_iteration_columns(self) -> tuple:
        """Get column names for object iteration - PostgreSQL uses 'key' and 'value'"""
        return ('key', 'value')
    
    # Pipeline-specific dialect method implementations
    
    def string_split_reference(self, input_sql: str) -> str:
        """Split reference string like 'ResourceType/id' to extract id part using PostgreSQL functions"""
        return f"split_part({input_sql}, '/', -1)"
    
    def starts_with_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate starts_with condition for field matching using PostgreSQL syntax"""
        return f"starts_with(@.{field_name}, \"{value}\")"
    
    def ends_with_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate ends_with condition for field matching using PostgreSQL syntax"""
        return f"(@.{field_name} like_regex \".*{value}$\")"
    
    def contains_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate contains condition for field matching using PostgreSQL syntax"""
        return f"(@.{field_name} like_regex \".*{value}.*\")"
    
    def get_value_primitive_sql(self, input_sql: str) -> str:
        """Generate SQL for getValue() function handling primitive and complex types using PostgreSQL JSONB functions"""
        return f"""CASE 
            WHEN {input_sql} IS NULL THEN NULL
            WHEN jsonb_typeof({input_sql}) = 'string' THEN 
                {input_sql} #>> '{{}}'
            WHEN jsonb_typeof({input_sql}) = 'number' THEN 
                {input_sql} #>> '{{}}'
            WHEN jsonb_typeof({input_sql}) = 'boolean' THEN 
                CASE WHEN ({input_sql})::boolean THEN 'true' ELSE 'false' END
            WHEN jsonb_typeof({input_sql}) = 'object' AND {input_sql} ? 'value' THEN
                {input_sql} #>> '{{value}}'
            WHEN jsonb_typeof({input_sql}) = 'object' AND {input_sql} ? 'family' THEN
                COALESCE(
                    ({input_sql} #>> '{{family}}') || 
                    CASE 
                        WHEN {input_sql} ? 'given' AND jsonb_typeof({input_sql} -> 'given') = 'array'
                        THEN ', ' || array_to_string(
                            ARRAY(SELECT jsonb_array_elements_text({input_sql} -> 'given')), ', '
                        )
                        ELSE '' 
                    END,
                    {input_sql} #>> '{{family}}'
                )
            WHEN jsonb_typeof({input_sql}) = 'array' AND jsonb_array_length({input_sql}) > 0 THEN
                COALESCE(
                    ({input_sql} -> 0 #>> '{{family}}') || 
                    CASE 
                        WHEN {input_sql} -> 0 ? 'given' AND jsonb_typeof({input_sql} -> 0 -> 'given') = 'array'
                        THEN ', ' || array_to_string(
                            ARRAY(SELECT jsonb_array_elements_text({input_sql} -> 0 -> 'given')), ', '
                        )
                        ELSE '' 
                    END,
                    {input_sql} -> 0 #>> '{{family}}',
                    {input_sql} -> 0 #>> '{{value}}',
                    {input_sql} -> 0 #>> '{{}}'
                )
            ELSE 
                {input_sql} #>> '{{}}'
        END"""
    
    def resolve_reference_sql(self, input_sql: str) -> str:
        """Generate SQL for resolve() function to resolve FHIR references using PostgreSQL JSONB syntax"""
        return f"""CASE 
            WHEN {input_sql} IS NULL THEN NULL
            WHEN {input_sql} ? 'reference' THEN
                (
                    SELECT fhir_data.data 
                    FROM fhir_data 
                    WHERE (fhir_data.data #>> '{{resourceType}}') || '/' || (fhir_data.data #>> '{{id}}') = {input_sql} #>> '{{reference}}'
                    LIMIT 1
                )
            WHEN jsonb_typeof({input_sql}) = 'string' THEN
                (
                    SELECT fhir_data.data 
                    FROM fhir_data 
                    WHERE (fhir_data.data #>> '{{resourceType}}') || '/' || (fhir_data.data #>> '{{id}}') = {input_sql} #>> '{{}}'
                    LIMIT 1
                )
            ELSE NULL
        END"""
    
    def extract_json_text(self, column: str, path: str) -> str:
        """Extract a JSON field as text using PostgreSQL's jsonb_extract_path_text function"""
        if path.startswith('$.'):
            field_path = path[2:]  # Remove $.
            
            # Handle array indexing like name[0].family -> jsonb_extract_path_text(column, 'name', '0', 'family')
            if '[' in field_path:
                import re
                # Convert array indexing: name[0] -> name,0
                processed_path = re.sub(r'(\w+)\[(\d+)\]', r'\1,\2', field_path)
                parts = processed_path.replace('.', ',').split(',')
                path_args = ', '.join([f"'{part}'" for part in parts])
                return f"jsonb_extract_path_text({column}, {path_args})"
            elif '.' in field_path:
                # Check if this is an array field that needs array iteration instead of simple extraction
                parts = field_path.split('.')
                first_part = parts[0]
                
                # Use centralized FHIR schema instead of hardcoded array fields
                from ..schema import fhir_schema
                
                if fhir_schema.is_array_field(first_part) and len(parts) > 1:
                    # Use array iteration: name.family -> jsonb_path_query_array for multiple values
                    # For text extraction, we need to handle this differently since we can't return an array as text
                    # Instead, convert to a single text value by taking the first non-null result
                    array_path = f"$.{'.'.join(parts[:1])}[*].{'.'.join(parts[1:])}"
                    return f"jsonb_path_query_first({column}, '{array_path}') #>> '{{}}'"
                else:
                    # Regular nested path: $.id.value -> jsonb_extract_path_text(column, 'id', 'value') 
                    path_args = ', '.join([f"'{part}'" for part in parts])
                    return f"jsonb_extract_path_text({column}, {path_args})"
            else:
                # Simple field: $.id -> jsonb_extract_path_text(column, 'id')
                return f"jsonb_extract_path_text({column}, '{field_path}')"
        else:
            # Complex JSONPath - use jsonb_path_query_first as fallback
            return f"jsonb_path_query_first({column}, '{path}') #>> '{{}}'"
    
    def json_extract_string(self, column: str, path: str) -> str:
        """Generate JSON string extraction SQL using PostgreSQL's jsonb_extract_path_text"""
        # This method is an alias for extract_json_field - both extract text
        return self.extract_json_field(column, path)
    
    def get_json_array_element(self, json_expr: str, index: int) -> str:
        """Extract element at index from JSON array."""
        return f"({json_expr} -> {index})"
    
    def get_json_extract_string(self, json_expr: str, path: str) -> str:
        """Extract string value from JSON path."""
        if path.startswith('$.'):
            field_path = path[2:]
            path_args = field_path.replace('.', ',').replace(',', '\', \'')
            return f"jsonb_extract_path_text({json_expr}, '{path_args}')"
        else:
            return f"jsonb_path_query_first({json_expr}, '{path}') #>> '{{}}'"
    
    def create_json_array(self, *args) -> str:
        """Create a JSON array from arguments using PostgreSQL's jsonb_build_array"""
        if args:
            return f"jsonb_build_array({', '.join(str(arg) for arg in args)})"
        return "'[]'::jsonb"
    
    def create_json_object(self, *args) -> str:
        """Create a JSON object from key-value pairs using PostgreSQL's jsonb_build_object"""
        if args:
            return f"jsonb_build_object({', '.join(str(arg) for arg in args)})"
        return "'{}'::jsonb"
    
    def aggregate_values(self, expression: str, distinct: bool = False) -> str:
        """Array aggregation using PostgreSQL's array_agg"""
        if distinct:
            return f"array_agg(DISTINCT {expression})"
        return f"array_agg({expression})"
    
    def aggregate_strings(self, expression: str, separator: str) -> str:
        """String aggregation using PostgreSQL's string_agg"""
        return f"string_agg({expression}, {separator})"

    def join_array_elements(self, base_expr: str, separator_sql: str) -> str:
        """Join array elements with separator using PostgreSQL's jsonb functions"""
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN ''
            ELSE COALESCE((
                SELECT string_agg(
                    CASE 
                        WHEN jsonb_typeof(inner_elem.value) = 'string' THEN inner_elem.value #>> '{{}}'
                        ELSE inner_elem.value::text
                    END, 
                    {separator_sql}
                    ORDER BY outer_elem.outer_ord, inner_elem.inner_ord
                )
                FROM (
                    SELECT 
                        CASE 
                            WHEN jsonb_typeof({base_expr}) = 'array' THEN {base_expr}
                            ELSE jsonb_build_array({base_expr})
                        END as array_val
                ) base_array,
                jsonb_array_elements(base_array.array_val) WITH ORDINALITY AS outer_elem(value, outer_ord),
                jsonb_array_elements(
                    CASE 
                        WHEN jsonb_typeof(outer_elem.value) = 'array' THEN outer_elem.value
                        ELSE jsonb_build_array(outer_elem.value)
                    END
                ) WITH ORDINALITY AS inner_elem(value, inner_ord)
                WHERE inner_elem.value IS NOT NULL AND inner_elem.value != 'null'::jsonb
            ), '')
        END
        """
    
    def extract_nested_array_path(self, json_base: str, current_path: str, identifier_name: str, new_path: str) -> str:
        """Extract path from nested array structures using PostgreSQL's JSONB functions"""
        
        # Always return array results for FHIR array fields to support collection functions
        # This fixes the root cause of collection function failures
        
        # Handle root level access (current_path = "$")
        if current_path == "$":
            array_result = f"jsonb_path_query_array({json_base}, '$[*].{identifier_name}')"
            scalar_result = f"{json_base} -> '{identifier_name}'"
            
            # Context-aware array extraction: 
            # - For single-element arrays from where(), extract scalar values
            # - For multi-element arrays, return arrays for collection operations  
            return f"""CASE WHEN jsonb_typeof({json_base}) = 'array'
            THEN CASE WHEN jsonb_array_length({json_base}) = 1
                 THEN {json_base} -> 0 -> '{identifier_name}'
                 ELSE {array_result}
                 END
            ELSE {scalar_result}
            END"""
        
        # Convert DuckDB JSONPath syntax to PostgreSQL syntax
        if current_path.startswith('$.'):
            # Convert $.name to 'name' for PostgreSQL JSONB operators
            current_field = current_path[2:]  # Remove $.
            new_field_path = new_path[2:]     # Remove $.
            
            # Handle nested paths like $.name.given
            if '.' in current_field:
                # For nested paths, use jsonb_path_query for more complex operations
                array_path = f"{current_path}[*].{identifier_name}"
                array_result = f"jsonb_path_query_array({json_base}, '{array_path}')"
                scalar_result = f"({json_base} #> '{{{new_field_path.replace('.', ',')}}}')"
                
                # Context-aware array extraction for nested paths
                return f"""CASE WHEN jsonb_typeof(({json_base} #> '{{{current_field.replace('.', ',')}}}')) = 'array'
                THEN CASE WHEN jsonb_array_length(({json_base} #> '{{{current_field.replace('.', ',')}}}')) = 1
                     THEN ({json_base} #> '{{{current_field.replace('.', ',')}}}') -> 0 -> '{identifier_name}'
                     ELSE {array_result}
                     END
                ELSE {scalar_result}
                END"""
            else:
                # Simple paths like $.name
                array_path = f"{current_path}[*].{identifier_name}"
                array_result = f"jsonb_path_query_array({json_base}, '{array_path}')"
                scalar_result = f"{json_base} -> '{current_field}' -> '{identifier_name}'"
                
                # Context-aware array extraction for simple paths
                return f"""CASE WHEN jsonb_typeof({json_base} -> '{current_field}') = 'array'
                THEN CASE WHEN jsonb_array_length({json_base} -> '{current_field}') = 1
                     THEN ({json_base} -> '{current_field}') -> 0 -> '{identifier_name}'
                     ELSE {array_result}
                     END
                ELSE {scalar_result}
                END"""
        else:
            # Handle complex paths with jsonb_path_query
            array_path = f"{current_path}[*].{identifier_name}"
            array_result = f"jsonb_path_query_array({json_base}, '{array_path}')"
            scalar_result = f"jsonb_path_query_first({json_base}, '{new_path}')"
            
            # Context-aware array extraction for complex paths
            return f"""CASE WHEN jsonb_path_exists({json_base}, '{current_path}') AND jsonb_typeof(jsonb_path_query_first({json_base}, '{current_path}')) = 'array'
            THEN CASE WHEN jsonb_array_length(jsonb_path_query_first({json_base}, '{current_path}')) = 1
                 THEN jsonb_path_query_first({json_base}, '{current_path}') -> 0 -> '{identifier_name}'
                 ELSE {array_result}
                 END
            ELSE {scalar_result}
            END"""
    
    def split_string(self, expression: str, delimiter: str) -> str:
        """Split string into array using PostgreSQL's string_to_array function"""
        return f"string_to_array(CAST({expression} AS TEXT), {delimiter})"
    
    def substring(self, expression: str, start: str, length: str) -> str:
        """Extract substring using PostgreSQL's SUBSTRING function"""
        return f"SUBSTRING({expression}, ({start}) + 1, {length})"
    
    def string_position(self, search_str: str, target_str: str) -> str:
        """Find position using PostgreSQL's POSITION function (0-based index)"""
        return f"CASE WHEN POSITION(CAST({search_str} AS TEXT) IN CAST({target_str} AS TEXT)) > 0 THEN POSITION(CAST({search_str} AS TEXT) IN CAST({target_str} AS TEXT)) - 1 ELSE -1 END"
    
    def string_concat(self, left: str, right: str) -> str:
        """Concatenate strings using PostgreSQL's || operator"""
        return f"({left} || {right})"
    
    def optimize_cte_definition(self, cte_name: str, cte_expr: str) -> str:
        """Apply PostgreSQL-specific CTE optimizations using MATERIALIZED/NOT MATERIALIZED hints"""
        should_materialize = self._should_materialize_cte(cte_expr)
        
        if should_materialize:
            return f"{cte_name} AS MATERIALIZED ({cte_expr})"
        else:
            return f"{cte_name} AS NOT MATERIALIZED ({cte_expr})"
    
    def _should_materialize_cte(self, cte_expr: str) -> bool:
        """
        Determine if a CTE should be materialized in PostgreSQL
        
        MATERIALIZED CTEs are beneficial when:
        - The CTE produces large intermediate results
        - The CTE is referenced multiple times
        - The CTE contains expensive operations (joins, aggregations)
        """
        # Check for expensive operations that benefit from materialization
        expensive_operations = [
            'jsonb_agg',         # Aggregation operations
            'jsonb_array_elements',  # Table-valued functions
            'GROUP BY',          # Explicit grouping
            'ORDER BY',          # Sorting operations
            'DISTINCT',          # Deduplication
            'CASE WHEN'          # Complex conditional logic
        ]
        
        # Check for multiple expensive operations or very long expressions
        expensive_count = sum(1 for op in expensive_operations if op in cte_expr.upper())
        
        return (
            len(cte_expr) > 500 or           # Large/complex CTEs
            expensive_count >= 2 or          # Multiple expensive operations
            'jsonb_agg' in cte_expr          # Always materialize aggregations
        )
    
    def iterate_json_elements_for_arrays(self, column: str) -> str:
        """
        Iterate over JSON array elements using jsonb_array_elements with ordinality.
        This is specifically for arrays and provides proper key/value iteration.
        """
        return f"jsonb_array_elements({column}) WITH ORDINALITY AS t(value, key)"
    
    def json_each_safe(self, column: str) -> str:
        """
        Safe version of json_each that handles both arrays and objects.
        For arrays, uses jsonb_array_elements with ordinality to provide key/value pairs.
        For objects, uses jsonb_each directly.
        """
        return f"""
        CASE 
            WHEN jsonb_typeof({column}) = 'array' THEN (
                SELECT (key-1)::text as key, value 
                FROM jsonb_array_elements({column}) WITH ORDINALITY AS t(value, key)
            )
            ELSE (
                SELECT key, value 
                FROM jsonb_each({column})
            )
        END
        """
    
    # Array manipulation functions for collection operations
    def array_concat_function(self, array1: str, array2: str) -> str:
        """Concatenate two JSON arrays into a single JSON array."""
        return f"(({array1})::jsonb || ({array2})::jsonb)"
    
    def array_slice_function(self, array: str, start_index: str, end_index: str) -> str:
        """Slice a JSON array from start_index to end_index (1-based indexing)."""
        # PostgreSQL jsonb array slicing with proper null handling
        return f"""
        CASE 
            WHEN ({array})::jsonb IS NULL OR jsonb_typeof(({array})::jsonb) != 'array' THEN NULL
            WHEN jsonb_array_length(({array})::jsonb) = 0 THEN '[]'::jsonb
            ELSE (
                SELECT COALESCE(jsonb_agg(value), '[]'::jsonb)
                FROM (
                    SELECT value, row_number() OVER () as rn
                    FROM jsonb_array_elements(({array})::jsonb) as value
                ) indexed_array 
                WHERE rn >= ({start_index}) AND rn <= ({end_index})
            )
        END
        """
    
    def array_distinct_function(self, array: str) -> str:
        """Remove duplicates from a JSON array."""
        return f"""
        CASE 
            WHEN ({array})::jsonb IS NULL OR jsonb_typeof(({array})::jsonb) != 'array' THEN NULL
            WHEN jsonb_array_length(({array})::jsonb) = 0 THEN '[]'::jsonb
            ELSE (
                SELECT COALESCE(jsonb_agg(DISTINCT value ORDER BY value), '[]'::jsonb)
                FROM jsonb_array_elements(({array})::jsonb) as value
            )
        END
        """
    
    def array_union_function(self, array1: str, array2: str) -> str:
        """Union two JSON arrays (concatenate and remove duplicates)."""
        return f"""
        CASE 
            WHEN ({array1})::jsonb IS NULL AND ({array2})::jsonb IS NULL THEN NULL
            WHEN ({array1})::jsonb IS NULL THEN ({array2})::jsonb
            WHEN ({array2})::jsonb IS NULL THEN ({array1})::jsonb
            ELSE (
                SELECT COALESCE(jsonb_agg(DISTINCT value ORDER BY value), '[]'::jsonb)
                FROM (
                    SELECT value FROM jsonb_array_elements(({array1})::jsonb) as value
                    UNION 
                    SELECT value FROM jsonb_array_elements(({array2})::jsonb) as value
                ) combined
            )
        END
        """
    
    # Encoding/decoding functions for feature parity with DuckDB
    def url_encode(self, expression: str) -> str:
        """URL encode using PostgreSQL extensions"""
        return f"encode(convert_to(CAST({expression} AS TEXT), 'UTF8'), 'escape')"
    
    def url_decode(self, expression: str) -> str:
        """URL decode using PostgreSQL extensions"""
        return f"convert_from(decode(CAST({expression} AS TEXT), 'escape'), 'UTF8')"
    
    def base64_encode(self, expression: str) -> str:
        """Base64 encode using PostgreSQL's encode function"""
        return f"encode(convert_to(CAST({expression} AS TEXT), 'UTF8'), 'base64')"
    
    def base64_decode(self, expression: str) -> str:
        """Base64 decode using PostgreSQL's decode function"""
        return f"convert_from(decode(CAST({expression} AS TEXT), 'base64'), 'UTF8')"
    
    def get_collection_count_expression(self, base_expr: str) -> str:
        """Generate PostgreSQL-specific count expression for collection functions"""
        # PostgreSQL-specific count logic moved from collection_functions.py
        # This fixes the architecture violation of having database-specific logic outside dialects
        return f"""
        CASE 
            WHEN {base_expr} IS NULL THEN 0
            WHEN {base_expr}::text LIKE '[%]' THEN 
                CASE 
                    WHEN {base_expr}::text = '[]' THEN 0
                    ELSE (
                        SELECT COUNT(*)
                        FROM jsonb_array_elements({base_expr}::jsonb)
                    )
                END
            WHEN {base_expr} IS NOT NULL THEN 1
            ELSE 0
        END
        """
    
    def generate_from_json_each(self, column: str) -> str:
        """Generate FROM clause for JSON each iteration using PostgreSQL JSONB functions"""
        # PostgreSQL: FROM jsonb_array_elements(column) WITH ORDINALITY AS t(value, ordinality)
        return f"jsonb_array_elements({column}) WITH ORDINALITY AS t(value, ordinality)"
    
    def iterate_json_elements_indexed(self, column: str) -> str:
        """Iterate JSON elements with proper indexing for both arrays and objects using PostgreSQL"""
        # PostgreSQL needs different functions for arrays vs objects
        return f"""(
            SELECT value, (ordinality - 1)::text as key
            FROM jsonb_array_elements({column}) WITH ORDINALITY AS t(value, ordinality)
            UNION ALL
            SELECT value, key  
            FROM jsonb_each({column})
        )"""
    
    def create_optimized_indexes(self, table_name: str, json_col: str) -> None:
        """Create PostgreSQL-optimized indexes for FHIR data"""
        cursor = self.connection.cursor()
        
        try:
            # Resource type index (most common filter)
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_resource_type 
                ON {table_name} USING BTREE(({json_col}->>'resourceType'))
            """)
            
            # Patient ID index (common in clinical queries)
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_patient_id 
                ON {table_name} USING BTREE(({json_col}->>'id')) 
                WHERE {json_col}->>'resourceType' = 'Patient'
            """)
            
            # Subject reference index (for observations, conditions, etc.)
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_subject_ref 
                ON {table_name} USING BTREE(({json_col}->'subject'->>'reference'))
            """)
            
            # Observation code index (for lab results, vitals, etc.)
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_observation_code 
                ON {table_name} USING BTREE(({json_col}->'code'->'coding'->0->>'code'))
                WHERE {json_col}->>'resourceType' = 'Observation'
            """)
            
            # Effective date index (for temporal queries)
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_effective_date 
                ON {table_name} USING BTREE(({json_col}->>'effectiveDateTime'))
                WHERE {json_col}->>'resourceType' IN ('Observation', 'DiagnosticReport', 'Procedure')
            """)
            
            # Identifier value index (for external ID lookups)
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_identifier_value 
                ON {table_name} USING BTREE(({json_col}->'identifier'->0->>'value'))
                WHERE {json_col} ? 'identifier'
            """)
            
            logger.info(f"Created optimized indexes for {table_name}")
            
        except Exception as e:
            logger.warning(f"Failed to create some optimized indexes for {table_name}: {e}")
            # Continue execution as basic GIN index should still work
    
    def _bulk_load_json_copy(self, file_path: str, table_name: str, json_col: str) -> int:
        """Enhanced bulk loading using PostgreSQL COPY command for 5-10x performance improvement"""
        import tempfile
        import os
        
        cursor = self.connection.cursor()
        
        # Read and process JSON data
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
        
        # Handle both single resources and arrays of resources
        if isinstance(data, dict):
            resources = [data]
        elif isinstance(data, list):
            resources = data
        else:
            raise ValueError(f"Unsupported JSON structure in {file_path}")
        
        # Create temporary file for COPY
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_path = temp_file.name
            
            # Write JSON resources as single-column CSV
            for resource in resources:
                # Escape any quotes in the JSON and write as CSV
                json_str = json.dumps(resource).replace('"', '""')
                temp_file.write(f'"{json_str}"\n')
        
        try:
            # Use COPY command for high-performance bulk loading
            with open(temp_path, 'r') as f:
                cursor.copy_from(
                    f, 
                    table_name, 
                    columns=[json_col],
                    sep='\t',  # Use tab separator to avoid conflicts with JSON
                    null='',
                    quote='"'
                )
            
            logger.info(f"Successfully loaded {len(resources)} resources using COPY")
            return len(resources)
            
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_path)
            except OSError:
                pass
    
    # Pipeline-specific optimized implementations for PostgreSQL
    
    def extract_json_path(self, base_expr: str, json_path: str, context_mode: 'ContextMode') -> str:
        """
        PostgreSQL-optimized JSON path extraction with context awareness.
        """
        from ..pipeline.core.base import ContextMode
        
        # Convert JSONPath to PostgreSQL syntax
        pg_path = json_path.replace('$.', '')
        
        if context_mode == ContextMode.COLLECTION:
            # Handle JSONPath syntax for array traversal
            if json_path.startswith('$[*].'):
                # Extract field from array elements: $[*].field -> jsonb_path_query_array(base, '$[*].field')
                return f"jsonb_path_query_array({base_expr}, '{json_path}')"
            elif '.' in pg_path:
                # Handle nested paths like "name.family"
                parts = pg_path.split('.')
                pg_expr = base_expr
                for part in parts:
                    pg_expr = f"({pg_expr} -> '{part}')"
                return pg_expr
            else:
                return f"({base_expr} -> '{pg_path}')"
        elif context_mode == ContextMode.WHERE_CLAUSE:
            # Optimize for boolean evaluation using ?
            if '.' in pg_path:
                parts = pg_path.split('.')
                # For nested paths, check if the path exists
                return f"({base_expr} #> '{{{','.join(parts)}}}' IS NOT NULL)"
            else:
                return f"({base_expr} ? '{pg_path}')"
        else:
            # Standard text extraction using ->>
            if '.' in pg_path:
                parts = pg_path.split('.')
                pg_expr = base_expr
                for i, part in enumerate(parts):
                    if i == len(parts) - 1:
                        # Last part - extract as text
                        pg_expr = f"({pg_expr} ->> '{part}')"
                    else:
                        # Intermediate part - extract as jsonb
                        pg_expr = f"({pg_expr} -> '{part}')"
                return pg_expr
            else:
                return f"({base_expr} ->> '{pg_path}')"
    
    def extract_array_element(self, array_expr: str, index: int) -> str:
        """PostgreSQL-optimized array element extraction."""
        return f"({array_expr} -> {index})"
    
    def extract_last_array_element(self, array_expr: str) -> str:
        """PostgreSQL-optimized last array element extraction."""
        return f"({array_expr} -> (jsonb_array_length({array_expr}) - 1))"
    
    def _extract_collection_path(self, base_expr: str, json_path: str) -> str:
        """PostgreSQL-optimized collection path extraction."""
        # Convert to PostgreSQL path format
        pg_path = json_path.replace('$.', '')
        if '.' in pg_path:
            parts = pg_path.split('.')
            pg_expr = base_expr
            for part in parts:
                pg_expr = f"({pg_expr} -> '{part}')"
            return pg_expr
        else:
            return f"({base_expr} -> '{pg_path}')"
    
    def _extract_boolean_path(self, base_expr: str, json_path: str) -> str:
        """PostgreSQL-optimized boolean path extraction."""
        # Use PostgreSQL's existence operator ? when possible
        pg_path = json_path.replace('$.', '')
        if '.' in pg_path:
            # For nested paths, use #> path operator
            parts = pg_path.split('.')
            path_array = "{" + ",".join(parts) + "}"
            return f"({base_expr} #> '{path_array}' IS NOT NULL)"
        else:
            # For simple paths, use existence operator
            return f"({base_expr} ? '{pg_path}')"
    
    # Implementation of new abstract methods for FHIRPath operations
    
    def try_cast(self, expression: str, target_type: str) -> str:
        """PostgreSQL safe type conversion using CASE statements"""
        if target_type.upper() == 'INTEGER':
            return f"""(
                CASE WHEN {expression}::text ~ '^[+-]?[0-9]+$' 
                     THEN CAST({expression} AS INTEGER)
                     ELSE NULL END
            )"""
        elif target_type.upper() == 'BOOLEAN':
            return f"""(
                CASE WHEN LOWER({expression}::text) ~ '^(true|false|t|f|1|0)$'
                     THEN CAST({expression} AS BOOLEAN)
                     ELSE NULL END
            )"""
        elif target_type.upper() in ('DATE', 'TIME', 'TIMESTAMP'):
            # For temporal types, use basic casting with NULL on error
            return f"""(
                CASE 
                    WHEN {expression} IS NULL THEN NULL
                    ELSE 
                        CASE 
                            WHEN {expression}::text ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN
                                CAST({expression} AS {target_type.upper()})
                            ELSE NULL
                        END
                END
            )"""
        else:
            # Default to basic casting
            return f"CAST({expression} AS {target_type.upper()})"
    
    def string_to_char_array(self, expression: str) -> str:
        """PostgreSQL string to character array conversion"""
        return f"regexp_split_to_array({expression}, '')"
    
    def regex_matches(self, string_expr: str, pattern: str) -> str:
        """PostgreSQL regex pattern matching"""
        return f"({string_expr} ~ {pattern})"
    
    def regex_replace(self, string_expr: str, pattern: str, replacement: str) -> str:
        """PostgreSQL regex pattern replacement"""
        return f"regexp_replace({string_expr}, {pattern}, {replacement}, 'g')"
    
    def json_group_array(self, value_expr: str, from_clause: str = None) -> str:
        """PostgreSQL JSONB array aggregation"""
        if from_clause:
            return f"(SELECT jsonb_agg({value_expr}) FROM {from_clause})"
        return f"jsonb_agg({value_expr})"
    
    def json_each(self, json_expr: str, path: str = None) -> str:
        """PostgreSQL JSONB object iteration"""
        if path:
            # PostgreSQL doesn't have direct path-based json_each, use jsonb_extract_path
            return f"jsonb_each(jsonb_extract_path({json_expr}, {path}))"
        return f"jsonb_each({json_expr})"
    
    def json_typeof(self, json_expr: str) -> str:
        """PostgreSQL JSONB type checking"""
        return f"jsonb_typeof({json_expr})"
    
    def json_array_elements(self, json_expr: str, with_ordinality: bool = False) -> str:
        """PostgreSQL JSONB array element extraction"""
        if with_ordinality:
            return f"jsonb_array_elements({json_expr}) WITH ORDINALITY"
        return f"jsonb_array_elements({json_expr})"
    
    def cast_to_timestamp(self, expression: str) -> str:
        """PostgreSQL timestamp casting"""
        return f"CAST({expression} AS TIMESTAMP)"
    
    def cast_to_time(self, expression: str) -> str:
        """PostgreSQL time casting"""
        return f"CAST({expression} AS TIME)"
    
    def _generate_type_display_sql(self, input_sql: str, type_structure: Dict[str, Any],
                                   array_handling: str) -> str:
        """PostgreSQL-specific FHIR type display generation."""
        fields = type_structure["fields"]
        arrays = type_structure.get("arrays", [])
        template = type_structure.get("display_template", "{text || value}")
        
        # Generate field extraction logic
        field_extractions = {}
        for field in fields:
            if field in arrays:
                if array_handling == "first":
                    # Get first element of array
                    field_extractions[field] = f"{input_sql} -> '{field}' ->> 0"
                elif array_handling == "concat":
                    # Join array elements with comma
                    field_extractions[field] = f"array_to_string(ARRAY(SELECT jsonb_array_elements_text({input_sql} -> '{field}')), ', ')"
                elif array_handling == "all":
                    # Return full array as JSON
                    field_extractions[field] = f"{input_sql} -> '{field}'"
            else:
                # Simple field extraction
                field_extractions[field] = f"{input_sql} ->> '{field}'"
        
        # Apply template logic  
        return self._apply_display_template(template, field_extractions)
    
    # Function-specific SQL generation methods
    # These replace hardcoded dialect conditionals in functions.py
    
    def generate_array_contains(self, array_sql: str, element_sql: str) -> str:
        """PostgreSQL array contains check."""
        return f"{element_sql} = ANY({array_sql})"

    def generate_array_length(self, array_sql: str) -> str:
        """PostgreSQL array length."""
        return f"array_length({array_sql}, 1)"

    def generate_substring_sql(self, string_sql: str, start_pos: str, length: Optional[str] = None) -> str:
        """PostgreSQL substring operation."""
        if length:
            return f"substring({string_sql}, {start_pos}, {length})"
        return f"substring({string_sql}, {start_pos})"

    def generate_string_split(self, string_sql: str, delimiter: str) -> str:
        """PostgreSQL string split."""
        return f"string_to_array({string_sql}, {delimiter})"

    def generate_array_element_at(self, array_sql: str, index: int) -> str:
        """PostgreSQL array element access (1-based indexing)."""
        return f"{array_sql}[{index + 1}]"

    def generate_case_insensitive_like(self, field_sql: str, pattern: str) -> str:
        """PostgreSQL case-insensitive pattern matching."""
        return f"{field_sql} ILIKE {pattern}"

    def generate_regexp_match(self, field_sql: str, pattern: str) -> str:
        """PostgreSQL regex matching."""
        return f"{field_sql} ~ {pattern}"

    def generate_date_arithmetic(self, date_sql: str, interval: str, unit: str) -> str:
        """PostgreSQL date arithmetic."""
        return f"{date_sql} + INTERVAL '{interval} {unit}'"

    def generate_cast_to_numeric(self, value_sql: str) -> str:
        """PostgreSQL numeric casting."""
        return f"CAST({value_sql} AS NUMERIC)"

    def generate_null_coalesce(self, *expressions: str) -> str:
        """PostgreSQL null coalescing."""
        return f"COALESCE({', '.join(expressions)})"
    
    def generate_json_typeof(self, json_expr: str) -> str:
        """PostgreSQL JSON type detection."""
        return f"jsonb_typeof({json_expr})"
    
    def generate_string_concat(self, *expressions: str) -> str:
        """PostgreSQL string concatenation."""
        return f"({' || '.join(expressions)})"
    
    def generate_json_array_length(self, json_expr: str) -> str:
        """PostgreSQL JSON array length."""
        return f"jsonb_array_length({json_expr})"
    
    def generate_json_extract(self, json_expr: str, path: str) -> str:
        """PostgreSQL JSON path extraction."""
        return f"({json_expr} -> '{path.replace('$.', '')}')"
    
    def generate_json_extract_last(self, json_expr: str) -> str:
        """PostgreSQL extract last element from JSON array."""
        return f"({json_expr} -> (jsonb_array_length({json_expr}) - 1))"
    
    def generate_collection_contains_element(self, collection_expr: str, element_expr: str) -> str:
        """PostgreSQL check if collection contains specific element."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN false
                WHEN jsonb_typeof({collection_expr}) = 'array' THEN (
                    EXISTS (
                        SELECT 1 FROM jsonb_array_elements({collection_expr}) AS elem
                        WHERE elem = {element_expr}
                    )
                )
                ELSE {collection_expr} = {element_expr}
            END
        )"""
    
    def generate_element_in_collection(self, element_expr: str, collection_expr: str) -> str:
        """PostgreSQL check if element exists in collection."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN false
                WHEN jsonb_typeof({collection_expr}) = 'array' THEN (
                    EXISTS (
                        SELECT 1 FROM jsonb_array_elements({collection_expr}) AS elem
                        WHERE elem = {element_expr}
                    )
                )
                ELSE {collection_expr} = {element_expr}
            END
        )"""
    
    def generate_logical_combine(self, left_condition: str, operator: str, right_condition: str) -> str:
        """PostgreSQL logical condition combination using JSONPath operators."""
        # For JSONPath, use && and || operators
        jsonpath_op = '&&' if operator.upper() == 'AND' else '||'
        return f"({left_condition}) {jsonpath_op} ({right_condition})"
    
    def generate_collection_combine(self, first_collection: str, second_collection: str) -> str:
        """PostgreSQL collection combination."""
        return f"""(
            CASE 
                WHEN {first_collection} IS NULL AND {second_collection} IS NULL THEN NULL
                WHEN {first_collection} IS NULL THEN {second_collection}
                WHEN {second_collection} IS NULL THEN {first_collection}
                WHEN jsonb_typeof({first_collection}) = 'array' AND jsonb_typeof({second_collection}) = 'array' THEN (
                    SELECT jsonb_agg(elem)
                    FROM (
                        SELECT elem FROM jsonb_array_elements({first_collection}) AS elem
                        UNION ALL
                        SELECT elem FROM jsonb_array_elements({second_collection}) AS elem
                    ) AS combined
                )
                ELSE jsonb_build_array({first_collection}, {second_collection})
            END
        )"""
    
    def generate_where_clause_filter(self, collection_expr: str, condition_sql: str) -> str:
        """PostgreSQL WHERE clause filtering using jsonb_path_query_array."""
        # For PostgreSQL, prefer jsonb_path_query_array with JSONPath syntax
        return f"jsonb_path_query_array({collection_expr}, '$[*] ? ({condition_sql})')"
    
    def generate_select_transformation(self, collection_expr: str, transform_path: str) -> str:
        """PostgreSQL SELECT transformation for collections."""
        return f"""jsonb_agg(elem->>'{transform_path}')
        FROM jsonb_array_elements({collection_expr}) AS elem"""
    
    def generate_json_path_query_array(self, json_expr: str, path_condition: str) -> str:
        """PostgreSQL JSON path query with filtering."""
        return f"jsonb_path_query_array({json_expr}, '{path_condition}')"
    
    def generate_json_group_array_with_condition(self, collection_expr: str, condition: str, value_expr: str = "value") -> str:
        """PostgreSQL JSON array creation from filtered elements."""
        return f"""(
            SELECT jsonb_agg({value_expr})
            FROM jsonb_array_elements({collection_expr}) AS elem
            WHERE {condition.replace('value', 'elem')}
        )"""
    
    def generate_single_element_check(self, collection_expr: str) -> str:
        """PostgreSQL single element check."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN NULL
                WHEN jsonb_typeof({collection_expr}) = 'array' THEN
                    CASE 
                        WHEN jsonb_array_length({collection_expr}) = 1 THEN 
                            ({collection_expr} -> 0)
                        ELSE NULL  -- Error: not exactly one element
                    END
                ELSE {collection_expr}  -- Single value already
            END
        )"""
    
    def generate_array_slice_operation(self, array_expr: str, start_index: int, count: int = None) -> str:
        """PostgreSQL array slice operation."""
        if count is not None:
            return f"""(
                SELECT jsonb_agg(elem)
                FROM (
                    SELECT elem, row_number() OVER () as rn
                    FROM jsonb_array_elements({array_expr}) AS elem
                ) t
                WHERE rn > {start_index} AND rn <= {start_index + count}
            )"""
        else:
            return f"""(
                SELECT jsonb_agg(elem)
                FROM (
                    SELECT elem, row_number() OVER () as rn
                    FROM jsonb_array_elements({array_expr}) AS elem
                ) t
                WHERE rn > {start_index}
            )"""
    
    def generate_array_tail_operation(self, array_expr: str) -> str:
        """PostgreSQL array tail operation (all except first)."""
        return self.generate_array_slice_operation(array_expr, 1)
    
    def generate_array_distinct_operation(self, array_expr: str) -> str:
        """PostgreSQL array distinct operation."""
        return f"""(
            SELECT jsonb_agg(DISTINCT elem)
            FROM jsonb_array_elements({array_expr}) AS elem
        )"""
    
    def generate_descendants_operation(self, base_expr: str) -> str:
        """PostgreSQL descendants operation (recursive JSON traversal)."""
        # Simplified version that gets immediate children
        return f"""(
            SELECT jsonb_agg(elem)
            FROM jsonb_array_elements({base_expr}) AS elem
        )"""
    
    def generate_union_operation(self, first_collection: str, second_collection: str) -> str:
        """PostgreSQL union operation (delegates to collection combine)."""
        return self.generate_collection_combine(first_collection, second_collection)
    
    def generate_mathematical_function(self, func_name: str, operand: str) -> str:
        """PostgreSQL mathematical functions."""
        func_map = {
            'sqrt': 'sqrt',
            'ln': 'ln',
            'log': 'log',
            'exp': 'exp', 
            'power': 'power',
            'truncate': 'trunc'
        }
        sql_func = func_map.get(func_name.lower(), func_name.lower())
        if func_name.lower() == 'power':
            # PostgreSQL power function takes two arguments
            return f"power({operand}, 1)"  # Default power of 1, should be parameterized
        return f"{sql_func}({operand})"
    
    def generate_date_time_now(self) -> str:
        """PostgreSQL current timestamp."""
        return "now()"
    
    def generate_date_time_today(self) -> str:
        """PostgreSQL current date."""
        return "current_date"
    
    def generate_conditional_expression(self, condition: str, true_expr: str, false_expr: str) -> str:
        """PostgreSQL conditional expression."""
        return f"CASE WHEN {condition} THEN {true_expr} ELSE {false_expr} END"
    
    def generate_power_operation(self, base_expr: str, exponent_expr: str) -> str:
        """PostgreSQL power operation."""
        return f"power({base_expr}, {exponent_expr})"
    
    def generate_conversion_functions(self, conversion_type: str, operand: str) -> str:
        """PostgreSQL type conversion functions."""
        conversion_map = {
            'boolean': f"CAST({operand} AS BOOLEAN)",
            'integer': f"CAST({operand} AS INTEGER)",
            'decimal': f"CAST({operand} AS DECIMAL)", 
            'string': f"CAST({operand} AS TEXT)",
            'date': f"CAST({operand} AS DATE)",
            'datetime': f"CAST({operand} AS TIMESTAMP)"
        }
        return conversion_map.get(conversion_type.lower(), f"CAST({operand} AS {conversion_type.upper()})")
    
    def generate_iif_expression(self, condition: str, true_result: str, false_result: str = "NULL") -> str:
        """PostgreSQL iif conditional expression with null handling."""
        return f"""(
            CASE 
                WHEN {condition} IS NULL THEN NULL
                WHEN {condition} THEN {true_result}
                ELSE {false_result}
            END
        )"""
    
    def generate_recursive_descendants_with_cte(self, base_expr: str, max_levels: int = 5) -> str:
        """PostgreSQL recursive descendants using CTE."""
        return f"""(
            WITH RECURSIVE descendants AS (
                -- Base case: direct children
                SELECT elem as value, jsonb_typeof(elem) as type, 0 as level
                FROM jsonb_array_elements({base_expr}) AS elem
                WHERE jsonb_typeof({base_expr}) IN ('object', 'array')
                
                UNION ALL
                
                -- Recursive case: children of children
                SELECT child as value, jsonb_typeof(child) as type, descendants.level + 1
                FROM descendants, jsonb_array_elements(descendants.value) AS child
                WHERE descendants.level < {max_levels}
                  AND jsonb_typeof(descendants.value) IN ('object', 'array')
            )
            SELECT jsonb_agg(value) FROM descendants
        )"""
    
    def generate_type_filtering_operation(self, collection_expr: str, type_criteria: str) -> str:
        """PostgreSQL type filtering operation."""
        return f"""(
            SELECT jsonb_agg(elem)
            FROM jsonb_array_elements({collection_expr}) AS elem
            WHERE jsonb_typeof(elem) = '{type_criteria}'
        )"""
    
    def generate_set_intersection_operation(self, first_set: str, second_set: str) -> str:
        """PostgreSQL set intersection operation."""
        return f"""(
            SELECT jsonb_agg(DISTINCT a.elem)
            FROM jsonb_array_elements({first_set}) AS a(elem)
            WHERE EXISTS (
                SELECT 1 FROM jsonb_array_elements({second_set}) AS b(elem)
                WHERE a.elem = b.elem
            )
        )"""
    
    def generate_aggregate_with_condition(self, collection_expr: str, aggregate_type: str, condition: str) -> str:
        """PostgreSQL aggregation with condition."""
        agg_map = {
            'count': 'COUNT',
            'sum': 'SUM',
            'avg': 'AVG', 
            'min': 'MIN',
            'max': 'MAX'
        }
        agg_func = agg_map.get(aggregate_type.lower(), 'COUNT')
        return f"""(
            SELECT {agg_func}(CASE WHEN {condition} THEN elem ELSE NULL END)
            FROM jsonb_array_elements({collection_expr}) AS elem
        )"""
    
    def generate_all_elements_match_criteria(self, collection_expr: str, criteria: str) -> str:
        """PostgreSQL check if all elements match criteria."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN true
                WHEN jsonb_typeof({collection_expr}) = 'array' THEN (
                    NOT EXISTS (
                        SELECT 1 FROM jsonb_array_elements({collection_expr}) AS elem
                        WHERE NOT ({criteria.replace('value', 'elem')})
                    )
                )
                ELSE ({criteria})
            END
        )"""
    
    def generate_path_condition_comparison(self, left_expr: str, operator: str, right_expr: str, context_item: str = '$ITEM') -> str:
        """PostgreSQL path-based condition comparison with JSONPath support."""
        # Map the operator - PostgreSQL uses second element of tuple
        operator_mapping = {
            '=': '==', '==': '==', '!=': '!=', '<>': '!=', '<': '<', 
            '>': '>', '<=': '<=', '>=': '>='
        }
        mapped_op = operator_mapping.get(operator, operator)
        
        # For PostgreSQL jsonb_path_query, we need JSONPath format
        if left_expr.startswith('json_extract_string($ITEM'):
            # Convert to JSONPath format: @.field_name op "value"
            field_match = left_expr.replace('json_extract_string($ITEM, \'$.', '@.').replace('\')', '')
            return f"{field_match} {mapped_op} {right_expr}"
        else:
            return f"{left_expr} {mapped_op} {right_expr}"
    
    def generate_field_equality_condition(self, field_path: str, value: str, context_item: str = '$ITEM') -> str:
        """PostgreSQL field equality condition."""
        return f'@.{field_path} == "{value}"'
    
    def generate_field_extraction(self, item_placeholder: str, field_name: str) -> str:
        """PostgreSQL field extraction from JSON."""
        return f"@.{field_name}"
    
    def generate_field_exists_check(self, item_placeholder: str, field_name: str) -> str:
        """PostgreSQL check if field exists (is not null)."""
        return f"@.{field_name} != null"
    
    def generate_field_count_operation(self, item_placeholder: str, field_name: str) -> str:
        """PostgreSQL count elements in a field array."""
        return f"(@.{field_name}).size()"
    
    def generate_field_length_operation(self, item_placeholder: str, field_name: str) -> str:
        """PostgreSQL get length of a field string."""
        return f"(@.{field_name}).size()"
    
    def generate_path_expression_extraction(self, item_placeholder: str, path_expr: str) -> str:
        """PostgreSQL complex path expression extraction (e.g., name.family.value)."""
        return f"@.{path_expr}"
    
    def generate_exclude_operation(self, first_collection: str, second_collection: str) -> str:
        """PostgreSQL exclude operation - elements in first but not in second collection."""
        return f"""(
            CASE 
                WHEN {first_collection} IS NULL THEN NULL
                WHEN {second_collection} IS NULL THEN 
                    CASE 
                        WHEN jsonb_typeof({first_collection}) = 'array' THEN {first_collection}
                        ELSE jsonb_build_array({first_collection})
                    END
                WHEN jsonb_typeof({first_collection}) = 'array' AND jsonb_typeof({second_collection}) = 'array' THEN (
                    SELECT CASE 
                        WHEN COUNT(*) = 0 THEN '[]'::jsonb
                        ELSE jsonb_agg(DISTINCT base_value)
                    END
                    FROM (
                        SELECT base_val.value as base_value
                        FROM jsonb_array_elements({first_collection}) base_val
                        WHERE base_val.value IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1 FROM jsonb_array_elements({second_collection}) other_val
                              WHERE other_val.value = base_val.value
                          )
                    )
                )
                ELSE jsonb_build_array({first_collection})
            END
        )"""

    def generate_boolean_all_true(self, collection_expr: str) -> str:
        """PostgreSQL check if all elements in collection are true."""
        # Handle both JSONB and TEXT input by converting ->> to ->
        jsonb_expr = collection_expr.replace('->>', '->') if '->>' in collection_expr else collection_expr
        return f"""(
            CASE 
                WHEN {jsonb_expr} IS NULL THEN NULL
                WHEN jsonb_typeof({jsonb_expr}) = 'array' THEN
                    CASE 
                        WHEN jsonb_array_length({jsonb_expr}) = 0 THEN NULL
                        ELSE (
                            SELECT COUNT(*) = COUNT(CASE WHEN 
                                value::text = 'true' THEN 1 END)
                            FROM jsonb_array_elements({jsonb_expr}) AS value
                            WHERE value::text IN ('true', 'false')
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {jsonb_expr}::text = 'true' THEN true
                        WHEN {jsonb_expr}::text = 'false' THEN false
                        ELSE NULL
                    END
            END
        )"""

    def generate_boolean_all_false(self, collection_expr: str) -> str:
        """PostgreSQL check if all elements in collection are false."""
        jsonb_expr = collection_expr.replace('->>', '->') if '->>' in collection_expr else collection_expr
        return f"""(
            CASE 
                WHEN {jsonb_expr} IS NULL THEN NULL
                WHEN jsonb_typeof({jsonb_expr}) = 'array' THEN
                    CASE 
                        WHEN jsonb_array_length({jsonb_expr}) = 0 THEN NULL
                        ELSE (
                            SELECT COUNT(*) = COUNT(CASE WHEN 
                                value::text = 'false' THEN 1 END)
                            FROM jsonb_array_elements({jsonb_expr}) AS value
                            WHERE value::text IN ('true', 'false')
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {jsonb_expr}::text = 'false' THEN true
                        WHEN {jsonb_expr}::text = 'true' THEN false
                        ELSE NULL
                    END
            END
        )"""

    def generate_boolean_any_true(self, collection_expr: str) -> str:
        """PostgreSQL check if any element in collection is true."""
        jsonb_expr = collection_expr.replace('->>', '->') if '->>' in collection_expr else collection_expr
        return f"""(
            CASE 
                WHEN {jsonb_expr} IS NULL THEN NULL
                WHEN jsonb_typeof({jsonb_expr}) = 'array' THEN
                    CASE 
                        WHEN jsonb_array_length({jsonb_expr}) = 0 THEN NULL
                        ELSE EXISTS (
                            SELECT 1 FROM jsonb_array_elements({jsonb_expr}) AS value
                            WHERE value::text = 'true'
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {jsonb_expr}::text = 'true' THEN true
                        ELSE false
                    END
            END
        )"""

    def generate_boolean_any_false(self, collection_expr: str) -> str:
        """PostgreSQL check if any element in collection is false."""
        jsonb_expr = collection_expr.replace('->>', '->') if '->>' in collection_expr else collection_expr
        return f"""(
            CASE 
                WHEN {jsonb_expr} IS NULL THEN NULL
                WHEN jsonb_typeof({jsonb_expr}) = 'array' THEN
                    CASE 
                        WHEN jsonb_array_length({jsonb_expr}) = 0 THEN NULL
                        ELSE EXISTS (
                            SELECT 1 FROM jsonb_array_elements({jsonb_expr}) AS value
                            WHERE value::text = 'false'
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {jsonb_expr}::text = 'false' THEN true
                        ELSE false
                    END
            END
        )"""
    
    def generate_children_extraction(self, collection_expr: str) -> str:
        """PostgreSQL extract all immediate children from JSONB object/array."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN '[]'::jsonb
                WHEN jsonb_typeof({collection_expr}) = 'object' THEN (
                    SELECT jsonb_agg(value)
                    FROM jsonb_each({collection_expr})
                )
                WHEN jsonb_typeof({collection_expr}) = 'array' THEN (
                    SELECT jsonb_agg(value)
                    FROM jsonb_array_elements({collection_expr}) AS value
                )
                ELSE jsonb_build_array({collection_expr})
            END
        )"""
    
    def generate_tail_operation(self, array_expr: str) -> str:
        """PostgreSQL get all elements except the first (tail operation)."""
        return self.generate_array_tail_operation(array_expr)
    
    def generate_collection_contains_element(self, collection_expr: str, search_element: str) -> str:
        """PostgreSQL check if collection contains specific element."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN false
                WHEN jsonb_typeof({collection_expr}) = 'array' THEN 
                    EXISTS (
                        SELECT 1 FROM jsonb_array_elements({collection_expr}) AS value
                        WHERE value::text = {search_element}
                    )
                ELSE {collection_expr}::text = {search_element}
            END
        )"""
    
    def generate_subset_check(self, first_collection: str, second_collection: str) -> str:
        """PostgreSQL check if first collection is subset of second collection."""
        return f"""(
            CASE 
                WHEN {first_collection} IS NULL THEN true
                WHEN jsonb_typeof({first_collection}) = 'array' AND jsonb_typeof({second_collection}) = 'array' THEN (
                    NOT EXISTS (
                        SELECT 1 FROM jsonb_array_elements({first_collection}) AS elem1
                        WHERE NOT EXISTS (
                            SELECT 1 FROM jsonb_array_elements({second_collection}) AS elem2
                            WHERE elem1 = elem2
                        )
                    )
                )
                ELSE false
            END
        )"""
    
    def generate_superset_check(self, first_collection: str, second_collection: str) -> str:
        """PostgreSQL check if first collection is superset of second collection."""
        return f"""(
            CASE 
                WHEN {second_collection} IS NULL THEN true
                WHEN jsonb_typeof({first_collection}) = 'array' AND jsonb_typeof({second_collection}) = 'array' THEN (
                    NOT EXISTS (
                        SELECT 1 FROM jsonb_array_elements({second_collection}) AS elem2
                        WHERE NOT EXISTS (
                            SELECT 1 FROM jsonb_array_elements({first_collection}) AS elem1
                            WHERE elem1 = elem2
                        )
                    )
                )
                ELSE false
            END
        )"""
    
    def generate_repeat_operation(self, input_expr: str, expression: str, max_iterations: int = 10) -> str:
        """PostgreSQL repeat operation with recursive CTE."""
        return f"""(
            WITH RECURSIVE repeat_result AS (
                -- Base case: initial input
                SELECT {input_expr} as value, 0 as iteration
                
                UNION ALL
                
                -- Recursive case: apply expression to previous results
                SELECT {expression} as value, iteration + 1
                FROM repeat_result
                WHERE iteration < {max_iterations}
            )
            SELECT jsonb_agg(value) FROM repeat_result
        )"""
    
    def generate_aggregate_operation(self, collection_expr: str, aggregator_expr: str, init_value: str = "NULL") -> str:
        """PostgreSQL aggregate/fold operation."""
        return f"""(
            WITH RECURSIVE aggregate_result AS (
                -- Base case: initialize with first element or init value
                SELECT 
                    CASE 
                        WHEN {init_value} IS NOT NULL THEN {init_value}
                        ELSE (SELECT value FROM jsonb_array_elements({collection_expr}) LIMIT 1)
                    END as accumulator,
                    1 as pos
                FROM (SELECT 1) as dummy
                
                UNION ALL
                
                -- Recursive case: apply aggregator to next element
                SELECT {aggregator_expr} as accumulator, pos + 1
                FROM aggregate_result, jsonb_array_elements({collection_expr}) as elem
                WHERE pos <= jsonb_array_length({collection_expr})
            )
            SELECT accumulator FROM aggregate_result ORDER BY pos DESC LIMIT 1
        )"""
    
    def generate_iif_expression(self, condition: str, true_result: str, false_result: str) -> str:
        """PostgreSQL inline if expression."""
        return f"CASE WHEN ({condition}) THEN ({true_result}) ELSE ({false_result}) END"
    
    def generate_flatten_operation(self, collection_expr: str) -> str:
        """PostgreSQL flatten nested JSONB arrays."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN '[]'::jsonb
                WHEN jsonb_typeof({collection_expr}) = 'array' THEN (
                    SELECT jsonb_agg(nested_value)
                    FROM (
                        SELECT 
                            CASE 
                                WHEN jsonb_typeof(value) = 'array' THEN 
                                    (SELECT nested_value FROM jsonb_array_elements(value) AS nested_value)
                                ELSE value
                            END as nested_value
                        FROM jsonb_array_elements({collection_expr}) AS value
                    ) flattened
                    WHERE flattened.nested_value IS NOT NULL
                )
                ELSE jsonb_build_array({collection_expr})
            END
        )"""

    def generate_all_criteria_check(self, collection_expr: str, criteria: str) -> str:
        """PostgreSQL check if all elements satisfy criteria."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN true
                WHEN jsonb_typeof({collection_expr}) = 'array' THEN (
                    NOT EXISTS (
                        SELECT 1 FROM jsonb_array_elements({collection_expr}) AS value
                        WHERE NOT ({criteria})
                    )
                )
                ELSE ({criteria})
            END
        )"""

    def generate_converts_to_check(self, input_expr: str, target_type: str) -> str:
        """PostgreSQL check if value can be converted to target type."""
        if target_type == 'boolean':
            # PostgreSQL boolean conversion check
            return f"""(
                {input_expr} IS NOT NULL AND 
                LOWER({input_expr}::text) ~ '^(true|false|t|f|yes|no|y|n|1|0)$'
            )"""
        elif target_type in ('integer', 'decimal'):
            # Check if value matches numeric pattern
            return f"""(
                {input_expr} IS NOT NULL AND
                {input_expr}::text ~ '^[+-]?[0-9]*\\.?[0-9]+([eE][+-]?[0-9]+)?$'
            )"""
        elif target_type == 'date':
            # Check date pattern
            return f"""(
                {input_expr} IS NOT NULL AND
                {input_expr}::text ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
            )"""
        elif target_type in ('datetime', 'time'):
            # Check datetime/time patterns
            pattern = '^[0-9]{2}:[0-9]{2}:[0-9]{2}' if target_type == 'time' else '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}'
            return f"""(
                {input_expr} IS NOT NULL AND
                {input_expr}::text ~ '{pattern}'
            )"""
        else:
            # Default: check if not null
            return f"{input_expr} IS NOT NULL"

    def generate_type_cast(self, input_expr: str, target_type: str) -> str:
        """PostgreSQL cast value to target type."""
        # PostgreSQL doesn't have TRY_CAST, so use CASE for safe casting
        if target_type == 'integer':
            return f"""(
                CASE WHEN {input_expr}::text ~ '^[+-]?[0-9]+$' 
                     THEN CAST({input_expr} AS INTEGER)
                     ELSE NULL END
            )"""
        elif target_type == 'boolean':
            return f"""(
                CASE WHEN LOWER({input_expr}::text) ~ '^(true|false|t|f|1|0)$'
                     THEN CAST({input_expr} AS BOOLEAN)
                     ELSE NULL END
            )"""
        else:
            return f"CAST({input_expr} AS TEXT)"

    def generate_of_type_filter(self, collection_expr: str, target_type: str) -> str:
        """PostgreSQL filter collection by type."""
        return f"""(
            SELECT jsonb_agg(value)
            FROM jsonb_array_elements({collection_expr}) AS value
            WHERE jsonb_typeof(value) = '{target_type}'
        )"""

    def generate_equivalent_check(self, left_expr: str, right_expr: str) -> str:
        """PostgreSQL equivalent (~) operator with type-aware comparison."""
        return f"""(
            CASE 
                -- Try to compare as numbers first
                WHEN {left_expr}::text ~ '^-?[0-9]+\.?[0-9]*$' 
                     AND {right_expr}::text ~ '^-?[0-9]+\.?[0-9]*$' THEN
                    ({left_expr})::decimal = ({right_expr})::decimal
                -- Otherwise, compare as strings
                ELSE {left_expr}::text = {right_expr}::text
            END
        )"""

    def generate_age_in_years(self, birthdate_expr: str) -> str:
        """PostgreSQL calculate age in years from birth date."""
        return f"EXTRACT(year FROM AGE(CURRENT_DATE, CAST({birthdate_expr} AS DATE)))::INTEGER"

    def generate_age_in_years_at(self, birthdate_expr: str, as_of_date_expr: str) -> str:
        """PostgreSQL calculate age in years at specific date."""
        return f"EXTRACT(year FROM AGE(CAST({as_of_date_expr} AS DATE), CAST({birthdate_expr} AS DATE)))::INTEGER"

    def generate_resource_query(self, input_state: 'SQLState', resource_type: str) -> str:
        """PostgreSQL generate SQL for resource retrieval."""
        base_table = input_state.base_table or "fhir_resources"
        json_column = input_state.json_column or "resource"
        
        return f"""
        (
            SELECT {json_column}
            FROM {base_table}
            WHERE ({json_column} ->> 'resourceType') = '{resource_type}'
        )
        """

    def generate_max_operation(self, operands: list) -> str:
        """PostgreSQL max() function with multiple operands."""
        # PostgreSQL has GREATEST function that handles multiple arguments
        operands_str = ', '.join(f"CAST({op} AS DECIMAL)" for op in operands)
        return f"GREATEST({operands_str})"

    def generate_min_operation(self, operands: list) -> str:
        """PostgreSQL min() function with multiple operands."""
        # PostgreSQL has LEAST function that handles multiple arguments
        operands_str = ', '.join(f"CAST({op} AS DECIMAL)" for op in operands)
        return f"LEAST({operands_str})"

    def generate_median_operation(self, operands: list) -> str:
        """PostgreSQL median() function."""
        # PostgreSQL: PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY expression)
        operands_str = ', '.join(f"CAST({op} AS DOUBLE PRECISION)" for op in operands)
        return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {operands_str})"

    def generate_population_stddev(self, operands: list) -> str:
        """PostgreSQL population standard deviation."""
        operands_str = ', '.join(f"CAST({op} AS DOUBLE PRECISION)" for op in operands)
        return f"STDDEV_POP({operands_str})"

    def generate_population_variance(self, operands: list) -> str:
        """PostgreSQL population variance."""
        operands_str = ', '.join(f"CAST({op} AS DOUBLE PRECISION)" for op in operands)
        return f"VAR_POP({operands_str})"

    def generate_datetime_creation(self, year: str, month: str, day: str, hour: str, minute: str, second: str) -> str:
        """PostgreSQL datetime creation."""
        return f"make_timestamp({year}, {month}, {day}, {hour}, {minute}, {second})"

    def generate_union_operation(self, first_collection: str, second_collection: str) -> str:
        """PostgreSQL union of two collections."""
        return f"""(
            CASE 
                WHEN {first_collection} IS NULL AND {second_collection} IS NULL THEN NULL
                WHEN {first_collection} IS NULL THEN {second_collection}
                WHEN {second_collection} IS NULL THEN {first_collection}
                WHEN jsonb_typeof({first_collection}) = 'array' AND jsonb_typeof({second_collection}) = 'array' THEN (
                    SELECT jsonb_agg(value)
                    FROM (
                        SELECT value FROM jsonb_array_elements({first_collection})
                        UNION
                        SELECT value FROM jsonb_array_elements({second_collection})
                    ) AS combined
                )
                ELSE jsonb_build_array({first_collection}, {second_collection})
            END
        )"""

    def generate_exists_check(self, fragment: str, is_collection: bool) -> str:
        """PostgreSQL exists/empty checks."""
        if is_collection:
            return f"(jsonb_array_length({fragment}) > 0)"
        else:
            return f"({fragment} IS NOT NULL)"

    def generate_join_operation(self, collection_expr: str, separator: str) -> str:
        """PostgreSQL join operation (concatenate array elements)."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN ''
                WHEN jsonb_typeof({collection_expr}) = 'array' THEN (
                    SELECT COALESCE(string_agg(
                        CASE 
                            WHEN jsonb_typeof(value) = 'string' THEN value #>> '{{}}'
                            WHEN jsonb_typeof(value) = 'array' THEN (
                                SELECT string_agg(elem #>> '{{}}', {separator})
                                FROM jsonb_array_elements(value) AS elem
                            )
                            ELSE value #>> '{{}}'
                        END, {separator}), '')
                    FROM jsonb_array_elements({collection_expr})
                )
                ELSE COALESCE({collection_expr} #>> '{{}}', '')
            END
        )"""