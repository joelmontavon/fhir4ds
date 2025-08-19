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
