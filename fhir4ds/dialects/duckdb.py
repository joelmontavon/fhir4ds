"""
DuckDB dialect implementation for FHIR4DS.

This module provides DuckDB-specific functionality for FHIR data storage,
optimized for JSON operations and bulk loading.
"""

import json
import logging
from typing import Dict, List, Any, Optional

from .base import DatabaseDialect

# Optional import for DuckDB
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

logger = logging.getLogger(__name__)


class DuckDBDialect(DatabaseDialect):
    """DuckDB implementation of the database dialect"""
    
    def __init__(self, connection: Optional[Any] = None, database: str = ":memory:"):
        super().__init__()  # Initialize base class
        
        if not DUCKDB_AVAILABLE:
            raise ImportError("DuckDB is required but not installed. Install with: pip install duckdb")
        
        # DuckDB-specific settings
        self.name = "DUCKDB"
        self.supports_jsonb = False
        self.supports_json_functions = True
        self.json_type = "JSON"
        self.json_extract_function = "json_extract"
        self.json_extract_string_function = "json_extract_string"
        self.json_array_function = "json_array"
        self.json_object_function = "json_object"
        self.json_type_function = "json_type"
        self.json_array_length_function = "json_array_length"
        self.json_each_function = "json_each"
        self.array_agg_function = "array_agg"
        self.string_agg_function = "string_agg"
        self.regex_function = "regexp_extract"
        self.cast_syntax = "::"
        self.quote_char = '"'
        
        try:
            self.connection = connection or duckdb.connect(database)
            self.connection.execute("INSTALL json; LOAD json;")
            logger.info(f"Initialized DuckDB dialect with database: {database}")
        except Exception as e:
            if "not a valid DuckDB database file" in str(e):
                # Provide helpful error message for invalid database files
                import os
                if os.path.exists(database) and database != ":memory:":
                    file_size = os.path.getsize(database)
                    if file_size == 0:
                        raise ValueError(f"Cannot open DuckDB database '{database}': The file is empty. "
                                       f"Delete the empty file and try again, or use a different filename.") from e
                    else:
                        raise ValueError(f"Cannot open DuckDB database '{database}': The file exists but is not a valid DuckDB database. "
                                       f"This could be a text file, corrupted database, or file from an incompatible DuckDB version. "
                                       f"Delete the file and try again, or use a different filename.") from e
            raise
    
    def get_connection(self) -> Any:
        return self.connection
    
    def execute_sql(self, sql: str, view_def: Optional[Dict] = None) -> 'QueryResult':
        """Execute SQL and return wrapped results"""
        # Import locally to avoid circular imports
        from .. import datastore
        from ..datastore import QueryResult
        return QueryResult(self, sql, view_def)
    
    def execute_query(self, sql: str) -> Any:
        """Execute a query and return raw results"""
        self.connection.execute(sql)
        return self.connection.fetchall()
    
    def get_query_description(self, connection: Any) -> Any:
        """Get column descriptions from last executed query"""
        return self.connection.description
    
    def create_fhir_table(self, table_name: str, json_col: str) -> None:
        """Create FHIR resources table optimized for DuckDB"""
        # Check if table already exists before dropping
        try:
            result = self.connection.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
            table_exists = result[0] > 0 if result else False
        except:
            # Fallback check method for older DuckDB versions
            try:
                self.connection.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
                table_exists = True
            except:
                table_exists = False
        
        if table_exists:
            logger.info(f"FHIR table '{table_name}' already exists, skipping creation")
            return
        
        # Create table only if it doesn't exist
        self.connection.execute("CREATE SEQUENCE IF NOT EXISTS id_sequence START 1;")
        self.connection.execute(f"""
            CREATE TABLE {table_name} (
                id INTEGER DEFAULT nextval('id_sequence'),
                {json_col} JSON
            )
        """)
        logger.info(f"Created FHIR table: {table_name}")
    
    def bulk_load_json(self, file_path: str, table_name: str, json_col: str) -> int:
        """Bulk load JSON file using DuckDB's read_json functionality"""
        # Detect file type by sampling
        with open(file_path, 'r') as f:
            sample = json.load(f)
        
        if isinstance(sample, list):
            # Array of resources
            load_sql = f"""
            INSERT INTO {table_name} ({json_col}) 
            SELECT unnest(json_extract(json, '$[*]')) 
            FROM read_json('{file_path}', records=False, ignore_errors=True, maximum_object_size=99999999)
            """
        elif sample.get('resourceType') == 'Bundle':
            # FHIR Bundle(s)
            load_sql = f"""
            INSERT INTO {table_name} ({json_col}) 
            SELECT unnest(json_extract(json, '$.entry[*].resource')) 
            FROM read_json('{file_path}', records=False, ignore_errors=True, maximum_object_size=99999999)
            WHERE json_extract(json, '$.entry') IS NOT NULL
            """
        else:
            # Individual resource(s)
            load_sql = f"""
            INSERT INTO {table_name} ({json_col}) 
            SELECT json
            FROM read_json('{file_path}', records=False, ignore_errors=True, maximum_object_size=99999999)
            WHERE json_extract_string(json, '$.resourceType') IS NOT NULL
            """
        
        # Get count before and after
        before_count = self._get_total_count(table_name)
        self.connection.execute(load_sql)
        after_count = self._get_total_count(table_name)
        
        return after_count - before_count
    
    def insert_resource(self, resource: Dict[str, Any], table_name: str, json_col: str) -> None:
        """Insert a single FHIR resource"""
        self.connection.execute(
            f"INSERT INTO {table_name} ({json_col}) VALUES (?)",
            (json.dumps(resource),)
        )
    
    def get_resource_counts(self, table_name: str, json_col: str) -> Dict[str, int]:
        """Get resource counts by type"""
        result = self.connection.execute(f"""
            SELECT json_extract_string({json_col}, '$.resourceType') as resource_type, COUNT(*) as count 
            FROM {table_name} 
            GROUP BY resource_type 
            ORDER BY count DESC
        """)
        return {row[0]: row[1] for row in result.fetchall()}
    
    def _get_total_count(self, table_name: str) -> int:
        """Get total resource count"""
        result = self.connection.execute(f"SELECT COUNT(*) FROM {table_name}")
        return result.fetchone()[0]
    
    # Dialect-specific SQL generation methods for DuckDB
    
    def extract_json_field(self, column: str, path: str) -> str:
        """Extract a JSON field as text using DuckDB's json_extract_string"""
        return f"json_extract_string({column}, '{path}')"
    
    def extract_json_object(self, column: str, path: str) -> str:
        """Extract a JSON object using DuckDB's json_extract"""
        return f"json_extract({column}, '{path}')"
    
    def extract_json_text(self, column: str, path: str) -> str:
        """Extract a JSON field as text using DuckDB's json_extract function"""
        # DuckDB uses same function for both object and text extraction
        return self.extract_json_object(column, path)
    
    def iterate_json_array(self, column: str, path: str) -> str:
        """Iterate over JSON array elements using DuckDB's json_each"""
        return f"json_each({column}, '{path}')"
    
    def check_json_exists(self, column: str, path: str) -> str:
        """Check if JSON path exists using DuckDB pattern"""
        return f"({self.extract_json_object(column, path)} IS NOT NULL)"
    
    def get_json_type(self, column: str) -> str:
        """Get JSON value type using DuckDB's json_type"""
        return f"json_type({column})"
    
    def get_json_array_length(self, column: str, path: str = None) -> str:
        """Get JSON array length using DuckDB's json_array_length"""
        if path:
            return f"json_array_length({self.extract_json_object(column, path)})"
        else:
            return f"json_array_length({column})"
    
    def aggregate_to_json_array(self, expression: str) -> str:
        """Aggregate values into a JSON array using DuckDB's json_group_array"""
        return f"json_group_array({expression})"
    
    def json_array_agg_function(self, expression: str) -> str:
        """JSON array aggregation function alias - same as aggregate_to_json_array"""
        return f"json_group_array({expression})"
    
    def coalesce_empty_array(self, expression: str) -> str:
        """COALESCE with empty array using DuckDB syntax"""
        return f"COALESCE({expression}, json_array())"
    
    def get_array_iteration_columns(self) -> tuple:
        """Get column names for array iteration - standardized to 'value' and 'ordinality'"""
        return ('value', 'ordinality')
    
    def get_object_iteration_columns(self) -> tuple:
        """Get column names for object iteration - DuckDB uses 'key' and 'value'"""
        return ('key', 'value')
    
    # DuckDB-specific optimization methods
    def bulk_insert_resources(self, resources: List[Dict[str, Any]], 
                             table_name: str, json_col: str,
                             parallel: bool = True, batch_size: int = 1000) -> int:
        """
        Efficiently bulk insert FHIR resources using DuckDB optimizations.
        
        Uses UNION ALL to insert multiple resources in a single statement for better performance.
        """
        if not resources:
            return 0
            
        import json as json_module
        
        # For very large datasets, process in batches
        if len(resources) > batch_size:
            total_loaded = 0
            for i in range(0, len(resources), batch_size):
                batch = resources[i:i + batch_size]
                total_loaded += self._bulk_insert_batch(batch, table_name, json_col)
            return total_loaded
        else:
            return self._bulk_insert_batch(resources, table_name, json_col)
    
    def _bulk_insert_batch(self, resources: List[Dict[str, Any]], 
                          table_name: str, json_col: str) -> int:
        """Insert a batch of resources using UNION ALL."""
        import json as json_module
        
        if not resources:
            return 0
        
        # Build a single INSERT with UNION ALL
        values_clauses = []
        for resource in resources:
            json_str = json_module.dumps(resource).replace("'", "''")  # Escape single quotes
            values_clauses.append(f"SELECT '{json_str}' as {json_col}")
        
        # Combine all values with UNION ALL
        union_sql = " UNION ALL ".join(values_clauses)
        insert_sql = f"INSERT INTO {table_name} ({json_col}) {union_sql}"
        
        try:
            self.connection.execute(insert_sql)
            return len(resources)
        except Exception as e:
            # Fallback to individual inserts if bulk insert fails
            logging.warning(f"Bulk insert failed, falling back to individual inserts: {e}")
            count = 0
            for resource in resources:
                try:
                    self.insert_resource(resource, table_name, json_col)
                    count += 1
                except Exception as insert_error:
                    logging.error(f"Failed to insert resource: {insert_error}")
            return count
    
    def load_json_file(self, file_path: str, table_name: str, json_col: str) -> int:
        """
        Load JSON file using DuckDB's native read_json function for optimal performance.
        
        This is much faster than parsing JSON in Python for large files.
        """
        # Get count before loading
        count_sql = f"SELECT COUNT(*) FROM {table_name}"
        before_count = self.connection.execute(count_sql).fetchone()[0]
        
        try:
            # Try different strategies for JSON loading
            
            # Strategy 1: Direct array of FHIR resources
            try:
                insert_sql = f"""
                INSERT INTO {table_name} ({json_col})
                SELECT to_json(resource_data) 
                FROM read_json('{file_path}', format='array') as resource_data
                WHERE resource_data.resourceType IS NOT NULL
                """
                self.connection.execute(insert_sql)
                
            except Exception:
                # Strategy 2: FHIR Bundle with entries
                try:
                    insert_sql = f"""
                    INSERT INTO {table_name} ({json_col})
                    SELECT to_json(entry_resource)
                    FROM (
                        SELECT unnest(entry) as entry_data
                        FROM read_json('{file_path}') 
                        WHERE resourceType = 'Bundle'
                    ),
                    LATERAL (SELECT entry_data.resource as entry_resource) as entries
                    WHERE entry_resource.resourceType IS NOT NULL
                    """
                    self.connection.execute(insert_sql)
                    
                except Exception:
                    # Strategy 3: Single resource
                    insert_sql = f"""
                    INSERT INTO {table_name} ({json_col})
                    SELECT to_json(resource_data)
                    FROM read_json('{file_path}') as resource_data
                    WHERE resource_data.resourceType IS NOT NULL
                    """
                    self.connection.execute(insert_sql)
            
            # Count inserted resources
            after_count = self.connection.execute(count_sql).fetchone()[0]
            inserted_count = after_count - before_count
            
            if inserted_count > 0:
                logging.info(f"DuckDB read_json loaded {inserted_count} resources from {file_path}")
                return inserted_count
            else:
                # If no resources were loaded, try fallback
                raise Exception("No resources loaded with read_json")
            
        except Exception as e:
            logging.warning(f"DuckDB read_json failed for {file_path}: {e}, using fallback")
            # Fallback to individual parsing
            return self._load_file_fallback(file_path, table_name, json_col)
    
    def _load_file_fallback(self, file_path: str, table_name: str, json_col: str) -> int:
        """Fallback JSON file loading using Python parsing."""
        import json as json_module
        
        loaded_count = 0
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json_module.load(f)
            
            if isinstance(data, list):
                # Array of resources
                for resource in data:
                    if isinstance(resource, dict) and 'resourceType' in resource:
                        self.insert_resource(resource, table_name, json_col)
                        loaded_count += 1
            elif isinstance(data, dict):
                if data.get('resourceType') == 'Bundle':
                    # FHIR Bundle
                    for entry in data.get('entry', []):
                        resource = entry.get('resource', {})
                        if 'resourceType' in resource:
                            self.insert_resource(resource, table_name, json_col)
                            loaded_count += 1
                elif 'resourceType' in data:
                    # Single resource
                    self.insert_resource(data, table_name, json_col)
                    loaded_count += 1
                    
        except (FileNotFoundError, PermissionError, json_module.JSONDecodeError) as e:
            # Re-raise fundamental errors that should not be silently handled
            logging.error(f"Fallback loading failed for {file_path}: {e}")
            raise
        except Exception as e:
            # Log other errors but don't raise
            logging.error(f"Fallback loading failed for {file_path}: {e}")
            
        return loaded_count
    
    def optimize_table(self, table_name: str) -> None:
        """Optimize table for better query performance (DuckDB specific)."""
        try:
            # DuckDB automatically optimizes, but we can run ANALYZE
            self.connection.execute(f"ANALYZE {table_name}")
            logging.info(f"Analyzed table {table_name} for query optimization")
        except Exception as e:
            logging.warning(f"Table optimization failed: {e}")
    
    def join_array_elements(self, base_expr: str, separator_sql: str) -> str:
        """Join array elements with separator using DuckDB's json_each"""
        return f"""
        COALESCE((
            SELECT string_agg(
                json_extract_string(json_object('v', inner_t.value), '$.v'), 
                {separator_sql}
                ORDER BY outer_t.key, inner_t.key
            )
            FROM json_each(
                CASE WHEN json_type({base_expr}) = 'ARRAY' 
                THEN {base_expr} 
                ELSE json_array({base_expr}) END
            ) AS outer_t,
            json_each(
                CASE WHEN json_type(outer_t.value) = 'ARRAY' 
                THEN outer_t.value 
                ELSE json_array(outer_t.value) END
            ) AS inner_t
            WHERE inner_t.value IS NOT NULL
        ), '')
        """
    
    def create_json_array(self, *args) -> str:
        """Create a JSON array from arguments using DuckDB's json_array"""
        if args:
            return f"json_array({', '.join(str(arg) for arg in args)})"
        return "json_array()"
    
    def create_json_object(self, *args) -> str:
        """Create a JSON object from key-value pairs using DuckDB's json_object"""
        if args:
            return f"json_object({', '.join(str(arg) for arg in args)})"
        return "json_object()"
    
    def aggregate_values(self, expression: str, distinct: bool = False) -> str:
        """Array aggregation using DuckDB's array_agg"""
        if distinct:
            return f"array_agg(DISTINCT {expression})"
        return f"array_agg({expression})"
    
    def aggregate_strings(self, expression: str, separator: str) -> str:
        """String aggregation using DuckDB's string_agg"""
        return f"string_agg({expression}, {separator})"

    def extract_nested_array_path(self, json_base: str, current_path: str, identifier_name: str, new_path: str) -> str:
        """Extract path from nested array structures using DuckDB's JSON functions"""
        # Handle root level access (current_path = "$")
        if current_path == "$":
            # Phase 4.6: Improved array extraction - extract scalar values from single-element arrays
            return f"""CASE WHEN json_type({json_base}) = 'ARRAY' 
            THEN 
                CASE 
                    WHEN json_array_length({json_base}) = 1 
                    THEN json_extract({json_base}, '$[0].{identifier_name}')
                    ELSE json_extract({json_base}, '$[*].{identifier_name}')
                END
            ELSE json_extract({json_base}, '$.{identifier_name}') 
            END"""
        
        # Handle missing properties by explicitly checking each array element
        return f"""CASE WHEN json_type(json_extract({json_base}, '{current_path}')) = 'ARRAY' 
        THEN (
            SELECT to_json(list(
                CASE 
                    WHEN json_extract(value, '$.{identifier_name}') IS NULL THEN NULL
                    ELSE json_extract(value, '$.{identifier_name}')
                END
            ))
            FROM json_each(json_extract({json_base}, '{current_path}'))
        )
        ELSE json_extract({json_base}, '{new_path}') 
        END"""
    
    def split_string(self, expression: str, delimiter: str) -> str:
        """Split string into array using DuckDB's string_split function"""
        return f"string_split(CAST({expression} AS VARCHAR), {delimiter})"
    
    def substring(self, expression: str, start: str, length: str) -> str:
        """Extract substring using DuckDB's SUBSTRING function"""
        return f"SUBSTRING({expression}, ({start}) + 1, {length})"
    
    def string_position(self, search_str: str, target_str: str) -> str:
        """Find position using DuckDB's POSITION function (0-based index)"""
        return f"CASE WHEN POSITION(CAST({search_str} AS VARCHAR) IN CAST({target_str} AS VARCHAR)) > 0 THEN POSITION(CAST({search_str} AS VARCHAR) IN CAST({target_str} AS VARCHAR)) - 1 ELSE -1 END"
    
    def string_concat(self, left: str, right: str) -> str:
        """Concatenate strings using DuckDB's || operator"""
        return f"({left} || {right})"
    
    def optimize_cte_definition(self, cte_name: str, cte_expr: str) -> str:
        """Apply DuckDB-specific CTE optimizations"""
        # DuckDB generally handles CTE optimization automatically
        # Add hints for specific patterns if beneficial
        optimized_expr = self._apply_json_optimizations(cte_expr)
        
        return f"{cte_name} AS ({optimized_expr})"
    
    def _apply_json_optimizations(self, cte_expr: str) -> str:
        """
        Apply DuckDB-specific JSON optimizations
        
        DuckDB has native JSON support that can be leveraged for better performance
        """
        # For now, return as-is since DuckDB's query planner is already excellent
        # Future enhancements could include:
        # - JSON path expression simplification
        # - Predicate pushdown hints
        # - Column store optimization hints
        
        return cte_expr
    
    def url_encode(self, expression: str) -> str:
        """URL encode string using DuckDB's url_encode function"""
        # DuckDB has built-in url_encode function
        return f"url_encode(CAST({expression} AS VARCHAR))"
    
    def url_decode(self, expression: str) -> str:
        """URL decode string using DuckDB's url_decode function"""
        # DuckDB has built-in url_decode function  
        return f"url_decode(CAST({expression} AS VARCHAR))"
    
    def base64_encode(self, expression: str) -> str:
        """Base64 encode string using DuckDB's base64 function"""
        return f"base64(CAST({expression} AS VARCHAR))"
        
    def base64_decode(self, expression: str) -> str:
        """Base64 decode string using DuckDB's from_base64 function"""
        return f"from_base64(CAST({expression} AS VARCHAR))"
    
    # Array manipulation functions for collection operations
    def array_concat_function(self, array1: str, array2: str) -> str:
        """Concatenate two JSON arrays or single values into a single JSON array."""
        return f"""
        to_json(list_concat(
            CASE 
                WHEN json_type({array1}) = 'ARRAY' THEN json_extract({array1}, '$[*]')
                ELSE [{array1}]
            END,
            CASE 
                WHEN json_type({array2}) = 'ARRAY' THEN json_extract({array2}, '$[*]')
                ELSE [{array2}]
            END
        ))
        """
    
    def array_slice_function(self, array: str, start_index: str, end_index: str) -> str:
        """Slice a JSON array from start_index to end_index (1-based indexing)."""
        return f"to_json(array_slice(json_extract({array}, '$[*]'), {start_index}, {end_index}))"
    
    def array_distinct_function(self, array: str) -> str:
        """Remove duplicates from a JSON array."""
        return f"to_json(list_distinct(json_extract({array}, '$[*]')))"
    
    def array_union_function(self, array1: str, array2: str) -> str:
        """Union two JSON arrays or single values into a single JSON array (remove duplicates, preserve order)."""
        # Use a more complex approach to preserve order while removing duplicates
        # This manually concatenates and removes duplicates while preserving the order of first occurrence
        return f"""
        (
            WITH 
            arr1 AS (
                SELECT unnest(
                    CASE 
                        WHEN json_type({array1}) = 'ARRAY' THEN json_extract({array1}, '$[*]')
                        ELSE [{array1}]
                    END
                ) as value, 1 as source_order, ROW_NUMBER() OVER () as item_order
            ),
            arr2 AS (
                SELECT unnest(
                    CASE 
                        WHEN json_type({array2}) = 'ARRAY' THEN json_extract({array2}, '$[*]')
                        ELSE [{array2}]
                    END
                ) as value, 2 as source_order, ROW_NUMBER() OVER () as item_order
            ),
            combined AS (
                SELECT value, source_order, item_order FROM arr1
                UNION ALL
                SELECT value, source_order, item_order FROM arr2
            ),
            distinct_ordered AS (
                SELECT value, MIN(source_order) as first_source, MIN(
                    CASE WHEN source_order = 1 THEN item_order ELSE 999999 + item_order END
                ) as first_occurrence
                FROM combined
                GROUP BY value
            )
            SELECT to_json(list(value ORDER BY first_occurrence))
            FROM distinct_ordered
        )
        """