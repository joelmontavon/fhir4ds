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

    def create_terminology_system_mappings_table(self) -> None:
        """Create the terminology system mappings table for crosswalking OID/URI/URN"""
        try:
            from ..terminology.system_mappings import get_all_system_identifiers

            # Check if table already exists
            try:
                result = self.connection.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'terminology_system_mappings'").fetchone()
                table_exists = result[0] > 0 if result else False
            except:
                table_exists = False

            if table_exists:
                logger.info("Terminology system mappings table already exists, skipping creation")
                return

            # Create terminology mappings table
            create_sql = """
                CREATE TABLE terminology_system_mappings (
                    original_system VARCHAR PRIMARY KEY,
                    canonical_system VARCHAR NOT NULL,
                    system_type VARCHAR NOT NULL,
                    name VARCHAR NOT NULL
                )
            """
            self.connection.execute(create_sql)

            # Insert all mappings
            mappings = get_all_system_identifiers()
            insert_sql = """
                INSERT INTO terminology_system_mappings
                (original_system, canonical_system, system_type, name)
                VALUES (?, ?, ?, ?)
            """

            for mapping in mappings:
                self.connection.execute(insert_sql, [
                    mapping['original_system'],
                    mapping['canonical_system'],
                    mapping['system_type'],
                    mapping['name']
                ])

            logger.info(f"Created terminology mappings table with {len(mappings)} mappings")

        except Exception as e:
            self._handle_operation_error("terminology mappings table creation", e)

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
    
    def get_json_array_element(self, json_expr: str, index: int) -> str:
        """Extract element at index from JSON array."""
        return f"json_extract({json_expr}, '$[{index}]')"
    
    def get_json_extract_string(self, json_expr: str, path: str) -> str:
        """Extract string value from JSON path."""
        return f"json_extract_string({json_expr}, '{path}')"
    
    def get_array_iteration_columns(self) -> tuple:
        """Get column names for array iteration - standardized to 'value' and 'ordinality'"""
        return ('value', 'ordinality')
    
    def get_object_iteration_columns(self) -> tuple:
        """Get column names for object iteration - DuckDB uses 'key' and 'value'"""
        return ('key', 'value')
    
    # Pipeline-specific dialect method implementations
    
    def string_split_reference(self, input_sql: str) -> str:
        """Split reference string like 'ResourceType/id' to extract id part using DuckDB functions"""
        return f"list_extract(string_split({input_sql}, '/'), -1)"
    
    def starts_with_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate starts_with condition for field matching using DuckDB syntax"""
        return f"starts_with(json_extract_string({item_expr}, '$.{field_name}'), '{value}')"
    
    def ends_with_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate ends_with condition for field matching using DuckDB syntax"""
        return f"ends_with(json_extract_string({item_expr}, '$.{field_name}'), '{value}')"
    
    def contains_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate contains condition for field matching using DuckDB syntax"""
        return f"contains(json_extract_string({item_expr}, '$.{field_name}'), '{value}')"
    
    def generate_field_startswith_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate field startsWith condition - delegates to starts_with_condition"""
        return self.starts_with_condition(item_expr, field_name, value)
    
    def generate_field_endswith_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate field endsWith condition - delegates to ends_with_condition"""
        return self.ends_with_condition(item_expr, field_name, value)
    
    def generate_field_contains_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate field contains condition - delegates to contains_condition"""
        return self.contains_condition(item_expr, field_name, value)
    
    def get_value_primitive_sql(self, input_sql: str) -> str:
        """Generate SQL for getValue() function handling primitive and complex types using DuckDB functions"""
        return f"""CASE 
            WHEN {input_sql} IS NULL THEN NULL
            WHEN json_type({input_sql}) = 'STRING' THEN 
                json_extract_string({input_sql}, '$')
            WHEN json_type({input_sql}) = 'NUMBER' THEN 
                json_extract({input_sql}, '$')::varchar
            WHEN json_type({input_sql}) = 'BOOLEAN' THEN 
                CASE WHEN json_extract({input_sql}, '$')::boolean THEN 'true' ELSE 'false' END
            WHEN json_type({input_sql}) = 'OBJECT' AND json_extract_string({input_sql}, '$.value') IS NOT NULL THEN
                json_extract_string({input_sql}, '$.value')
            WHEN json_type({input_sql}) = 'OBJECT' AND json_extract_string({input_sql}, '$.family') IS NOT NULL THEN
                COALESCE(
                    json_extract_string({input_sql}, '$.family') || 
                    CASE 
                        WHEN json_extract({input_sql}, '$.given') IS NOT NULL 
                        THEN ', ' || array_to_string(
                            json_extract({input_sql}, '$.given')::varchar[], ', '
                        )
                        ELSE '' 
                    END,
                    json_extract_string({input_sql}, '$.family')
                )
            WHEN json_type({input_sql}) = 'ARRAY' AND json_array_length({input_sql}) > 0 THEN
                COALESCE(
                    json_extract_string(json_extract({input_sql}, '$[0]'), '$.family') || 
                    CASE 
                        WHEN json_extract(json_extract({input_sql}, '$[0]'), '$.given') IS NOT NULL 
                        THEN ', ' || array_to_string(
                            json_extract(json_extract({input_sql}, '$[0]'), '$.given')::varchar[], ', '
                        )
                        ELSE '' 
                    END,
                    json_extract_string(json_extract({input_sql}, '$[0]'), '$.family'),
                    json_extract_string(json_extract({input_sql}, '$[0]'), '$.value'),
                    json_extract(json_extract({input_sql}, '$[0]'), '$')::varchar
                )
            ELSE 
                {input_sql}::varchar
        END"""
    
    def resolve_reference_sql(self, input_sql: str) -> str:
        """Generate SQL for resolve() function to resolve FHIR references using DuckDB syntax"""
        return f"""CASE 
            WHEN {input_sql} IS NULL THEN NULL
            WHEN json_extract_string({input_sql}, '$.reference') IS NOT NULL THEN
                (
                    SELECT fhir_data.data 
                    FROM fhir_data 
                    WHERE json_extract_string(fhir_data.data, '$.resourceType') || '/' || json_extract_string(fhir_data.data, '$.id') = json_extract_string({input_sql}, '$.reference')
                    LIMIT 1
                )
            WHEN json_type({input_sql}) = 'STRING' THEN
                (
                    SELECT fhir_data.data 
                    FROM fhir_data 
                    WHERE json_extract_string(fhir_data.data, '$.resourceType') || '/' || json_extract_string(fhir_data.data, '$.id') = {input_sql}
                    LIMIT 1
                )
            ELSE NULL
        END"""
    
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

    def extract_nested_array_path(self, json_base: str, current_path: str, identifier_name: str, new_path: str, context=None) -> str:
        """Extract path from nested array structures using DuckDB's JSON functions"""
        # Check if this is a collection operation that needs array preservation
        try:
            from fhir4ds.dialects.context import ExtractionContext
            is_collection_op = context == ExtractionContext.COLLECTION_OPERATION
        except ImportError:
            is_collection_op = False
        
        # Handle root level access (current_path = "$")
        if current_path == "$":
            array_result = f"json_extract({json_base}, '$[*].{identifier_name}')"
            scalar_result = f"json_extract({json_base}, '$.{identifier_name}')"
            
            # For collection operations, always preserve array structure
            if is_collection_op:
                return f"""CASE WHEN json_type({json_base}) = 'ARRAY'
                THEN {array_result}
                ELSE CASE WHEN {scalar_result} IS NOT NULL THEN json_array({scalar_result}) ELSE NULL END
                END"""
            
            # Context-aware array extraction for non-collection operations:
            # - For single-element arrays from where(), extract scalar values
            # - For multi-element arrays, return arrays for collection operations
            return f"""CASE WHEN json_type({json_base}) = 'ARRAY'
            THEN CASE WHEN json_array_length({json_base}) = 1
                 THEN json_extract({json_base}, '$[0].{identifier_name}')
                 ELSE {array_result}
                 END
            ELSE {scalar_result}
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
    
    def get_collection_count_expression(self, collection_expr: str) -> str:
        """Get count of elements in a collection for DuckDB."""
        return f"""CASE 
            WHEN json_type({collection_expr}) = 'ARRAY' 
            THEN json_array_length({collection_expr})
            WHEN {collection_expr} IS NULL 
            THEN 0
            ELSE 1
        END"""
    
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
    
    # Pipeline-specific optimized implementations for DuckDB
    
    def extract_json_path(self, base_expr: str, json_path: str, context_mode: 'ContextMode') -> str:
        """
        DuckDB-optimized JSON path extraction with context awareness.
        """
        from ..pipeline.core.base import ContextMode
        
        if context_mode == ContextMode.COLLECTION:
            # Use DuckDB's efficient JSON array handling
            return f"json_extract({base_expr}, '{json_path}')"
        elif context_mode == ContextMode.WHERE_CLAUSE:
            # Optimize for boolean evaluation
            return f"(json_extract({base_expr}, '{json_path}') IS NOT NULL)"
        else:
            # Standard string extraction
            return f"json_extract_string({base_expr}, '{json_path}')"
    
    def extract_array_element(self, array_expr: str, index: int) -> str:
        """DuckDB-optimized array element extraction."""
        return f"json_extract({array_expr}, '$[{index}]')"
    
    def extract_last_array_element(self, array_expr: str) -> str:
        """DuckDB-optimized last array element extraction."""
        return f"json_extract({array_expr}, '$[' || (json_array_length({array_expr}) - 1) || ']')"
    
    def _extract_collection_path(self, base_expr: str, json_path: str) -> str:
        """DuckDB-optimized collection path extraction."""
        # DuckDB handles JSON arrays efficiently with json_extract
        return f"json_extract({base_expr}, '{json_path}')"
    
    def _extract_boolean_path(self, base_expr: str, json_path: str) -> str:
        """DuckDB-optimized boolean path extraction."""
        # Use DuckDB's efficient null checking
        return f"(json_extract({base_expr}, '{json_path}') IS NOT NULL)"
    
    # Implementation of new abstract methods for FHIRPath operations
    
    def try_cast(self, expression: str, target_type: str) -> str:
        """DuckDB safe type conversion using TRY_CAST"""
        return f"TRY_CAST({expression} AS {target_type.upper()})"
    
    def string_to_char_array(self, expression: str) -> str:
        """DuckDB string to character array conversion"""
        return f"""(
            SELECT json_group_array(chr)
            FROM (
                SELECT substr({expression}, i, 1) as chr
                FROM (
                    SELECT generate_series(1, length({expression})) as i
                )
            )
        )"""
    
    def regex_matches(self, string_expr: str, pattern: str) -> str:
        """DuckDB regex pattern matching"""
        return f"regexp_matches({string_expr}, {pattern})"
    
    def regex_replace(self, string_expr: str, pattern: str, replacement: str) -> str:
        """DuckDB regex pattern replacement"""
        return f"regexp_replace({string_expr}, {pattern}, {replacement}, 'g')"
    
    def json_group_array(self, value_expr: str, from_clause: str = None) -> str:
        """DuckDB JSON array aggregation"""
        if from_clause:
            return f"(SELECT json_group_array({value_expr}) FROM {from_clause})"
        return f"json_group_array({value_expr})"
    
    def json_each(self, json_expr: str, path: str = None) -> str:
        """DuckDB JSON array iteration"""
        if path:
            return f"json_each({json_expr}, '{path}')"
        return f"json_each({json_expr})"
    
    def json_typeof(self, json_expr: str) -> str:
        """DuckDB JSON type checking"""
        return f"json_type({json_expr})"
    
    def json_array_elements(self, json_expr: str, with_ordinality: bool = False) -> str:
        """DuckDB JSON array element extraction"""
        # DuckDB uses json_each for this functionality
        return f"json_each({json_expr})"
    
    def cast_to_timestamp(self, expression: str) -> str:
        """DuckDB timestamp casting"""
        return f"CAST({expression} AS TIMESTAMP)"
    
    def cast_to_time(self, expression: str) -> str:
        """DuckDB time casting"""
        return f"CAST({expression} AS TIME)"
    
    def _generate_type_display_sql(self, input_sql: str, type_structure: Dict[str, Any], 
                                   array_handling: str) -> str:
        """DuckDB-specific FHIR type display generation."""
        fields = type_structure["fields"]
        arrays = type_structure.get("arrays", [])
        template = type_structure.get("display_template", "{text || value}")
        
        # Generate field extraction logic
        field_extractions = {}
        for field in fields:
            if field in arrays:
                if array_handling == "first":
                    # Get first element of array
                    field_extractions[field] = f"json_extract_string(json_extract({input_sql}, '$.{field}[0]'), '$')"
                elif array_handling == "concat":
                    # Join array elements with comma
                    field_extractions[field] = f"array_to_string(CAST(json_extract({input_sql}, '$.{field}') AS VARCHAR[]), ', ')"
                elif array_handling == "all":
                    # Return full array as JSON
                    field_extractions[field] = f"json_extract({input_sql}, '$.{field}')"
            else:
                # Simple field extraction
                field_extractions[field] = f"json_extract_string({input_sql}, '$.{field}')"
        
        # Apply template logic
        return self._apply_display_template(template, field_extractions)
    
    # Function-specific SQL generation methods
    # These replace hardcoded dialect conditionals in functions.py
    
    def generate_array_contains(self, array_sql: str, element_sql: str) -> str:
        """DuckDB array contains check."""
        return f"list_contains({array_sql}, {element_sql})"

    def generate_array_length(self, array_sql: str) -> str:
        """DuckDB array length."""
        return f"array_length({array_sql})"

    def generate_substring_sql(self, string_sql: str, start_pos: str, length: Optional[str] = None) -> str:
        """DuckDB substring operation."""
        if length:
            return f"substr({string_sql}, {start_pos}, {length})"
        return f"substr({string_sql}, {start_pos})"

    def generate_string_split(self, string_sql: str, delimiter: str) -> str:
        """DuckDB string split."""
        return f"string_split({string_sql}, {delimiter})"

    def generate_array_element_at(self, array_sql: str, index: int) -> str:
        """DuckDB array element access (1-based indexing)."""
        return f"{array_sql}[{index + 1}]"

    def generate_case_insensitive_like(self, field_sql: str, pattern: str) -> str:
        """DuckDB case-insensitive pattern matching."""
        return f"lower({field_sql}) LIKE lower({pattern})"

    def generate_regexp_match(self, field_sql: str, pattern: str) -> str:
        """DuckDB regex matching."""
        return f"regexp_matches({field_sql}, {pattern})"

    def generate_date_arithmetic(self, date_sql: str, interval: str, unit: str) -> str:
        """DuckDB date arithmetic."""
        return f"{date_sql} + INTERVAL {interval} {unit}"

    def generate_cast_to_numeric(self, value_sql: str) -> str:
        """DuckDB numeric casting."""
        return f"CAST({value_sql} AS DECIMAL)"

    def generate_null_coalesce(self, *expressions: str) -> str:
        """DuckDB null coalescing."""
        return f"coalesce({', '.join(expressions)})"
    
    def generate_json_typeof(self, json_expr: str) -> str:
        """DuckDB JSON type detection."""
        return f"json_type({json_expr})"
    
    def generate_string_concat(self, *expressions: str) -> str:
        """DuckDB string concatenation."""
        return f"({' || '.join(expressions)})"
    
    def generate_json_array_length(self, json_expr: str) -> str:
        """DuckDB JSON array length."""
        return f"json_array_length({json_expr})"
    
    def generate_json_extract(self, json_expr: str, path: str) -> str:
        """DuckDB JSON path extraction."""
        return f"json_extract({json_expr}, '{path}')"
    
    def generate_json_extract_last(self, json_expr: str) -> str:
        """DuckDB extract last element from JSON array."""
        return f"json_extract({json_expr}, '$[' || (json_array_length({json_expr}) - 1) || ']')"
    
    def generate_collection_contains_element(self, collection_expr: str, element_expr: str) -> str:
        """DuckDB check if collection contains specific element."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN false
                WHEN json_type({collection_expr}) = 'ARRAY' THEN (
                    EXISTS (
                        SELECT 1 FROM json_each({collection_expr})
                        WHERE value = {element_expr}
                    )
                )
                ELSE {collection_expr} = {element_expr}
            END
        )"""
    
    def generate_element_in_collection(self, element_expr: str, collection_expr: str) -> str:
        """DuckDB check if element exists in collection."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN false
                WHEN json_type({collection_expr}) = 'ARRAY' THEN (
                    EXISTS (
                        SELECT 1 FROM json_each({collection_expr}) AS elem
                        WHERE elem.value = {element_expr}
                    )
                )
                ELSE {collection_expr} = {element_expr}
            END
        )"""
    
    def generate_logical_combine(self, left_condition: str, operator: str, right_condition: str) -> str:
        """DuckDB logical condition combination."""
        return f"({left_condition}) {operator.upper()} ({right_condition})"
    
    def generate_collection_combine(self, first_collection: str, second_collection: str) -> str:
        """DuckDB collection combination."""
        return f"""(
            CASE 
                WHEN {first_collection} IS NULL AND {second_collection} IS NULL THEN NULL
                WHEN {first_collection} IS NULL THEN {second_collection}
                WHEN {second_collection} IS NULL THEN {first_collection}
                WHEN json_type({first_collection}) = 'ARRAY' AND json_type({second_collection}) = 'ARRAY' THEN (
                    SELECT json_group_array(value)
                    FROM (
                        SELECT value FROM json_each({first_collection})
                        UNION ALL
                        SELECT value FROM json_each({second_collection})
                    )
                )
                ELSE json_array({first_collection}, {second_collection})
            END
        )"""

    def generate_self_combine_operation(self, collection: str) -> str:
        """DuckDB self-combination optimization (x.combine(x))."""
        return f"""(
            CASE 
                WHEN {collection} IS NULL THEN NULL
                WHEN json_type({collection}) = 'ARRAY' THEN (
                    SELECT json_group_array(value)
                    FROM (
                        SELECT value FROM json_each({collection})
                        UNION ALL
                        SELECT value FROM json_each({collection})
                    )
                )
                ELSE json_array({collection}, {collection})
            END
        )"""
    
    def generate_where_clause_filter(self, collection_expr: str, condition_sql: str) -> str:
        """DuckDB WHERE clause filtering for collections."""
        return f"""(
            SELECT json_group_array(item.value)
            FROM json_each({collection_expr}) AS item
            WHERE {condition_sql.replace('$ITEM', 'item.value')}
        )"""
    
    def generate_select_transformation(self, collection_expr: str, transform_path: str) -> str:
        """DuckDB SELECT transformation for collections."""
        return f"""json_array(
            SELECT json_extract(value, '$.{transform_path}')
            FROM json_each({collection_expr})
        )"""
    
    def generate_json_path_query_array(self, json_expr: str, path_condition: str) -> str:
        """DuckDB JSON path query with filtering (fallback to WHERE clause filter)."""
        return self.generate_where_clause_filter(json_expr, path_condition)
    
    def generate_json_group_array_with_condition(self, collection_expr: str, condition: str, value_expr: str = "value") -> str:
        """DuckDB JSON array creation from filtered elements."""
        return f"""(
            SELECT json_group_array({value_expr})
            FROM json_each({collection_expr})
            WHERE {condition}
        )"""
    
    def generate_single_element_check(self, collection_expr: str) -> str:
        """DuckDB single element check."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN NULL
                WHEN json_type({collection_expr}) = 'ARRAY' THEN
                    CASE 
                        WHEN json_array_length({collection_expr}) = 1 THEN 
                            json_extract({collection_expr}, '$[0]')
                        ELSE NULL  -- Error: not exactly one element
                    END
                ELSE {collection_expr}  -- Single value already
            END
        )"""
    
    def generate_array_slice_operation(self, array_expr: str, start_index: int, count: int = None) -> str:
        """DuckDB array slice operation."""
        if count is not None:
            return f"""(
                SELECT json_group_array(value)
                FROM (
                    SELECT value, row_number() OVER () as rn
                    FROM json_each({array_expr})
                ) 
                WHERE rn > {start_index} AND rn <= {start_index + count}
            )"""
        else:
            return f"""(
                SELECT json_group_array(value)
                FROM (
                    SELECT value, row_number() OVER () as rn
                    FROM json_each({array_expr})
                ) 
                WHERE rn > {start_index}
            )"""
    
    def generate_array_tail_operation(self, array_expr: str) -> str:
        """DuckDB array tail operation (all except first)."""
        return self.generate_array_slice_operation(array_expr, 1)
    
    def generate_array_distinct_operation(self, array_expr: str) -> str:
        """DuckDB array distinct operation."""
        return f"""(
            SELECT json_group_array(value)
            FROM (
                SELECT DISTINCT value
                FROM json_each({array_expr})
            )
        )"""
    
    def generate_descendants_operation(self, base_expr: str) -> str:
        """DuckDB descendants operation (recursive JSON traversal)."""
        # This is a complex operation that would need recursive CTE
        # For now, return a simplified version that gets immediate children
        return f"""(
            SELECT json_group_array(value)
            FROM json_each({base_expr})
        )"""
    
    def generate_union_operation(self, first_collection: str, second_collection: str) -> str:
        """DuckDB union operation (delegates to collection combine)."""
        return self.generate_collection_combine(first_collection, second_collection)
    
    def generate_mathematical_function(self, func_name: str, operand: str) -> str:
        """DuckDB mathematical functions."""
        func_map = {
            'sqrt': 'sqrt',
            'ln': 'ln', 
            'log': 'log10',
            'exp': 'exp',
            'power': 'pow',
            'truncate': 'trunc'
        }
        sql_func = func_map.get(func_name.lower(), func_name.lower())
        return f"{sql_func}({operand})"
    
    def generate_date_time_now(self) -> str:
        """DuckDB current timestamp."""
        return "now()"
    
    def generate_date_time_today(self) -> str:
        """DuckDB current date."""
        return "current_date"
    
    def generate_conditional_expression(self, condition: str, true_expr: str, false_expr: str) -> str:
        """DuckDB conditional expression."""
        return f"CASE WHEN {condition} THEN {true_expr} ELSE {false_expr} END"
    
    def generate_power_operation(self, base_expr: str, exponent_expr: str) -> str:
        """DuckDB power operation."""
        return f"pow({base_expr}, {exponent_expr})"
    
    def generate_conversion_functions(self, conversion_type: str, operand: str) -> str:
        """DuckDB type conversion functions."""
        conversion_map = {
            'boolean': f"CAST({operand} AS BOOLEAN)",
            'integer': f"CAST({operand} AS INTEGER)", 
            'decimal': f"CAST({operand} AS DECIMAL)",
            'string': f"CAST({operand} AS VARCHAR)",
            'date': f"CAST({operand} AS DATE)",
            'datetime': f"CAST({operand} AS TIMESTAMP)"
        }
        return conversion_map.get(conversion_type.lower(), f"CAST({operand} AS {conversion_type.upper()})")
    
    def generate_iif_expression(self, condition: str, true_result: str, false_result: str = "NULL") -> str:
        """DuckDB iif conditional expression with null handling."""
        return f"""(
            CASE 
                WHEN {condition} IS NULL THEN NULL
                WHEN {condition} THEN {true_result}
                ELSE {false_result}
            END
        )"""
    
    def generate_recursive_descendants_with_cte(self, base_expr: str, max_levels: int = 5) -> str:
        """DuckDB recursive descendants using CTE."""
        return f"""(
            WITH RECURSIVE descendants AS (
                -- Base case: direct children  
                SELECT value, json_type(value) as type, 0 as level
                FROM json_each({base_expr})
                WHERE json_type({base_expr}) IN ('OBJECT', 'ARRAY')
                
                UNION ALL
                
                -- Recursive case: children of children
                SELECT child.value, json_type(child.value) as type, descendants.level + 1
                FROM descendants, json_each(descendants.value) AS child
                WHERE descendants.level < {max_levels}
                  AND json_type(descendants.value) IN ('OBJECT', 'ARRAY')
            )
            SELECT json_group_array(value) FROM descendants
        )"""
    
    def generate_type_filtering_operation(self, collection_expr: str, type_criteria: str) -> str:
        """DuckDB type filtering operation."""
        return f"""(
            SELECT json_group_array(value)
            FROM json_each({collection_expr})
            WHERE json_type(value) = '{type_criteria}'
        )"""
    
    def generate_set_intersection_operation(self, first_set: str, second_set: str) -> str:
        """DuckDB set intersection operation."""
        return f"""(
            SELECT json_group_array(DISTINCT a.value)
            FROM json_each({first_set}) AS a
            WHERE EXISTS (
                SELECT 1 FROM json_each({second_set}) AS b
                WHERE a.value = b.value
            )
        )"""
    
    def generate_aggregate_with_condition(self, collection_expr: str, aggregate_type: str, condition: str) -> str:
        """DuckDB aggregation with condition."""
        agg_map = {
            'count': 'COUNT',
            'sum': 'SUM', 
            'avg': 'AVG',
            'min': 'MIN',
            'max': 'MAX'
        }
        agg_func = agg_map.get(aggregate_type.lower(), 'COUNT')
        return f"""(
            SELECT {agg_func}(CASE WHEN {condition} THEN value ELSE NULL END)
            FROM json_each({collection_expr})
        )"""
    
    def generate_all_elements_match_criteria(self, collection_expr: str, criteria: str) -> str:
        """DuckDB check if all elements match criteria."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN true
                WHEN json_type({collection_expr}) = 'ARRAY' THEN (
                    NOT EXISTS (
                        SELECT 1 FROM json_each({collection_expr})
                        WHERE NOT ({criteria})
                    )
                )
                ELSE ({criteria})
            END
        )"""
    
    def generate_path_condition_comparison(self, left_expr: str, operator: str, right_expr: str, context_item: str = '$ITEM') -> str:
        """DuckDB path-based condition comparison."""
        # Map the operator - DuckDB uses first element of tuple
        operator_mapping = {
            '=': '=', '==': '=', '!=': '!=', '<>': '<>', '<': '<', 
            '>': '>', '<=': '<=', '>=': '>='
        }
        mapped_op = operator_mapping.get(operator, operator)
        return f"{left_expr} {mapped_op} {right_expr}"
    
    def generate_field_equality_condition(self, field_path: str, value: str, context_item: str = '$ITEM') -> str:
        """DuckDB field equality condition."""
        return f"json_extract_string({context_item}, '$.{field_path}') = '{value}'"
    
    def generate_field_extraction(self, item_placeholder: str, field_name: str) -> str:
        """DuckDB field extraction from JSON."""
        return f"json_extract_string({item_placeholder}, '$.{field_name}')"
    
    def generate_field_exists_check(self, item_placeholder: str, field_name: str) -> str:
        """DuckDB check if field exists (is not null)."""
        return f"json_extract_string({item_placeholder}, '$.{field_name}') IS NOT NULL"
    
    def generate_field_count_operation(self, item_placeholder: str, field_name: str) -> str:
        """DuckDB count elements in a field array."""
        return f"json_array_length(json_extract({item_placeholder}, '$.{field_name}'))"
    
    def generate_field_length_operation(self, item_placeholder: str, field_name: str) -> str:
        """DuckDB get length of a field string."""
        return f"LENGTH(json_extract_string({item_placeholder}, '$.{field_name}'))"
    
    def generate_path_expression_extraction(self, item_placeholder: str, path_expr: str) -> str:
        """DuckDB complex path expression extraction (e.g., name.family.value)."""
        return f"json_extract_string({item_placeholder}, '$.{path_expr}')"
    
    def generate_exclude_operation(self, first_collection: str, second_collection: str) -> str:
        """DuckDB exclude operation - elements in first but not in second collection."""
        return f"""(
            CASE 
                WHEN {first_collection} IS NULL THEN NULL
                WHEN {second_collection} IS NULL THEN 
                    CASE 
                        WHEN json_type({first_collection}) = 'ARRAY' THEN {first_collection}
                        ELSE json_array({first_collection})
                    END
                WHEN json_type({first_collection}) = 'ARRAY' AND json_type({second_collection}) = 'ARRAY' THEN (
                    SELECT CASE 
                        WHEN COUNT(*) = 0 THEN json_array()
                        ELSE json_group_array(DISTINCT base_value)
                    END
                    FROM (
                        SELECT base_val.value as base_value
                        FROM json_each({first_collection}) base_val
                        WHERE base_val.value IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1 FROM json_each({second_collection}) other_val
                              WHERE other_val.value = base_val.value
                          )
                    )
                )
                ELSE json_array({first_collection})
            END
        )"""
    
    def generate_boolean_all_true(self, collection_expr: str) -> str:
        """DuckDB check if all elements in collection are true."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN NULL
                WHEN json_type({collection_expr}) = 'ARRAY' THEN
                    CASE 
                        WHEN json_array_length({collection_expr}) = 0 THEN NULL
                        -- If any value is null, return null
                        WHEN EXISTS (
                            SELECT 1 FROM json_each({collection_expr})
                            WHERE json_extract_string(value, '$') IS NULL OR 
                                  json_extract_string(value, '$') NOT IN ('true', 'false')
                        ) THEN NULL
                        ELSE (
                            SELECT COUNT(*) = COUNT(CASE WHEN 
                                json_extract_string(value, '$') = 'true' THEN 1 END)
                            FROM json_each({collection_expr})
                            WHERE json_extract_string(value, '$') IN ('true', 'false')
                        )
                    END
                ELSE 
                    CASE 
                        WHEN json_extract_string({collection_expr}, '$') = 'true' THEN true
                        WHEN json_extract_string({collection_expr}, '$') = 'false' THEN false
                        ELSE NULL
                    END
            END
        )"""
    
    def generate_boolean_all_false(self, collection_expr: str) -> str:
        """DuckDB check if all elements in collection are false."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN NULL
                WHEN json_type({collection_expr}) = 'ARRAY' THEN
                    CASE 
                        WHEN json_array_length({collection_expr}) = 0 THEN NULL
                        -- If any value is null, return null
                        WHEN EXISTS (
                            SELECT 1 FROM json_each({collection_expr})
                            WHERE json_extract_string(value, '$') IS NULL OR 
                                  json_extract_string(value, '$') NOT IN ('true', 'false')
                        ) THEN NULL
                        ELSE (
                            SELECT COUNT(*) = COUNT(CASE WHEN 
                                json_extract_string(value, '$') = 'false' THEN 1 END)
                            FROM json_each({collection_expr})
                            WHERE json_extract_string(value, '$') IN ('true', 'false')
                        )
                    END
                ELSE 
                    CASE 
                        WHEN json_extract_string({collection_expr}, '$') = 'false' THEN true
                        WHEN json_extract_string({collection_expr}, '$') = 'true' THEN false
                        ELSE NULL
                    END
            END
        )"""
    
    def generate_boolean_any_true(self, collection_expr: str) -> str:
        """DuckDB check if any element in collection is true."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN NULL
                WHEN json_type({collection_expr}) = 'ARRAY' THEN
                    CASE 
                        WHEN json_array_length({collection_expr}) = 0 THEN NULL
                        ELSE EXISTS (
                            SELECT 1 FROM json_each({collection_expr})
                            WHERE json_extract_string(value, '$') = 'true'
                        )
                    END
                ELSE 
                    CASE 
                        WHEN json_extract_string({collection_expr}, '$') = 'true' THEN true
                        ELSE false
                    END
            END
        )"""
    
    def generate_boolean_any_false(self, collection_expr: str) -> str:
        """DuckDB check if any element in collection is false."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN NULL
                WHEN json_type({collection_expr}) = 'ARRAY' THEN
                    CASE 
                        WHEN json_array_length({collection_expr}) = 0 THEN NULL
                        ELSE EXISTS (
                            SELECT 1 FROM json_each({collection_expr})
                            WHERE json_extract_string(value, '$') = 'false'
                        )
                    END
                ELSE 
                    CASE 
                        WHEN json_extract_string({collection_expr}, '$') = 'false' THEN true
                        ELSE false
                    END
            END
        )"""
    
    def generate_children_extraction(self, collection_expr: str) -> str:
        """DuckDB extract all immediate children from JSON object/array."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN json_array()
                WHEN json_type({collection_expr}) = 'OBJECT' THEN (
                    SELECT json_group_array(value)
                    FROM json_each({collection_expr})
                )
                WHEN json_type({collection_expr}) = 'ARRAY' THEN (
                    SELECT json_group_array(value)
                    FROM json_each({collection_expr})
                )
                ELSE json_array({collection_expr})
            END
        )"""
    
    def generate_tail_operation(self, array_expr: str) -> str:
        """DuckDB get all elements except the first (tail operation)."""
        return self.generate_array_tail_operation(array_expr)
    
    def generate_collection_contains_element(self, collection_expr: str, search_element: str) -> str:
        """DuckDB check if collection contains specific element."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN false
                WHEN json_type({collection_expr}) = 'ARRAY' THEN 
                    EXISTS (
                        SELECT 1 FROM json_each({collection_expr})
                        WHERE json_extract_string(value, '$') = {search_element}
                    )
                ELSE json_extract_string({collection_expr}, '$') = {search_element}
            END
        )"""
    
    def generate_subset_check(self, first_collection: str, second_collection: str) -> str:
        """DuckDB check if first collection is subset of second collection."""
        return f"""(
            CASE 
                WHEN {first_collection} IS NULL THEN true
                WHEN json_type({first_collection}) = 'ARRAY' AND json_type({second_collection}) = 'ARRAY' THEN (
                    NOT EXISTS (
                        SELECT 1 FROM json_each({first_collection}) AS elem1
                        WHERE NOT EXISTS (
                            SELECT 1 FROM json_each({second_collection}) AS elem2
                            WHERE elem1.value = elem2.value
                        )
                    )
                )
                ELSE false
            END
        )"""
    
    def generate_superset_check(self, first_collection: str, second_collection: str) -> str:
        """DuckDB check if first collection is superset of second collection."""
        return f"""(
            CASE 
                WHEN {second_collection} IS NULL THEN true
                WHEN json_type({first_collection}) = 'ARRAY' AND json_type({second_collection}) = 'ARRAY' THEN (
                    NOT EXISTS (
                        SELECT 1 FROM json_each({second_collection}) AS elem2
                        WHERE NOT EXISTS (
                            SELECT 1 FROM json_each({first_collection}) AS elem1
                            WHERE elem1.value = elem2.value
                        )
                    )
                )
                ELSE false
            END
        )"""    
    def generate_repeat_operation(self, input_expr: str, expression: str, max_iterations: int = 10) -> str:
        """DuckDB repeat operation with recursive CTE."""
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
            SELECT json_group_array(value) FROM repeat_result
        )"""
    
    def generate_aggregate_operation(self, collection_expr: str, aggregator_expr: str, init_value: str = "NULL") -> str:
        """DuckDB aggregate/fold operation."""
        return f"""(
            WITH RECURSIVE aggregate_result AS (
                -- Base case: initialize with first element or init value
                SELECT 
                    CASE 
                        WHEN {init_value} IS NOT NULL THEN {init_value}
                        ELSE (SELECT value FROM json_each({collection_expr}) LIMIT 1)
                    END as accumulator,
                    1 as pos
                FROM (SELECT 1) as dummy
                
                UNION ALL
                
                -- Recursive case: apply aggregator to next element
                SELECT {aggregator_expr} as accumulator, pos + 1
                FROM aggregate_result, json_each({collection_expr}) as elem
                WHERE pos <= json_array_length({collection_expr})
            )
            SELECT accumulator FROM aggregate_result ORDER BY pos DESC LIMIT 1
        )"""
    
    def generate_iif_expression(self, condition: str, true_result: str, false_result: str) -> str:
        """DuckDB inline if expression."""
        return f"CASE WHEN ({condition}) THEN ({true_result}) ELSE ({false_result}) END"
    
    def generate_flatten_operation(self, collection_expr: str) -> str:
        """DuckDB flatten nested JSON arrays."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN json_array()
                WHEN json_type({collection_expr}) = 'ARRAY' THEN (
                    SELECT json_group_array(nested_value)
                    FROM (
                        SELECT 
                            CASE 
                                WHEN json_type(value) = 'ARRAY' THEN 
                                    (SELECT nested_value FROM json_each(value) AS nested)
                                ELSE value
                            END as nested_value
                        FROM json_each({collection_expr})
                    ) flattened
                    WHERE flattened.nested_value IS NOT NULL
                )
                ELSE json_array({collection_expr})
            END
        )"""
    
    def generate_all_criteria_check(self, collection_expr: str, criteria: str) -> str:
        """DuckDB check if all elements satisfy criteria."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN true
                WHEN json_type({collection_expr}) = 'ARRAY' THEN (
                    NOT EXISTS (
                        SELECT 1 FROM json_each({collection_expr})
                        WHERE NOT ({criteria})
                    )
                )
                ELSE ({criteria})
            END
        )"""
    
    def generate_converts_to_check(self, input_expr: str, target_type: str) -> str:
        """DuckDB check if value can be converted to target type."""
        if target_type == 'boolean':
            # Check if value can be converted to boolean
            return f"""(
                {input_expr} IS NOT NULL AND 
                LOWER({input_expr}::text) IN ('true', 'false', '1', '0', 't', 'f', 'yes', 'no')
            )"""
        elif target_type in ('integer', 'decimal'):
            # Check if value is numeric
            return f"TRY_CAST({input_expr} AS {target_type.upper()}) IS NOT NULL"
        elif target_type in ('date', 'datetime', 'time'):
            # Check if value can be converted to temporal type
            cast_type = 'TIMESTAMP' if target_type == 'datetime' else target_type.upper()
            return f"TRY_CAST({input_expr} AS {cast_type}) IS NOT NULL"
        else:
            # Default: check if not null (can convert to string)
            return f"{input_expr} IS NOT NULL"

    def generate_type_cast(self, input_expr: str, target_type: str) -> str:
        """DuckDB cast value to target type."""
        if target_type in ('integer', 'decimal', 'boolean', 'date', 'time'):
            cast_type = 'TIMESTAMP' if target_type == 'datetime' else target_type.upper()
            return f"TRY_CAST({input_expr} AS {cast_type})"
        else:
            return f"CAST({input_expr} AS STRING)"

    def generate_of_type_filter(self, collection_expr: str, target_type: str) -> str:
        """DuckDB filter collection by type."""
        return f"""(
            SELECT json_group_array(value)
            FROM json_each({collection_expr})
            WHERE json_typeof(value) = '{target_type}'
        )"""

    def generate_equivalent_check(self, left_expr: str, right_expr: str) -> str:
        """DuckDB equivalent (~) operator with type-aware comparison."""
        return f"""(
            CASE 
                -- Try to compare as numbers first
                WHEN TRY_CAST({left_expr} AS DECIMAL) IS NOT NULL 
                     AND TRY_CAST({right_expr} AS DECIMAL) IS NOT NULL THEN
                    CAST({left_expr} AS DECIMAL) = CAST({right_expr} AS DECIMAL)
                -- Otherwise, compare as strings
                ELSE {left_expr} = {right_expr}
            END
        )"""

    def generate_age_in_years(self, birthdate_expr: str) -> str:
        """DuckDB calculate age in years from birth date."""
        return f"CAST(EXTRACT(year FROM AGE(CURRENT_DATE, CAST({birthdate_expr} AS DATE))) AS INTEGER)"

    def generate_age_in_years_at(self, birthdate_expr: str, as_of_date_expr: str) -> str:
        """DuckDB calculate age in years at specific date."""
        return f"CAST(EXTRACT(year FROM AGE(CAST({as_of_date_expr} AS DATE), CAST({birthdate_expr} AS DATE))) AS INTEGER)"

    def generate_resource_query(self, input_state: 'SQLState', resource_type: str) -> str:
        """DuckDB generate SQL for resource retrieval."""
        base_table = input_state.base_table or "fhir_resources"
        json_column = input_state.json_column or "resource"
        
        return f"""
        (
            SELECT {json_column}
            FROM {base_table}
            WHERE json_extract_string({json_column}, '$.resourceType') = '{resource_type}'
        )
        """

    def generate_max_operation(self, operands: list) -> str:
        """DuckDB max() function with multiple operands."""
        if len(operands) == 2:
            return f"CASE WHEN CAST({operands[0]} AS DECIMAL) > CAST({operands[1]} AS DECIMAL) THEN CAST({operands[0]} AS DECIMAL) ELSE CAST({operands[1]} AS DECIMAL) END"
        else:
            # Use GREATEST function (DuckDB supports it too)
            operands_str = ', '.join(f"CAST({op} AS DECIMAL)" for op in operands)
            return f"GREATEST({operands_str})"

    def generate_min_operation(self, operands: list) -> str:
        """DuckDB min() function with multiple operands."""
        if len(operands) == 2:
            return f"CASE WHEN CAST({operands[0]} AS DECIMAL) < CAST({operands[1]} AS DECIMAL) THEN CAST({operands[0]} AS DECIMAL) ELSE CAST({operands[1]} AS DECIMAL) END"
        else:
            # Use LEAST function (DuckDB supports it too)
            operands_str = ', '.join(f"CAST({op} AS DECIMAL)" for op in operands)
            return f"LEAST({operands_str})"

    def generate_median_operation(self, operands: list) -> str:
        """DuckDB median() function."""
        operands_str = ', '.join(f"CAST({op} AS DOUBLE)" for op in operands)
        return f"MEDIAN({operands_str})"

    def generate_population_stddev(self, operands: list) -> str:
        """DuckDB population standard deviation."""
        operands_str = ', '.join(f"CAST({op} AS DOUBLE)" for op in operands)
        return f"STDDEV_POP({operands_str})"

    def generate_population_variance(self, operands: list) -> str:
        """DuckDB population variance."""
        operands_str = ', '.join(f"CAST({op} AS DOUBLE)" for op in operands)
        return f"VAR_POP({operands_str})"

    def generate_datetime_creation(self, year: str, month: str, day: str, hour: str, minute: str, second: str) -> str:
        """DuckDB datetime creation."""
        return f"make_timestamp({year}, {month}, {day}, {hour}, {minute}, {second})"

    def generate_union_operation(self, first_collection: str, second_collection: str) -> str:
        """DuckDB union of two collections."""
        return f"""(
            CASE 
                WHEN {first_collection} IS NULL AND {second_collection} IS NULL THEN NULL
                WHEN {first_collection} IS NULL THEN {second_collection}
                WHEN {second_collection} IS NULL THEN {first_collection}
                WHEN json_type({first_collection}) = 'ARRAY' AND json_type({second_collection}) = 'ARRAY' THEN (
                    SELECT json_group_array(value)
                    FROM (
                        SELECT value FROM json_each({first_collection})
                        UNION
                        SELECT value FROM json_each({second_collection})
                    )
                )
                ELSE json_array({first_collection}, {second_collection})
            END
        )"""

    def generate_exists_check(self, fragment: str, is_collection: bool) -> str:
        """DuckDB exists/empty checks."""
        if is_collection:
            return f"(json_array_length({fragment}) > 0)"
        else:
            return f"({fragment} IS NOT NULL)"

    def generate_join_operation(self, collection_expr: str, separator: str) -> str:
        """DuckDB join operation (concatenate array elements)."""
        return f"""(
            CASE 
                WHEN {collection_expr} IS NULL THEN ''
                WHEN json_type({collection_expr}) = 'ARRAY' THEN (
                    SELECT COALESCE(string_agg(
                        CASE 
                            WHEN json_type(value) = 'ARRAY' THEN (
                                SELECT string_agg(json_extract_string(nested_item.value, '$'), {separator})
                                FROM json_each(value) AS nested_item
                            )
                            ELSE json_extract_string(value, '$')
                        END, {separator}), '')
                    FROM json_each({collection_expr}) AS item
                    WHERE item.value IS NOT NULL
                )
                ELSE COALESCE(json_extract_string({collection_expr}, '$'), '')
            END
        )"""

    def generate_percentile_calculation(self, expression: str, percentile: float) -> str:
        """Generate SQL for percentile calculation using DuckDB syntax."""
        return f"QUANTILE_CONT({expression}, {percentile})"

    def generate_date_difference_years(self, start_date: str, end_date: str = "CURRENT_DATE") -> str:
        """Generate SQL for date difference in years using DuckDB syntax."""
        return f"CAST(({end_date} - DATE({start_date})) / 365.25 AS DOUBLE)"

    def generate_nested_array_aggregation(self, json_col: str, array_field: str, nested_field: str, separator: str) -> str:
        """Generate SQL for nested array aggregation using DuckDB syntax."""
        # Get dialect-specific column names for array iteration
        array_value_col, array_key_col = self.get_array_iteration_columns()
        nested_value_col, nested_key_col = self.get_array_iteration_columns()
        
        # Build DuckDB-specific nested array SQL
        nested_extract = self.extract_json_field(f'{nested_field}_item.{nested_value_col}', '$')
        array_iterate = self.iterate_json_array(json_col, f'$.{array_field}')
        nested_iterate = self.iterate_json_array(f'{array_field}_item.{array_value_col}', f'$.{nested_field}')
        
        return f"""(
            SELECT COALESCE({self.string_agg_function}({nested_extract}, '{separator}'), '')
            FROM {array_iterate} AS {array_field}_item,
                 {nested_iterate} AS {nested_field}_item
        )"""

    def generate_date_difference_with_unit(self, start_date: str, end_date: str, unit: str) -> str:
        """Generate SQL for date difference with specific unit using DuckDB syntax."""
        unit_lower = unit.lower()
        if unit_lower == 'months':
            return f"DATEDIFF('month', {start_date}, {end_date})"
        elif unit_lower == 'years':
            return f"DATEDIFF('year', {start_date}, {end_date})"
        elif unit_lower == 'days':
            return f"DATEDIFF('day', {start_date}, {end_date})"
        else:
            # Default to days for unknown units
            return f"DATEDIFF('day', {start_date}, {end_date})"

    def generate_age_calculation(self, birth_date: str, reference_date: str = "CURRENT_DATE") -> str:
        """Generate SQL for age calculation using DuckDB syntax (returns integer years)."""
        return f"CAST(EXTRACT(YEAR FROM {reference_date}) - EXTRACT(YEAR FROM {birth_date}) AS INTEGER)"

    def generate_intersect_operation(self, first_collection: str, second_collection: str) -> str:
        """Generate SQL for collection intersection using DuckDB syntax."""
        return f"""(
            SELECT json_group_array(DISTINCT value)
            FROM (
                SELECT value FROM json_each({first_collection}) 
                WHERE value IN (SELECT value FROM json_each({second_collection}))
            )
        )"""

    def generate_collection_distinct_check(self, collection_expr: str) -> str:
        """Generate SQL to check if all elements in collection are distinct using DuckDB."""
        return f"""(
            CASE
                WHEN json_array_length({collection_expr}) = 0 THEN TRUE
                WHEN json_array_length({collection_expr}) = 1 THEN TRUE
                ELSE (
                    SELECT COUNT(*) = COUNT(DISTINCT value)
                    FROM json_each({collection_expr})
                )
            END
        )"""

    def normalize_terminology_system(self, system_expr: str) -> str:
        """
        Generate DuckDB SQL to normalize terminology system identifiers for comparison.

        This handles canonical system URI crosswalking at execution time,
        using the terminology_system_mappings table for efficient lookups.
        """
        return f"""COALESCE(
            (SELECT canonical_system
             FROM terminology_system_mappings
             WHERE original_system = {system_expr}),
            {system_expr}
        )"""

    def generate_valueset_match_condition(self, valueset_id: str) -> str:
        """Generate DuckDB SQL condition to match resource codes against ValueSet expansion.

        Uses execution-time system normalization to handle OID/URI crosswalking
        without modifying stored data.
        """
        return f"""EXISTS (
            SELECT 1 FROM fhir_resources vs
            WHERE json_extract_string(vs.resource, '$.resourceType') = 'ValueSet'
            AND json_extract_string(vs.resource, '$.id') = '{valueset_id}'
            AND EXISTS (
                SELECT 1 FROM json_each(json_extract(vs.resource, '$.expansion.contains')) AS vs_code,
                             json_each(json_extract(resource, '$.code.coding')) AS resource_coding
                WHERE json_extract_string(vs_code.value, '$.code') = json_extract_string(resource_coding.value, '$.code')
                AND {self.normalize_terminology_system("json_extract_string(vs_code.value, '$.system')")} =
                    {self.normalize_terminology_system("json_extract_string(resource_coding.value, '$.system')")}
            )
        )"""

    # CQL Function Dialect Abstraction Methods - DuckDB Implementation

    def generate_math_function(self, function_name: str, *args: str) -> str:
        """DuckDB-specific mathematical function SQL generation."""
        func_name = function_name.lower()

        if func_name == 'power':
            return f"POW({', '.join(args)})"
        elif func_name in ['sqrt', 'ln', 'exp', 'log']:
            return f"{func_name.upper()}({', '.join(args)})"
        elif func_name == 'log10':
            return f"LOG10({', '.join(args)})"
        elif func_name == 'ceiling':
            return f"CEIL({', '.join(args)})"
        elif func_name == 'floor':
            return f"FLOOR({', '.join(args)})"
        elif func_name == 'round':
            return f"ROUND({', '.join(args)})"
        elif func_name == 'abs':
            return f"ABS({', '.join(args)})"
        else:
            return f"{func_name.upper()}({', '.join(args)})"

    def generate_date_diff(self, unit: str, start_date: str, end_date: str) -> str:
        """DuckDB date difference using DATE_DIFF function."""
        return f"DATE_DIFF('{unit}', {start_date}, {end_date})"

    def generate_current_timestamp(self) -> str:
        """DuckDB current timestamp."""
        return "CURRENT_TIMESTAMP"

    def generate_current_date(self) -> str:
        """DuckDB current date."""
        return "CURRENT_DATE"

    def generate_regex_match(self, text_expr: str, pattern: str) -> str:
        """DuckDB regex matching using REGEXP."""
        return f"{text_expr} REGEXP '{pattern}'"

    def generate_json_array_elements(self, json_expr: str) -> str:
        """DuckDB JSON array elements extraction using json_each."""
        return f"""(
            SELECT json_array_agg(value)
            FROM json_each({json_expr})
        )"""

    def generate_standard_type_cast(self, expression: str, target_type: str) -> str:
        """DuckDB type casting with proper type names."""
        type_mapping = {
            'double_precision': 'DOUBLE',
            'double': 'DOUBLE',
            'bigint': 'BIGINT',
            'integer': 'INTEGER',
            'int': 'INTEGER',
            'boolean': 'BOOLEAN',
            'varchar': 'VARCHAR',
            'text': 'VARCHAR'
        }
        db_type = type_mapping.get(target_type.lower(), target_type.upper())
        return f"CAST({expression} AS {db_type})"

    def generate_aggregate_function(self, function_name: str, expression: str,
                                  filter_condition: str = None, distinct: bool = False) -> str:
        """DuckDB aggregate function SQL generation."""
        func_name = function_name.upper()

        # Handle DuckDB-specific function name mappings
        func_map = {
            'VARIANCE': 'VAR_SAMP'
        }
        actual_func = func_map.get(func_name, func_name)

        # Handle DISTINCT
        expr = f"DISTINCT {expression}" if distinct else expression

        # Generate function call
        if actual_func in ['STDDEV', 'VAR_SAMP', 'AVG', 'SUM', 'COUNT', 'MIN', 'MAX']:
            sql = f"{actual_func}({expr})"
        else:
            sql = f"{actual_func}({expr})"

        # Add filter condition if provided
        if filter_condition:
            sql = f"{sql} FILTER (WHERE {filter_condition})"

        return sql

    def generate_interval_arithmetic(self, date_expr: str, interval_expr: str, operation: str = 'add') -> str:
        """DuckDB interval arithmetic using INTERVAL syntax."""
        if operation.lower() == 'add':
            return f"({date_expr} + INTERVAL {interval_expr})"
        elif operation.lower() == 'subtract':
            return f"({date_expr} - INTERVAL {interval_expr})"
        else:
            raise ValueError(f"Unsupported interval operation: {operation}")

    def generate_boolean_conversion(self, expression: str) -> str:
        """DuckDB boolean conversion handling."""
        return f"""CASE
            WHEN LOWER(TRIM({expression})) IN ('true', 't', '1') THEN true
            WHEN LOWER(TRIM({expression})) IN ('false', 'f', '0') THEN false
            WHEN {expression} IS NULL OR TRIM({expression}) = '' THEN NULL
            ELSE NULL
        END"""

    def generate_json_aggregate_function(self, function_name: str, json_expr: str,
                                        cast_type: str = None) -> str:
        """DuckDB aggregate function on JSON array elements."""
        cast_type = cast_type or 'DOUBLE'

        # Handle DuckDB-specific function names
        func_map = {
            'variance': 'VAR_SAMP',
            'stddev': 'STDDEV'
        }
        actual_func = func_map.get(function_name.lower(), function_name.upper())

        return f"""(
    SELECT {actual_func}(CAST(value AS {cast_type}))
    FROM (
        SELECT json_extract(json_array_elements({json_expr}), '$') AS value
    ) subq
    WHERE value IS NOT NULL AND value != 'null'
    AND value REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
)"""

    def generate_percentile_function(self, json_expr: str, percentile_fraction: str,
                                   cast_type: str = None) -> str:
        """DuckDB percentile function on JSON array."""
        cast_type = cast_type or 'DOUBLE'

        return f"""(
    SELECT QUANTILE_CONT(CAST(value AS {cast_type}), {percentile_fraction})
    FROM (
        SELECT json_extract(json_array_elements({json_expr}), '$') AS value
    ) subq
    WHERE value IS NOT NULL AND value != 'null'
    AND value REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
)"""

    def generate_json_array_elements(self, json_expr: str) -> str:
        """DuckDB JSON array elements extraction."""
        return f"json_each({json_expr}) AS elem"

    def generate_json_object_creation(self, key_value_pairs: List[str]) -> str:
        """DuckDB JSON object creation."""
        pairs = []
        for i in range(0, len(key_value_pairs), 2):
            key = key_value_pairs[i]
            value = key_value_pairs[i + 1] if i + 1 < len(key_value_pairs) else 'NULL'
            pairs.append(f"{key}: {value}")
        return f"{{{', '.join(pairs)}}}"

    def generate_json_array_creation(self, elements: List[str]) -> str:
        """DuckDB JSON array creation."""
        return f"[{', '.join(elements)}]"

    def generate_json_object_extraction(self, json_expr: str, path: str) -> str:
        """DuckDB JSON object field extraction."""
        return f"json_extract({json_expr}, '{path}')"

    def generate_json_array_aggregation(self, expr: str) -> str:
        """DuckDB JSON array aggregation."""
        return f"json_group_array({expr})"