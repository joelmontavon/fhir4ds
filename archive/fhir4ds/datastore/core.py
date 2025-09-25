"""
FHIR DataStore Core - Centralized Data Management

Core FHIRDataStore class providing unified interface for FHIR data storage,
querying, and management across different database dialects.
"""

from __future__ import annotations

import json
import logging
import os
import glob
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

# Import dialects from the dialects package
from ..dialects import DatabaseDialect, DuckDBDialect, PostgreSQLDialect
from .result import QueryResult

logger = logging.getLogger(__name__)


class FHIRDataStore:
    """
    Unified FHIR data storage and querying interface that abstracts database dialects
    and provides integrated data loading, querying, and result handling.
    """
    
    def __init__(self, dialect: Optional[DatabaseDialect] = None, table_name: str = "fhir_resources", 
                 json_col: str = "resource", initialize_table: bool = True):
        """
        Initialize FHIR data store.
        
        Args:
            dialect: Database dialect implementation. Defaults to DuckDB in-memory.
            table_name: Name of the FHIR resources table
            json_col: Name of the JSON column storing FHIR resources
            initialize_table: Whether to initialize/recreate the table
        """
        # Import here to avoid circular imports
        self.dialect = dialect or DuckDBDialect()
        self.table_name = table_name
        self.json_col = json_col
        self.logger = logger
        
        # Initialize the FHIR table only if requested
        if initialize_table:
            self._initialize_table()
    
    def _initialize_table(self):
        """Initialize the FHIR resources table and terminology mappings table"""
        self.dialect.create_fhir_table(self.table_name, self.json_col)
        self.dialect.create_terminology_system_mappings_table()
        self.logger.info(f"Initialized FHIR data store with {type(self.dialect).__name__}")
    
    def load_from_files(self, file_pattern: str, use_bulk_load: bool = True,
                       clean_patient_data: bool = True, max_file_size_mb: int = 100) -> 'FHIRDataStore':
        """
        Load FHIR data from files matching a pattern.
        
        Args:
            file_pattern: Glob pattern for files (e.g., './data/*.json')
            use_bulk_load: Try bulk loading before falling back to individual inserts
            clean_patient_data: Clean up Patient resources
            max_file_size_mb: Max file size for bulk loading
            
        Returns:
            Self for method chaining
        """
        files = glob.glob(file_pattern)
        if not files:
            raise FileNotFoundError(f"No files found matching pattern: {file_pattern}")
        
        self.logger.info(f"Found {len(files)} files matching pattern: {file_pattern}")
        
        for file_path in files:
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            
            if use_bulk_load and file_size_mb <= max_file_size_mb:
                try:
                    loaded = self.dialect.bulk_load_json(file_path, self.table_name, self.json_col)
                    if loaded > 0:
                        self.logger.info(f"Bulk loaded {loaded} resources from {file_path}")
                        continue
                except Exception as e:
                    self.logger.warning(f"Bulk load failed for {file_path}, falling back: {e}")
            
            # Fallback to individual inserts
            loaded = self._load_file_individually(file_path)
            self.logger.info(f"Individually loaded {loaded} resources from {file_path}")
        
        return self
    
    def load_resource(self, resource: Dict[str, Any]) -> None:
        """Load a single FHIR resource"""
        self.dialect.insert_resource(resource, self.table_name, self.json_col)
    
    def load_resources(self, resources: List[Dict[str, Any]]) -> 'FHIRDataStore':
        """Load multiple FHIR resources and return self for chaining"""
        for resource in resources:
            self.load_resource(resource)
        return self
    
    def insert_resources(self, resources: List[Dict[str, Any]]) -> None:
        """
        Insert FHIR resources into the datastore.
        
        This method provides API compatibility for expected insert_resources() interface
        and delegates to the existing load_resources() implementation.
        
        Args:
            resources: List of FHIR resource dictionaries
        """
        self.load_resources(resources)
    
    def bulk_load_resources(self, resources: List[Dict[str, Any]], 
                           parallel: bool = True, batch_size: int = 100) -> 'FHIRDataStore':
        """
        Efficiently load multiple FHIR resources using database-specific optimizations.
        
        Args:
            resources: List of FHIR resources as dictionaries
            parallel: Whether to use parallel processing
            batch_size: Number of resources per batch
            
        Returns:
            Self for method chaining
        """
        if not resources:
            return self
        
        # Use dialect-specific bulk loading if available
        if hasattr(self.dialect, 'bulk_insert_resources'):
            self.dialect.bulk_insert_resources(
                resources, self.table_name, self.json_col, 
                parallel=parallel, batch_size=batch_size
            )
        else:
            # Fallback to individual inserts with batching
            self._bulk_load_fallback(resources, parallel, batch_size)
        
        return self
    
    def _bulk_load_fallback(self, resources: List[Dict[str, Any]], 
                           parallel: bool = True, batch_size: int = 100) -> int:
        """Fallback bulk loading using individual inserts with batching."""
        if parallel and len(resources) > batch_size:
            from concurrent.futures import ThreadPoolExecutor
            import math
            
            # Split into batches
            num_batches = math.ceil(len(resources) / batch_size)
            batches = [resources[i*batch_size:(i+1)*batch_size] for i in range(num_batches)]
            
            def load_batch(batch):
                count = 0
                for resource in batch:
                    self.load_resource(resource)
                    count += 1
                return count
            
            # Load batches in parallel
            total_loaded = 0
            with ThreadPoolExecutor(max_workers=min(4, num_batches)) as executor:
                futures = [executor.submit(load_batch, batch) for batch in batches]
                total_loaded = sum(future.result() for future in futures)
            
            self.logger.info(f"Bulk loading completed: {total_loaded} resources in {num_batches} batches")
            return total_loaded
        else:
            # Sequential loading
            return self.load_resources(resources)
    
    def load_from_json_file(self, file_path: str, use_native_json: bool = True) -> 'FHIRDataStore':
        """
        Load FHIR resources from a JSON file with database-specific optimizations.
        
        Args:
            file_path: Path to JSON file containing FHIR resources
            use_native_json: Whether to use database-native JSON loading (DuckDB read_json)
            
        Returns:
            Self for method chaining
        """
        file_path = str(file_path)
        
        # Use DuckDB read_json for efficiency if available
        if (use_native_json and 
            self.dialect.name == "DUCKDB" and 
            hasattr(self.dialect, 'load_json_file')):
            
            self.dialect.load_json_file(file_path, self.table_name, self.json_col)
        else:
            # Fallback to individual parsing and loading
            self._load_file_individually(file_path)
        
        return self
    
    def execute_sql(self, sql: str, view_def: Optional[Dict] = None) -> QueryResult:
        """Execute SQL and return enhanced result set"""
        return self.dialect.execute_sql(sql, view_def)
    
    def get_resource_counts(self) -> Dict[str, int]:
        """Get counts of resources by type"""
        return self.dialect.get_resource_counts(self.table_name, self.json_col)

    # ValueSet Caching Methods

    def cache_valueset(self, valueset_name: str, valueset_resource: Dict[str, Any]) -> None:
        """
        Store VSAC-retrieved ValueSet in datastore cache.

        Args:
            valueset_name: Name of the ValueSet (e.g., "Galactosemia")
            valueset_resource: FHIR ValueSet resource with expansion
        """
        # Ensure the resource has proper metadata for caching
        if 'resourceType' not in valueset_resource:
            valueset_resource['resourceType'] = 'ValueSet'

        # Add cache metadata
        valueset_resource['_cache_key'] = f"cached_valueset_{valueset_name}"
        valueset_resource['_cached_at'] = self._get_current_timestamp()

        # Store in datastore
        self.load_resource(valueset_resource)
        self.logger.info(f"Cached ValueSet '{valueset_name}' with {self._count_valueset_codes(valueset_resource)} codes")

    def get_cached_valueset(self, valueset_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached ValueSet resource by name.

        Args:
            valueset_name: Name of the ValueSet to retrieve

        Returns:
            ValueSet resource dict if found, None otherwise
        """
        cache_key = f"cached_valueset_{valueset_name}"

        # Query for cached ValueSet
        sql = f"""
        SELECT {self.json_col}
        FROM {self.table_name}
        WHERE json_extract_string({self.json_col}, '$._cache_key') = '{cache_key}'
        AND json_extract_string({self.json_col}, '$.resourceType') = 'ValueSet'
        """

        try:
            result = self.dialect.execute_query(sql)
            if result and len(result) > 0:
                # Return the first match (should be unique)
                resource_json = result[0][0] if isinstance(result[0], tuple) else result[0]
                if isinstance(resource_json, str):
                    import json
                    return json.loads(resource_json)
                else:
                    return resource_json
        except Exception as e:
            self.logger.warning(f"Failed to retrieve cached ValueSet '{valueset_name}': {e}")

        return None

    def is_valueset_cached(self, valueset_name: str) -> bool:
        """
        Check if ValueSet is available in cache.

        Args:
            valueset_name: Name of the ValueSet to check

        Returns:
            True if ValueSet is cached, False otherwise
        """
        return self.get_cached_valueset(valueset_name) is not None

    def get_cached_valuesets(self) -> List[str]:
        """
        Get list of all cached ValueSet names.

        Returns:
            List of ValueSet names that are cached
        """
        sql = f"""
        SELECT json_extract_string({self.json_col}, '$.name') as name
        FROM {self.table_name}
        WHERE json_extract_string({self.json_col}, '$.resourceType') = 'ValueSet'
        AND json_extract_string({self.json_col}, '$._cache_key') IS NOT NULL
        AND json_extract_string({self.json_col}, '$.name') IS NOT NULL
        """

        try:
            result = self.dialect.execute_query(sql)
            if result:
                return [row[0] if isinstance(row, tuple) else row for row in result]
        except Exception as e:
            self.logger.warning(f"Failed to retrieve cached ValueSet list: {e}")

        return []

    def _get_current_timestamp(self) -> str:
        """Get current timestamp for cache metadata"""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()

    def _count_valueset_codes(self, valueset_resource: Dict[str, Any]) -> int:
        """Count the number of codes in a ValueSet expansion"""
        expansion = valueset_resource.get('expansion', {})
        contains = expansion.get('contains', [])
        return len(contains)
    
    def _load_file_individually(self, file_path: str) -> int:
        """Load file by parsing JSON and inserting resources individually"""
        loaded_count = 0
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                # Array of resources
                for resource in data:
                    if isinstance(resource, dict) and 'resourceType' in resource:
                        self.load_resource(resource)
                        loaded_count += 1
            elif isinstance(data, dict):
                if data.get('resourceType') == 'Bundle':
                    # FHIR Bundle
                    for entry in data.get('entry', []):
                        resource = entry.get('resource', {})
                        if 'resourceType' in resource:
                            self.load_resource(resource)
                            loaded_count += 1
                elif 'resourceType' in data:
                    # Single resource
                    self.load_resource(data)
                    loaded_count += 1
                    
        except (FileNotFoundError, PermissionError, json.JSONDecodeError) as e:
            # Re-raise fundamental errors that should not be silently handled
            self.logger.error(f"Failed to load {file_path}: {e}")
            raise
        except Exception as e:
            # Log other errors but don't raise
            self.logger.error(f"Failed to load {file_path}: {e}")
            
        return loaded_count

    @classmethod
    def with_duckdb(cls, database: str = ":memory:", **kwargs) -> 'FHIRDataStore':
        """Create a FHIRDataStore with DuckDB dialect"""
        dialect = DuckDBDialect(database=database)
        return cls(dialect=dialect, **kwargs)
    
    def view_runner(self) -> 'ViewRunner':
        """Create a ViewRunner instance configured with this datastore for chaining"""
        # Import here to avoid circular imports
        from ..view_runner import ViewRunner
        return ViewRunner(datastore=self)
    
    @classmethod
    def with_postgresql(cls, conn_str: str, **kwargs) -> 'FHIRDataStore':
        """Create a FHIRDataStore with PostgreSQL dialect"""
        dialect = PostgreSQLDialect(conn_str)
        return cls(dialect=dialect, **kwargs)