"""
Quick Database Connection Module

Provides simplified, one-line database initialization for FHIR4DS.
Automatically handles table creation, connection management, and dialect detection.
"""

import os
import logging
from pathlib import Path
from typing import Optional, Union, Dict, Any, List
from urllib.parse import urlparse

from .core import FHIRDataStore
from ..view_runner import ViewRunner
from .formatters import ResultFormatter
from .batch import BatchProcessor


class QuickConnect:
    """
    Simplified database connection factory for FHIR4DS.
    
    Provides one-line database initialization with automatic:
    - Table creation for FHIR resources
    - Connection string parsing and validation  
    - Dialect detection and configuration
    - Sample data loading (optional)
    
    Examples:
        >>> # Local DuckDB database
        >>> db = QuickConnect.duckdb("./my_fhir_data.db")
        
        >>> # PostgreSQL database
        >>> db = QuickConnect.postgresql("postgresql://user:pass@host:5432/db")
        
        >>> # Auto-detect from connection string
        >>> db = QuickConnect.auto("./local.db")
        >>> db = QuickConnect.auto("postgresql://...")
        
        >>> # In-memory database for testing
        >>> db = QuickConnect.memory()
    """
    
    @staticmethod
    def duckdb(database_path: Union[str, Path], initialize_table: bool = True) -> 'ConnectedDatabase':
        """
        Create a DuckDB connection with automatic FHIR table initialization.
        
        Args:
            database_path: Path to DuckDB database file (will be created if doesn't exist)
            initialize_table: Whether to automatically create FHIR resources table
            
        Returns:
            ConnectedDatabase: Ready-to-use database connection
            
        Example:
            >>> db = QuickConnect.duckdb("./healthcare_data.db")
            >>> # Database file created, FHIR table initialized, ready to use
        """
        database_path = str(database_path)
        
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(database_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
        
        # Create datastore
        datastore = FHIRDataStore.with_duckdb(
            database=database_path,
            initialize_table=initialize_table
        )
        
        return ConnectedDatabase(datastore, connection_type="DuckDB", database_path=database_path)
    
    @staticmethod
    def postgresql(connection_string: str, initialize_table: bool = True) -> 'ConnectedDatabase':
        """
        Create a PostgreSQL connection with automatic FHIR table initialization.
        
        Args:
            connection_string: PostgreSQL connection string
            initialize_table: Whether to automatically create FHIR resources table
            
        Returns:
            ConnectedDatabase: Ready-to-use database connection
            
        Example:
            >>> db = QuickConnect.postgresql("postgresql://user:pass@localhost:5432/fhir_db")
            >>> # Connected to PostgreSQL, FHIR table initialized, ready to use
        """
        # Validate connection string
        if not connection_string.startswith(('postgresql://', 'postgres://')):
            raise ValueError("PostgreSQL connection string must start with 'postgresql://' or 'postgres://'")
        
        # Create datastore
        datastore = FHIRDataStore.with_postgresql(
            conn_str=connection_string,
            initialize_table=initialize_table
        )
        
        return ConnectedDatabase(datastore, connection_type="PostgreSQL", connection_string=connection_string)
    
    @staticmethod
    def auto(connection_string_or_path: str, initialize_table: bool = True) -> 'ConnectedDatabase':
        """
        Auto-detect database type from connection string or file path.
        
        Args:
            connection_string_or_path: Database connection string or file path
            initialize_table: Whether to automatically create FHIR resources table
            
        Returns:
            ConnectedDatabase: Ready-to-use database connection
            
        Example:
            >>> # Auto-detects DuckDB from file extension
            >>> db = QuickConnect.auto("./data.db")
            
            >>> # Auto-detects PostgreSQL from connection string  
            >>> db = QuickConnect.auto("postgresql://user:pass@host:5432/db")
        """
        connection_str = str(connection_string_or_path)
        
        # Check if it's a PostgreSQL connection string
        if connection_str.startswith(('postgresql://', 'postgres://')):
            return QuickConnect.postgresql(connection_str, initialize_table)
        
        # Check if it's a file path (assume DuckDB)
        elif ('/' in connection_str or '\\' in connection_str or 
              connection_str.endswith(('.db', '.duckdb', '.sqlite'))):
            return QuickConnect.duckdb(connection_str, initialize_table)
        
        # Check if it's just a filename
        elif '.' in connection_str and not '://' in connection_str:
            return QuickConnect.duckdb(connection_str, initialize_table)
        
        else:
            raise ValueError(f"Cannot auto-detect database type from: {connection_str}")
    
    @staticmethod
    def memory(initialize_table: bool = True) -> 'ConnectedDatabase':
        """
        Create an in-memory DuckDB database for testing and experimentation.
        
        Args:
            initialize_table: Whether to automatically create FHIR resources table
            
        Returns:
            ConnectedDatabase: Ready-to-use in-memory database
            
        Example:
            >>> db = QuickConnect.memory()
            >>> # In-memory database created, perfect for testing
        """
        datastore = FHIRDataStore.with_duckdb(
            database=":memory:",
            initialize_table=initialize_table
        )
        
        return ConnectedDatabase(datastore, connection_type="DuckDB (Memory)", database_path=":memory:")
    
    @staticmethod
    def from_env(env_var: str = "DATABASE_URL", initialize_table: bool = True) -> 'ConnectedDatabase':
        """
        Create database connection from environment variable.
        
        Args:
            env_var: Environment variable name containing connection string
            initialize_table: Whether to automatically create FHIR resources table
            
        Returns:
            ConnectedDatabase: Ready-to-use database connection
            
        Example:
            >>> # Uses DATABASE_URL environment variable
            >>> db = QuickConnect.from_env()
            
            >>> # Uses custom environment variable
            >>> db = QuickConnect.from_env("FHIR_DATABASE_URL")
        """
        connection_string = os.getenv(env_var)
        if not connection_string:
            raise ValueError(f"Environment variable '{env_var}' not found")
        
        return QuickConnect.auto(connection_string, initialize_table)


class ConnectedDatabase:
    """
    A connected database ready for FHIR analytics operations.
    
    Provides simplified methods for common FHIR data operations including:
    - ViewDefinition execution
    - Result export to various formats
    - Batch processing
    - Resource loading
    """
    
    def __init__(self, datastore: FHIRDataStore, connection_type: str, **connection_info):
        """
        Initialize connected database wrapper.
        
        Args:
            datastore: Initialized FHIRDataStore instance
            connection_type: Human-readable connection type description
            **connection_info: Additional connection information for display
        """
        self.datastore = datastore
        self.connection_type = connection_type
        self.connection_info = connection_info
        
        # Create view runner
        self.view_runner = ViewRunner(datastore=datastore)
        
        # Track usage statistics
        self._queries_executed = 0
        self._resources_loaded = 0
        
        logging.info(f"Connected to {connection_type} database successfully")
    
    def __repr__(self) -> str:
        """String representation of connected database."""
        return f"ConnectedDatabase(type={self.connection_type}, queries={self._queries_executed})"
    
    def info(self) -> Dict[str, Any]:
        """
        Get information about the connected database.
        
        Returns:
            Dict containing connection information and statistics
        """
        return {
            'connection_type': self.connection_type,
            'dialect': self.datastore.dialect.name,
            'queries_executed': self._queries_executed,
            'resources_loaded': self._resources_loaded,
            'table_initialized': hasattr(self.datastore, 'connection'),
            **self.connection_info
        }
    
    def execute(self, view_definition: Dict[str, Any]) -> Any:
        """
        Execute a ViewDefinition and return results.
        
        Args:
            view_definition: FHIR ViewDefinition resource as dictionary
            
        Returns:
            Query results (format depends on database type)
        """
        self._queries_executed += 1
        return self.view_runner.execute_view_definition(view_definition)
    
    def load_resource(self, resource: Dict[str, Any]) -> None:
        """
        Load a FHIR resource into the database.
        
        Args:
            resource: FHIR resource as dictionary
        """
        self._resources_loaded += 1
        self.datastore.load_resource(resource)
    
    def load_resources(self, resources: list, parallel: bool = True, batch_size: int = 100) -> None:
        """
        Load multiple FHIR resources into the database with optimization.
        
        Args:
            resources: List of FHIR resources as dictionaries
            parallel: Whether to use parallel processing for loading
            batch_size: Number of resources to process in each batch
        """
        if not resources:
            return
            
        # Use optimized bulk loading for large datasets
        if len(resources) >= batch_size and hasattr(self.datastore, 'bulk_load_resources'):
            self.datastore.bulk_load_resources(resources, parallel=parallel, batch_size=batch_size)
            self._resources_loaded += len(resources)
        else:
            # Fallback to individual inserts with optional parallelization
            if parallel and len(resources) > 10:
                self._load_resources_parallel(resources, batch_size)
            else:
                for resource in resources:
                    self.load_resource(resource)
    
    def _load_resources_parallel(self, resources: list, batch_size: int) -> None:
        """Load resources in parallel batches."""
        from concurrent.futures import ThreadPoolExecutor
        import math
        
        # Split resources into batches
        num_batches = math.ceil(len(resources) / batch_size)
        batches = [resources[i*batch_size:(i+1)*batch_size] for i in range(num_batches)]
        
        def load_batch(batch):
            for resource in batch:
                self.load_resource(resource)
            return len(batch)
        
        # Load batches in parallel
        with ThreadPoolExecutor(max_workers=min(4, num_batches)) as executor:
            futures = [executor.submit(load_batch, batch) for batch in batches]
            total_loaded = sum(future.result() for future in futures)
        
        logging.info(f"Parallel loading completed: {total_loaded} resources in {num_batches} batches")
    
    def load_from_json_file(self, file_path: str, use_native_json: bool = True) -> int:
        """
        Load FHIR resources from a JSON file with database-specific optimizations.
        
        Args:
            file_path: Path to JSON file containing FHIR resources
            use_native_json: Whether to use database-native JSON loading (DuckDB read_json)
            
        Returns:
            Number of resources loaded
            
        Example:
            >>> count = db.load_from_json_file("./fhir_bundle.json")
            >>> print(f"Loaded {count} resources from file")
        """
        initial_counts = self.datastore.get_resource_counts()
        initial_total = sum(initial_counts.values())
        self.datastore.load_from_json_file(file_path, use_native_json)
        final_counts = self.datastore.get_resource_counts()
        final_total = sum(final_counts.values())
        count = final_total - initial_total
        self._resources_loaded += count
        return count
    
    def test_connection(self) -> bool:
        """
        Test if the database connection is working.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            # Try a simple query to test connection
            test_sql = "SELECT 1 as test_connection"
            if hasattr(self.datastore, 'execute_query'):
                result = self.datastore.execute_query(test_sql)
                return result is not None
            return True
        except Exception as e:
            logging.error(f"Connection test failed: {e}")
            return False
    
    def get_resource_count(self) -> int:
        """
        Get the number of FHIR resources in the database.
        
        Returns:
            Number of resources in the fhir_resources table
        """
        try:
            # Use the working get_resource_counts() method from datastore
            resource_counts = self.datastore.get_resource_counts()
            return sum(resource_counts.values())
        except Exception:
            return 0
    
    # Enhanced methods with formatters integration
    def execute_to_dataframe(self, view_definition: Dict[str, Any], include_metadata: bool = True):
        """
        Execute ViewDefinition and return results as pandas DataFrame.
        
        Args:
            view_definition: FHIR ViewDefinition resource as dictionary
            include_metadata: Whether to include query metadata as DataFrame attributes
            
        Returns:
            pandas.DataFrame with FHIR-aware formatting
            
        Example:
            >>> df = db.execute_to_dataframe(view_definition)
            >>> print(df.head())
        """
        result = self.execute(view_definition)
        return ResultFormatter.to_dataframe(result, include_metadata=include_metadata)
    
    def execute_to_json(self, view_definition: Dict[str, Any], output_path: Optional[str] = None,
                       include_metadata: bool = True, indent: int = 2):
        """
        Execute ViewDefinition and export results to JSON.
        
        Args:
            view_definition: FHIR ViewDefinition resource as dictionary
            output_path: Optional file path to save JSON. If None, returns JSON string.
            include_metadata: Whether to include query metadata
            indent: JSON indentation level
            
        Returns:
            JSON string if output_path is None, otherwise None
        """
        result = self.execute(view_definition)
        return ResultFormatter.to_json(result, output_path, include_metadata, indent)
    
    def execute_to_csv(self, view_definition: Dict[str, Any], output_path: Optional[str] = None,
                      delimiter: str = ',', include_header: bool = True, include_metadata: bool = False):
        """
        Execute ViewDefinition and export results to CSV.
        
        Args:
            view_definition: FHIR ViewDefinition resource as dictionary
            output_path: File path to save CSV (optional - returns string if None)
            delimiter: CSV delimiter character
            include_header: Whether to include column headers
            include_metadata: Whether to add metadata as comments
            
        Returns:
            If output_path is None, returns CSV as string. Otherwise saves to file.
        """
        result = self.execute(view_definition)
        return ResultFormatter.to_csv(result, output_path, delimiter, include_header, include_metadata)
    
    def execute_to_excel(self, view_definitions: Union[Dict[str, Any], List[Dict[str, Any]]], 
                        output_path: str, sheet_names: Optional[List[str]] = None,
                        include_metadata: bool = True):
        """
        Execute ViewDefinition(s) and export results to Excel with multiple sheets.
        
        Args:
            view_definitions: Single ViewDefinition or list of ViewDefinitions
            output_path: File path to save Excel file
            sheet_names: Optional list of sheet names
            include_metadata: Whether to include metadata sheet
            
        Example:
            >>> # Single sheet
            >>> db.execute_to_excel(view_definition, "output.xlsx")
            
            >>> # Multiple sheets
            >>> db.execute_to_excel([view1, view2], "report.xlsx", 
            ...                    sheet_names=["Patients", "Observations"])
        """
        # Ensure view_definitions is a list
        if not isinstance(view_definitions, list):
            view_definitions = [view_definitions]
        
        # Execute all ViewDefinitions
        results = []
        for view_def in view_definitions:
            result = self.execute(view_def)
            results.append(result)
        
        ResultFormatter.to_excel(results, output_path, sheet_names, include_metadata)
    
    def execute_to_parquet(self, view_definition: Dict[str, Any], output_path: str,
                          compression: str = 'snappy', include_metadata: bool = True):
        """
        Execute ViewDefinition and export results to Parquet format.
        
        Args:
            view_definition: FHIR ViewDefinition resource as dictionary
            output_path: File path to save Parquet file
            compression: Compression algorithm ('snappy', 'gzip', 'brotli', 'lz4')
            include_metadata: Whether to include query metadata
        """
        result = self.execute(view_definition)
        ResultFormatter.to_parquet(result, output_path, compression, include_metadata)
    
    # Batch processing methods
    def execute_batch(self, view_definitions: List[Dict[str, Any]], 
                     parallel: bool = True, max_workers: int = 4, 
                     show_progress: bool = True):
        """
        Execute multiple ViewDefinitions in batch with parallel processing.
        
        Args:
            view_definitions: List of FHIR ViewDefinition dictionaries
            parallel: Whether to execute in parallel (True) or sequentially (False)
            max_workers: Maximum number of parallel workers
            show_progress: Whether to show progress indicators
            
        Returns:
            List of BatchResult objects with execution results
            
        Example:
            >>> results = db.execute_batch([view1, view2, view3], parallel=True)
            >>> successful = [r for r in results if r.success]
            >>> print(f"Executed {len(successful)} queries successfully")
        """
        processor = BatchProcessor(self, max_workers=max_workers, show_progress=show_progress)
        return processor.execute_batch(view_definitions, parallel=parallel)
    
    def execute_and_export_batch(self, view_definitions: List[Dict[str, Any]], 
                                output_path: str, format: str = "excel",
                                sheet_names: Optional[List[str]] = None,
                                parallel: bool = True, max_workers: int = 4,
                                show_progress: bool = True):
        """
        Execute batch and immediately export results to specified format.
        
        Args:
            view_definitions: List of FHIR ViewDefinition dictionaries
            output_path: Path for output file
            format: Output format ('excel', 'json', 'csv')
            sheet_names: Optional sheet names for Excel export
            parallel: Whether to execute in parallel
            max_workers: Maximum number of parallel workers
            show_progress: Whether to show progress indicators
            
        Returns:
            BatchSummary with execution statistics
            
        Example:
            >>> summary = db.execute_and_export_batch(
            ...     [view1, view2, view3], "report.xlsx", 
            ...     format="excel", parallel=True
            ... )
            >>> print(f"Success rate: {summary.success_rate:.1f}%")
        """
        processor = BatchProcessor(self, max_workers=max_workers, show_progress=show_progress)
        return processor.execute_and_export_batch(
            view_definitions, output_path, format, sheet_names, parallel
        )
    
    def create_batch_processor(self, max_workers: int = 4, show_progress: bool = True) -> BatchProcessor:
        """
        Create a BatchProcessor instance for advanced batch operations.
        
        Args:
            max_workers: Maximum number of parallel workers
            show_progress: Whether to show progress indicators
            
        Returns:
            Configured BatchProcessor instance
            
        Example:
            >>> processor = db.create_batch_processor(max_workers=8)
            >>> results = processor.execute_batch(view_definitions)
            >>> stats = processor.get_processor_stats()
        """
        return BatchProcessor(self, max_workers=max_workers, show_progress=show_progress)
    
    # Database object creation methods
    def create_view(self, view_definition: Dict[str, Any], view_name: str, 
                   schema_name: Optional[str] = None, materialized: bool = False,
                   replace_if_exists: bool = True) -> str:
        """
        Create a database view or materialized view from a FHIR ViewDefinition.
        
        Args:
            view_definition: FHIR ViewDefinition resource as dictionary
            view_name: Name for the database view
            schema_name: Optional schema name (defaults to default schema)
            materialized: Whether to create a materialized view (if supported)
            replace_if_exists: Whether to replace view if it already exists
            
        Returns:
            The SQL statement that was executed
            
        Example:
            >>> sql = db.create_view(patient_view, "patient_demographics", 
            ...                     schema_name="analytics", materialized=True)
            >>> print(f"Created view with SQL: {sql}")
        """
        # Generate the SELECT SQL from ViewDefinition
        result = self.view_runner.execute_view_definition(view_definition)
        select_sql = result.sql.strip()
        
        # Remove any trailing semicolon
        if select_sql.endswith(';'):
            select_sql = select_sql[:-1]
        
        # Build view creation SQL
        full_view_name = f"{schema_name}.{view_name}" if schema_name else view_name
        
        # Determine view type and creation syntax
        if materialized and self.datastore.dialect.name == "POSTGRESQL":
            # PostgreSQL materialized view
            create_type = "MATERIALIZED VIEW"
        elif materialized and self.datastore.dialect.name == "DUCKDB":
            # DuckDB doesn't have materialized views, create table instead
            return self.create_table(view_definition, view_name, schema_name, replace_if_exists)
        else:
            # Regular view
            create_type = "VIEW"
        
        # Build CREATE statement
        replace_clause = "OR REPLACE " if replace_if_exists else ""
        create_sql = f"CREATE {replace_clause}{create_type} {full_view_name} AS {select_sql}"
        
        # Execute the CREATE statement
        if hasattr(self.datastore, 'execute_query'):
            self.datastore.execute_query(create_sql)
        elif hasattr(self.datastore.dialect, 'connection'):
            self.datastore.dialect.connection.execute(create_sql)
        else:
            raise AttributeError("Cannot access database connection for executing CREATE statements")
        
        logging.info(f"Created {create_type.lower()} '{full_view_name}' successfully")
        return create_sql
    
    def create_table(self, view_definition: Dict[str, Any], table_name: str,
                    schema_name: Optional[str] = None, replace_if_exists: bool = True) -> str:
        """
        Create a database table from a FHIR ViewDefinition.
        
        Args:
            view_definition: FHIR ViewDefinition resource as dictionary
            table_name: Name for the database table
            schema_name: Optional schema name (defaults to default schema)
            replace_if_exists: Whether to replace table if it already exists
            
        Returns:
            The SQL statement that was executed
            
        Example:
            >>> sql = db.create_table(patient_view, "patient_demographics_table",
            ...                      schema_name="warehouse")
            >>> print(f"Created table with SQL: {sql}")
        """
        # Generate the SELECT SQL from ViewDefinition
        result = self.view_runner.execute_view_definition(view_definition)
        select_sql = result.sql.strip()
        
        # Remove any trailing semicolon
        if select_sql.endswith(';'):
            select_sql = select_sql[:-1]
        
        # Build table creation SQL
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        
        # Handle table replacement
        if replace_if_exists:
            drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
            if hasattr(self.datastore, 'execute_query'):
                self.datastore.execute_query(drop_sql)
            elif hasattr(self.datastore.dialect, 'connection'):
                self.datastore.dialect.connection.execute(drop_sql)
            else:
                logging.warning("Cannot access database connection for DROP statement")
        
        # Create table with data
        create_sql = f"CREATE TABLE {full_table_name} AS {select_sql}"
        
        # Execute the CREATE statement
        if hasattr(self.datastore, 'execute_query'):
            self.datastore.execute_query(create_sql)
        elif hasattr(self.datastore.dialect, 'connection'):
            self.datastore.dialect.connection.execute(create_sql)
        else:
            raise AttributeError("Cannot access database connection for executing CREATE statements")
        
        logging.info(f"Created table '{full_table_name}' successfully")
        return create_sql
    
    def create_schema(self, schema_name: str, if_not_exists: bool = True) -> str:
        """
        Create a database schema.
        
        Args:
            schema_name: Name of the schema to create
            if_not_exists: Whether to use IF NOT EXISTS clause
            
        Returns:
            The SQL statement that was executed
            
        Example:
            >>> sql = db.create_schema("analytics")
            >>> print(f"Created schema with SQL: {sql}")
        """
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        create_sql = f"CREATE SCHEMA {if_not_exists_clause}{schema_name}"
        
        # Execute the CREATE statement
        if hasattr(self.datastore, 'execute_query'):
            self.datastore.execute_query(create_sql)
        elif hasattr(self.datastore.dialect, 'connection'):
            self.datastore.dialect.connection.execute(create_sql)
        else:
            raise AttributeError("Cannot access database connection for executing CREATE statements")
        
        logging.info(f"Created schema '{schema_name}' successfully")
        return create_sql
    
    def list_tables(self, schema_name: Optional[str] = None) -> List[str]:
        """
        List all tables in the database or specific schema.
        
        Args:
            schema_name: Optional schema name to filter by
            
        Returns:
            List of table names
            
        Example:
            >>> tables = db.list_tables("analytics")
            >>> print(f"Tables in analytics schema: {tables}")
        """
        if self.datastore.dialect.name == "POSTGRESQL":
            if schema_name:
                sql = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
                result = self.datastore.dialect.connection.execute(sql, (schema_name,)).fetchall()
            else:
                sql = """
                SELECT schemaname || '.' || tablename as full_name
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                ORDER BY schemaname, tablename
                """
                result = self.datastore.dialect.connection.execute(sql).fetchall()
        else:  # DuckDB
            if schema_name:
                sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
            else:
                sql = "SHOW TABLES"
            result = self.datastore.dialect.connection.execute(sql).fetchall()
        
        return [row[0] for row in result] if result else []
    
    def list_views(self, schema_name: Optional[str] = None) -> List[str]:
        """
        List all views in the database or specific schema.
        
        Args:
            schema_name: Optional schema name to filter by
            
        Returns:
            List of view names
            
        Example:
            >>> views = db.list_views("analytics") 
            >>> print(f"Views in analytics schema: {views}")
        """
        if self.datastore.dialect.name == "POSTGRESQL":
            if schema_name:
                sql = """
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = %s
                ORDER BY table_name
                """
                result = self.datastore.dialect.connection.execute(sql, (schema_name,)).fetchall()
            else:
                sql = """
                SELECT schemaname || '.' || viewname as full_name
                FROM pg_views 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                ORDER BY schemaname, viewname
                """
                result = self.datastore.dialect.connection.execute(sql).fetchall()
        else:  # DuckDB
            if schema_name:
                sql = f"SELECT table_name FROM information_schema.views WHERE table_schema = '{schema_name}'"
            else:
                sql = "SELECT table_name FROM information_schema.views"
            result = self.datastore.dialect.connection.execute(sql).fetchall()
        
        return [row[0] for row in result] if result else []