# FHIR4DS API Reference

## Overview

FHIR4DS (FHIR for Data Science) is a production-ready healthcare analytics platform that provides 100% SQL-on-FHIR v2.0 compliance with dual database support (DuckDB + PostgreSQL). This reference covers all public APIs and modules.

## Installation

```bash
pip install fhir4ds
# or with uv
uv add fhir4ds
```

## Quick Start

```python
from fhir4ds.datastore import QuickConnect

# One-line database setup
db = QuickConnect.duckdb("./healthcare.db")

# Load FHIR resources
db.load_resources(fhir_resources, parallel=True)

# Execute analytics
results = db.execute_to_dataframe(view_definition)
```

---

## Core Modules

### fhir4ds.datastore

**Data management interface for healthcare analytics - recommended for most users.**

#### QuickConnect

Factory class for one-line database setup with automatic FHIR table initialization.

```python
from fhir4ds.datastore import QuickConnect

# DuckDB (in-memory)
db = QuickConnect.duckdb(":memory:")

# DuckDB (file-based)
db = QuickConnect.duckdb("./data.db")

# PostgreSQL
db = QuickConnect.postgresql("postgresql://user:pass@localhost:5432/db")
```

**Methods:**
- `QuickConnect.duckdb(database: str) -> ConnectedDatabase`
- `QuickConnect.postgresql(connection_string: str) -> ConnectedDatabase`

#### ConnectedDatabase

High-level interface for FHIR data operations with automatic format handling.

```python
# Resource loading
db.load_resources(resources: List[Dict], parallel: bool = True)
db.load_from_json_file(file_path: str, use_native_json: bool = True)

# ViewDefinition execution
df = db.execute_to_dataframe(view_def: Dict, include_metadata: bool = False)
json_result = db.execute_to_json(view_def: Dict)
csv_result = db.execute_to_csv(view_def: Dict)

# Multi-format export
db.execute_to_excel(view_defs: List[Dict], filename: str, parallel: bool = True)
db.execute_to_parquet(view_def: Dict, filename: str)

# Database object creation
db.create_view(view_def: Dict, view_name: str, schema_name: str = None, materialized: bool = False)
db.create_table(view_def: Dict, table_name: str, schema_name: str = None)
db.create_schema(schema_name: str)

# Database introspection
tables = db.list_tables(schema_name: str = None)
views = db.list_views(schema_name: str = None)
```

**Parameters:**
- `view_def: Dict` - ViewDefinition in JSON format
- `parallel: bool` - Enable parallel processing (default: True)
- `include_metadata: bool` - Include execution metadata in results
- `use_native_json: bool` - Use DuckDB's read_json for better performance

**Returns:**
- `execute_to_dataframe()` → `pandas.DataFrame`
- `execute_to_json()` → `List[Dict]`
- `execute_to_csv()` → `str` (CSV content)

#### ResultFormatter

Multi-format result export with FHIR-aware data type handling.

```python
from fhir4ds.helpers.formatters import ResultFormatter

# Convert SQL results to various formats
df = ResultFormatter.to_dataframe(result, include_metadata=False)
json_data = ResultFormatter.to_json(result)
csv_content = ResultFormatter.to_csv(result)

# File export
ResultFormatter.to_excel(results_list, "report.xlsx", sheet_names=["sheet1"])
ResultFormatter.to_parquet(result, "data.parquet")
```

#### BatchProcessor

High-performance parallel query execution with progress monitoring.

```python
from fhir4ds.helpers.batch import BatchProcessor

processor = BatchProcessor(datastore, max_workers=4, show_progress=True)

# Execute multiple ViewDefinitions in parallel
results = processor.execute_batch([view1, view2, view3])

# Get performance statistics
stats = processor.get_statistics()
```

---

### fhir4ds.datastore

**Core data management for FHIR resources.**

#### FHIRDataStore

Low-level interface for FHIR resource storage and querying.

```python
from fhir4ds import FHIRDataStore, DuckDBDialect

# Create with dialect
dialect = DuckDBDialect(database="./data.db")
datastore = FHIRDataStore(dialect=dialect, initialize_table=True)

# Insert resources
datastore.insert_resources(resources)

# Execute ViewDefinition
result = datastore.execute_view_definition(view_definition)
```

**Constructor:**
```python
FHIRDataStore(
    dialect: DatabaseDialect,
    table_name: str = "fhir_resources",
    json_column: str = "resource", 
    initialize_table: bool = True
)
```

**Methods:**
- `insert_resources(resources: List[Dict]) -> None`
- `execute_view_definition(view_def: Dict) -> FHIRResultSet`
- `get_resource_count() -> int`
- `close() -> None`

#### FHIRResultSet

Enhanced result set with FHIR-specific features and format conversion.

```python
# Get results
rows = result.fetchall()
df = result.to_df(include_metadata=False)

# Metadata
columns = result.get_column_names()
row_count = result.get_row_count()
```

---

### fhir4ds.dialects

**Database-specific implementations for DuckDB and PostgreSQL.**

#### DatabaseDialect (Abstract Base)

Base class defining the database abstraction interface.

```python
from fhir4ds.dialects import DatabaseDialect

# Abstract methods (implemented by concrete dialects)
dialect.get_connection()
dialect.execute_query(sql: str)
dialect.create_table(table_name: str, columns: List[str])
```

#### DuckDBDialect

DuckDB implementation with optimized JSON operations.

```python
from fhir4ds import DuckDBDialect

# In-memory database
dialect = DuckDBDialect(database=":memory:")

# File-based database  
dialect = DuckDBDialect(database="./healthcare.db")

# Custom connection
import duckdb
conn = duckdb.connect("./data.db")
dialect = DuckDBDialect(connection=conn)
```

**Features:**
- Native JSON extension support
- Optimized `json_extract_string()` operations
- High-performance `read_json()` for bulk loading
- Memory-efficient aggregations

#### PostgreSQLDialect

PostgreSQL implementation with JSONB operations.

```python
from fhir4ds import PostgreSQLDialect

dialect = PostgreSQLDialect(
    connection_string="postgresql://user:pass@host:5432/db"
)
```

**Features:**
- JSONB operations (`jsonb_extract_path_text`, `jsonb_each`)
- Advanced indexing support for JSONB columns
- Production-ready transaction handling
- Connection pooling support

**SQL Function Mapping:**
- `json_extract_string` → `jsonb_extract_path_text`
- `json_each` → `jsonb_each`
- `json_extract` → `jsonb_path_query`

---

### fhir4ds.view_runner

**Core SQL-on-FHIR ViewDefinition execution engine.**

#### SQLOnFHIRViewRunner

Main class for executing ViewDefinitions with complete SQL-on-FHIR v2.0 support.

```python
from fhir4ds import SQLOnFHIRViewRunner, FHIRDataStore, DuckDBDialect

# Create with datastore
dialect = DuckDBDialect(database=":memory:")
datastore = FHIRDataStore(dialect=dialect, initialize_table=True)
runner = SQLOnFHIRViewRunner(datastore=datastore)

# Execute ViewDefinition
result = runner.execute_view_definition(view_definition)
```

**Constructor:**
```python
SQLOnFHIRViewRunner(
    datastore: FHIRDataStore,
    enable_extensions: bool = True,
    max_extension_depth: int = 10
)
```

**Methods:**

##### `execute_view_definition(view_def: Dict) -> FHIRResultSet`
Executes a ViewDefinition and returns results.

```python
view_definition = {
    "resource": "Patient", 
    "select": [{
        "column": [
            {"name": "id", "path": "id", "type": "id"},
            {"name": "family", "path": "name.family", "type": "string"}
        ]
    }]
}

result = runner.execute_view_definition(view_definition)
rows = result.fetchall()
```

##### `validate_view_definition(view_def: Dict) -> bool`
Validates ViewDefinition syntax and semantics.

##### `generate_sql(view_def: Dict) -> str`
Generates SQL query without execution.

```python
sql = runner.generate_sql(view_definition)
print(sql)  # See generated SQL
```

---

### fhir4ds.fhirpath

**FHIRPath expression parsing and SQL translation.**

#### FHIRPath

Main FHIRPath parser with advanced expression support.

```python
from fhir4ds.fhirpath import FHIRPath

parser = FHIRPath()

# Parse and validate
ast = parser.parse("Patient.name.family")
is_valid = parser.validate("Patient.name.given.first()")

# Get SQL translation
sql = parser.to_sql("Patient.telecom.where(system='email').value", 
                   resource_context="Patient")
```

#### SimpleFHIRPath

Lightweight parser for basic FHIRPath expressions.

```python
from fhir4ds.fhirpath import SimpleFHIRPath

simple_parser = SimpleFHIRPath()
sql = simple_parser.to_sql("name.family")
```

#### Utility Functions

```python
from fhir4ds.fhirpath import parse_fhirpath, fhirpath_to_sql, validate_fhirpath

# Parse expression
ast = parse_fhirpath("Patient.name.family")

# Convert to SQL
sql = fhirpath_to_sql("name.family", resource_type="Patient")

# Validate syntax
is_valid = validate_fhirpath("telecom.where(system='email').value")
```

---

### fhir4ds.server

**RESTful API server for analytics as a service.**

#### Server Startup

```bash
# Default DuckDB server
python -m fhir4ds.server

# PostgreSQL server
python -m fhir4ds.server --database-type postgresql \
    --database-url "postgresql://user:pass@localhost:5432/fhir"

# Custom configuration
python -m fhir4ds.server --host 0.0.0.0 --port 8080 --workers 4
```

#### FHIRAnalyticsServer

Programmatic server interface.

```python
from fhir4ds.server import FHIRAnalyticsServer, ServerConfig

# Configure server
config = ServerConfig(
    host="0.0.0.0",
    port=8000,
    database_type="duckdb", 
    database_url="./analytics.db",
    max_resources_per_request=1000
)

# Create and start server
server = FHIRAnalyticsServer(config)
await server.startup()
```

#### ServerConfig

Server configuration with environment variable support.

```python
from fhir4ds.server import ServerConfig

config = ServerConfig(
    host="0.0.0.0",                    # FHIR4DS_HOST
    port=8000,                         # FHIR4DS_PORT  
    database_type="duckdb",            # FHIR4DS_DATABASE_TYPE
    database_url="./server.db",        # FHIR4DS_DATABASE_URL
    max_resources_per_request=1000,    # FHIR4DS_MAX_RESOURCES
    enable_parallel_processing=True,   # FHIR4DS_ENABLE_PARALLEL
    batch_size=100                     # FHIR4DS_BATCH_SIZE
)
```

#### API Endpoints

**ViewDefinition Management:**
- `POST /views` - Create ViewDefinition
- `GET /views` - List ViewDefinitions  
- `GET /views/{name}` - Get ViewDefinition
- `DELETE /views/{name}` - Delete ViewDefinition

**Analytics Execution:**
- `POST /views/{name}/execute` - Execute analytics
- `GET /views/{name}/sql` - Get generated SQL

**Resource Management:**
- `POST /resources` - Load FHIR resources
- `GET /resources/count` - Get resource count

**Server Information:**
- `GET /health` - Health check
- `GET /info` - Server information

#### Request/Response Models

```python
from fhir4ds.server.models import AnalyticsRequest, AnalyticsResponse

# Execute analytics
request = AnalyticsRequest(
    resources=[patient1, patient2],
    format="json",  # json, csv, excel, parquet
    include_metadata=False
)

response = AnalyticsResponse(
    data=[...],
    metadata={
        "row_count": 100,
        "execution_time_ms": 45.2,
        "sql_generated": "SELECT ..."
    }
)
```

---

## ViewDefinition Format

### Basic Structure

```python
view_definition = {
    "name": "view_name",              # Required
    "resource": "Patient",            # Required  
    "description": "Description",     # Optional
    "select": [...],                  # Required
    "where": [...],                   # Optional
    "constants": [...]                # Optional
}
```

### Select Structures

```python
{
    "select": [{
        "column": [                   # Column definitions
            {
                "name": "patient_id",
                "path": "id", 
                "type": "id"
            }
        ],
        "select": [...],              # Nested selects
        "unionAll": [...],            # Union operations
        "forEach": "path",            # Iteration
        "forEachOrNull": "path"       # Null-safe iteration
    }]
}
```

### Column Definitions

```python
{
    "name": "column_name",           # Required - SQL column name
    "path": "fhirpath.expression",   # Required - FHIRPath
    "type": "string",                # Optional - FHIR type
    "collection": false,             # Optional - allow multiple values
    "description": "Description"     # Optional
}
```

### WHERE Clauses

```python
{
    "where": [
        {
            "path": "active = true",      # FHIRPath boolean expression
            "description": "Active only"  # Optional
        },
        {
            "path": "gender = 'female'"   # Multiple conditions (AND)
        }
    ]
}
```

### Constants

```python
{
    "constants": [
        {
            "name": "ACTIVE_STATUS",
            "valueString": "active"
        },
        {
            "name": "MAX_AGE", 
            "valueInteger": 65
        }
    ]
}
```

---

## FHIRPath Support

### Supported Operations

#### Path Traversal
```
id                           # Resource ID
name.family                  # Nested field access  
name[0].family              # Array indexing
telecom.where(system='email').value  # Filtered access
```

#### Collection Operations
```
name.family                  # First value (collection: false)
name.family                  # All values (collection: true)  
name.where(use='official')   # Filtered collections
name.exists()               # Boolean existence check
name.empty()                # Boolean empty check
```

#### Functions
```
first()                     # First element
last()                      # Last element (not implemented)
count()                     # Count elements (not implemented)
where(condition)            # Filter collection
extension(url)              # Extract FHIR extension
```

#### Reference Functions
```
getResourceKey()            # Extract resource ID
getReferenceKey()           # Extract reference ID
getReferenceKey(Patient)    # Extract typed reference
```

#### Logical Operations
```
active = true               # Equality
age > 18                    # Comparison  
name.exists() and active    # Logical AND
gender = 'male' or gender = 'female'  # Logical OR
not(deceased.exists())      # Logical NOT
```

#### Advanced Features
```
forEach: "contact"          # Iterate over collection
forEachOrNull: "name"       # Null-safe iteration
unionAll: [select1, select2] # Combine results
```

### Type System

**Primitive Types:**
- `string` - Text values
- `id` - FHIR identifiers  
- `code` - Coded values
- `boolean` - True/false
- `integer` - Whole numbers
- `decimal` - Decimal numbers
- `date` - Date values (YYYY-MM-DD)
- `dateTime` - DateTime with timezone
- `time` - Time values
- `uri`/`url` - URI/URL values

**Complex Types:**
- Arrays are handled with `collection: true`
- Objects are accessed via path traversal
- Extensions use `extension(url)` function

---

## Error Handling

### Common Exceptions

#### ViewDefinition Validation Errors
```python
try:
    result = runner.execute_view_definition(view_def)
except ValueError as e:
    if "ViewDefinition validation failed" in str(e):
        print(f"Invalid ViewDefinition: {e}")
```

#### Collection Constraint Violations
```python
try:
    result = runner.execute_view_definition(view_def)
except ValueError as e:
    if "Collection value" in str(e):
        print("Multiple values returned for collection: false")
        # Fix: Set collection: true or modify FHIRPath
```

#### FHIRPath Syntax Errors
```python
from fhir4ds.fhirpath import validate_fhirpath

if not validate_fhirpath("invalid.path.syntax"):
    print("Invalid FHIRPath expression")
```

#### Database Connection Errors
```python
try:
    db = QuickConnect.postgresql("invalid://connection")
except Exception as e:
    print(f"Database connection failed: {e}")
```

### Best Practices

1. **Validate ViewDefinitions** before execution
2. **Use collection: true** for paths that may return multiple values
3. **Handle null values** with `forEachOrNull` when appropriate
4. **Test FHIRPath expressions** with `validate_fhirpath()`
5. **Use appropriate data types** in column definitions

---

## Performance Optimization

### Database Configuration

#### DuckDB Optimization
```python
# Use file-based database for persistence
db = QuickConnect.duckdb("./analytics.db")

# Enable parallel processing
db.load_resources(resources, parallel=True)

# Use native JSON loading for large files
db.load_from_json_file("large_bundle.json", use_native_json=True)
```

#### PostgreSQL Optimization  
```python
# Use connection pooling
db = QuickConnect.postgresql(
    "postgresql://user:pass@localhost:5432/fhir?pool_size=10"
)

# Create indexes on JSONB columns
db.execute_sql("CREATE INDEX idx_resource_type ON fhir_resources USING GIN ((resource->>'resourceType'))")
```

### ViewDefinition Optimization

1. **Use specific resource types** in WHERE clauses
2. **Avoid deep nesting** in FHIRPath expressions  
3. **Use collection: false** when single values expected
4. **Create materialized views** for frequently-used analytics
5. **Use constants** for repeated values

### Batch Processing

```python
from fhir4ds.helpers.batch import BatchProcessor

# Process multiple ViewDefinitions in parallel
processor = BatchProcessor(datastore, max_workers=4)
results = processor.execute_batch([view1, view2, view3])
```

---

## Testing

### Running Tests

```bash
# Run official SQL-on-FHIR v2.0 tests
uv run python tests/run_tests.py --test-dir tests/official

# Run dual dialect tests  
uv run python test_dual_dialect_coverage.py

# Run dialect integration tests
uv run python tests/run_tests_for_all_dialects.py
```

### Test Coverage

- **117/117 tests passing** for SQL-on-FHIR v2.0 compliance
- **100% dual dialect support** (DuckDB + PostgreSQL)
- **Complete FHIRPath coverage** for supported operations

---

## Examples

### Basic Patient Analytics

```python
from fhir4ds.datastore import QuickConnect

# Setup
db = QuickConnect.duckdb(":memory:")
db.load_resources(patient_resources)

# ViewDefinition
patient_view = {
    "resource": "Patient",
    "select": [{
        "column": [
            {"name": "id", "path": "id", "type": "id"},
            {"name": "family", "path": "name.family", "type": "string"},
            {"name": "gender", "path": "gender", "type": "string"},
            {"name": "active", "path": "active", "type": "boolean"}
        ]
    }],
    "where": [{"path": "active = true"}]
}

# Execute
results = db.execute_to_dataframe(patient_view)
print(results.head())
```

### Complex Observation Analytics

```python
# Multi-format export
observation_view = {
    "resource": "Observation", 
    "select": [{
        "column": [
            {"name": "patient_id", "path": "subject.reference", "type": "string"},
            {"name": "code", "path": "code.coding.code", "type": "string"}, 
            {"name": "value", "path": "valueQuantity.value", "type": "decimal"},
            {"name": "unit", "path": "valueQuantity.unit", "type": "string"},
            {"name": "date", "path": "effectiveDateTime", "type": "dateTime"}
        ]
    }],
    "where": [{"path": "valueQuantity.exists()"}]
}

# Export to multiple formats
db.execute_to_excel([patient_view, observation_view], "healthcare_report.xlsx")
db.execute_to_parquet(observation_view, "observations.parquet")
```

### Server Deployment

```python
from fhir4ds.server import FHIRAnalyticsServer, ServerConfig

# Production configuration
config = ServerConfig(
    host="0.0.0.0",
    port=8000,
    database_type="postgresql", 
    database_url="postgresql://fhir:password@db-server:5432/healthcare",
    max_resources_per_request=5000,
    enable_parallel_processing=True
)

# Start server
server = FHIRAnalyticsServer(config)
await server.startup()
```

---

## Version Information

- **FHIR4DS Version:** 3.0.0
- **SQL-on-FHIR Compliance:** v2.0 (117/117 tests)
- **Python Support:** 3.8+
- **Database Support:** DuckDB 0.8+, PostgreSQL 12+

For complete examples and tutorials, see the [archive examples](../archive/examples/) and [development notebooks](../archive/temp_scripts/).