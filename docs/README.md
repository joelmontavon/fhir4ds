# FHIR4DS Documentation

Welcome to the complete documentation for **FHIR4DS** (FHIR for Data Science) - a production-ready healthcare analytics platform providing 100% SQL-on-FHIR v2.0 compliance and 100% FHIRPath specification coverage.

## Documentation Structure

### Getting Started
- **[API Reference](API.md)** - Complete API documentation for all modules
- **[Examples Overview](EXAMPLES.md)** - Usage examples and patterns
- **[Interactive Notebooks](./examples/notebooks/)** - Hands-on tutorials and examples

### User Guides
- **[Installation Guide](#installation)** - Setup instructions for different environments  
- **[Quick Start Tutorial](#quick-start)** - 5-minute introduction to FHIR4DS
- **[Database Setup](#database-setup)** - DuckDB vs PostgreSQL configuration
- **[Server Deployment](#server-deployment)** - Running the analytics API server

### Technical Reference
- **[FHIRPath Guide](#fhirpath-guide)** - Supported FHIRPath expressions and functions
- **[ViewDefinition Format](#viewdefinition-format)** - Complete ViewDefinition specification
- **[Docker Deployment](#docker-deployment)** - Container deployment options and configuration
- **[Performance Tuning](#performance-tuning)** - Optimization strategies and best practices
- **[Error Handling](#error-handling)** - Common issues and troubleshooting

### Development
- **[Testing Guide](#testing)** - Running tests and validation
- **[Contributing](#contributing)** - Development workflow and guidelines
- **[Compliance](#compliance)** - SQL-on-FHIR v2.0 specification compliance

---

## Installation

### pip/pip3
```bash
pip install fhir4ds
```

### uv (recommended)
```bash
uv add fhir4ds
```

### Development Installation
```bash
git clone https://github.com/fhir4ds/fhir4ds.git
cd fhir4ds
uv install --dev
```

---

## Quick Start

### 1. One-Line Database Setup
```python
from fhir4ds.datastore import QuickConnect

# DuckDB (file-based)
db = QuickConnect.duckdb("./healthcare.db")

# PostgreSQL  
db = QuickConnect.postgresql("postgresql://user:pass@localhost:5432/fhir")
```

### 2. Load FHIR Resources
```python
# High-performance parallel loading
db.load_resources(fhir_resources, parallel=True)

# Bulk loading from JSON files
db.load_from_json_file("bundle.json", use_native_json=True)
```

### 3. Execute Analytics
```python
patient_demographics = {
    "resource": "Patient",
    "select": [{
        "column": [
            {"name": "id", "path": "id", "type": "id"},
            {"name": "family", "path": "name.family", "type": "string"},
            {"name": "gender", "path": "gender", "type": "string"}
        ]
    }],
    "where": [{"path": "active = true"}]
}

# Execute and get results
df = db.execute_to_dataframe(patient_demographics)
print(df.head())
```

### 4. Multi-Format Export
```python
# Export to various formats
db.execute_to_excel([patient_view, observation_view], "report.xlsx")
db.execute_to_csv(analytics_view)
json_data = db.execute_to_json(summary_view)
```

---

## Database Setup

### DuckDB (Analytics Optimized)
```python
# In-memory (fastest for development)
db = QuickConnect.duckdb(":memory:")

# File-based (persistent storage)
db = QuickConnect.duckdb("./analytics.db")

# Custom configuration
import duckdb
conn = duckdb.connect("./data.db")
conn.execute("PRAGMA memory_limit='4GB'")
db = QuickConnect.from_connection(conn)
```

**Best for:**
- Development and testing
- Analytics workloads
- Fast JSON processing
- Single-user scenarios

### PostgreSQL (Production Ready)
```python
# Standard connection
db = QuickConnect.postgresql("postgresql://user:pass@localhost:5432/fhir")

# With connection pooling
db = QuickConnect.postgresql(
    "postgresql://user:pass@localhost:5432/fhir?pool_size=10&max_overflow=20"
)
```

**Best for:**
- Production deployments
- Multi-user environments
- Large-scale data storage
- Advanced indexing needs

---

## Server Deployment

### Basic Server Startup
```bash
# Default DuckDB server
python -m fhir4ds.server

# Access API documentation at: http://localhost:8000/docs
```

### Production Configuration
```bash
# PostgreSQL with custom settings
python -m fhir4ds.server \
    --host 0.0.0.0 \
    --port 8080 \
    --database-type postgresql \
    --database-url "postgresql://fhir:password@db-server:5432/healthcare" \
    --workers 4 \
    --max-resources 5000
```

### Programmatic Server
```python
from fhir4ds.server import FHIRAnalyticsServer, ServerConfig

config = ServerConfig(
    host="0.0.0.0",
    port=8000,
    database_type="postgresql",
    database_url="postgresql://fhir:pass@localhost:5432/healthcare",
    max_resources_per_request=5000
)

server = FHIRAnalyticsServer(config)
await server.startup()
```

### Environment Variables
```bash
export FHIR4DS_HOST="0.0.0.0"
export FHIR4DS_PORT="8080"
export FHIR4DS_DATABASE_TYPE="postgresql"
export FHIR4DS_DATABASE_URL="postgresql://user:pass@localhost:5432/fhir"
export FHIR4DS_MAX_RESOURCES="5000"

python -m fhir4ds.server
```

---

## Docker Deployment

### Quick Start
```bash
# DuckDB (lightweight, recommended for development)
docker-compose --profile duckdb up --build

# PostgreSQL (production-grade)  
docker-compose --profile postgresql up --build

# Development mode with hot reload
docker-compose --profile dev up --build
```

### Interactive Setup
```bash
# Use the interactive setup script
./examples/docker-start.sh
```

### Docker Features
- Multi-stage builds for optimized image size
- Multiple profiles: DuckDB, PostgreSQL, and development
- Persistent data storage with Docker volumes
- Health checks and monitoring
- Production-ready with non-root user

**Access**: http://localhost:8000 (API docs at http://localhost:8000/docs)

For complete Docker documentation, see the main [DOCKER.md](../DOCKER.md) guide.

---

## FHIRPath Guide

### Basic Path Traversal
```
id                          # Resource ID
name.family                 # Nested property access
name[0].family             # Array indexing
active                     # Boolean properties
```

### Collection Operations
```
name.family                # Single value (collection: false)
name.family                # All values (collection: true)
telecom.where(system='email').value  # Filtered collections
name.exists()              # Existence checks
```

### Supported Functions

FHIR4DS provides **100% FHIRPath specification coverage** (91/91 functions implemented):

#### Collection Functions
```
first()                    # First element of collection
last()                     # Last element of collection
where(condition)           # Filter collections
exists()                   # Existence checks
empty()                    # Empty collection checks
count()                    # Count elements
distinct()                 # Remove duplicates
all()                      # All elements match condition
allTrue()                  # All elements are true
allFalse()                 # All elements are false
anyTrue()                  # Any element is true
anyFalse()                 # Any element is false
subsetOf()                 # Set operations
supersetOf()               # Set operations
intersect()                # Set operations
exclude()                  # Set operations
union()                    # Set operations
combine()                  # Combine collections
single()                   # Single element validation
skip(n)                    # Skip n elements
take(n)                    # Take n elements
tail()                     # All except first
```

#### String Functions
```
toString()                 # Convert to string
substring()                # Extract substring
indexOf()                  # Find index
startsWith()               # String starts with
endsWith()                 # String ends with
contains()                 # String contains
upper()                    # Convert to uppercase
lower()                    # Convert to lowercase
replace()                  # Replace substring
matches()                  # Regex matching
replaceMatches()           # Regex replacement
length()                   # String length
toChars()                  # Convert to character array
join()                     # Join with separator
```

#### Math Functions
```
toInteger()                # Convert to integer
toDecimal()                # Convert to decimal
abs()                      # Absolute value
ceiling()                  # Round up
floor()                    # Round down
round()                    # Round to nearest
sqrt()                     # Square root
power()                    # Power operation
exp()                      # Exponential
ln()                       # Natural logarithm
log()                      # Logarithm
truncate()                 # Truncate decimal
```

#### Date/Time Functions
```
now()                      # Current timestamp
today()                    # Current date
timeOfDay()                # Current time
toDate()                   # Convert to date
toDateTime()               # Convert to datetime
toTime()                   # Convert to time
```

#### Type Functions
```
toBoolean()                # Convert to boolean
toQuantity()               # Convert to quantity
convertsToBoolean()        # Test boolean conversion
convertsToInteger()        # Test integer conversion
convertsToDecimal()        # Test decimal conversion
convertsToDate()           # Test date conversion
convertsToDateTime()       # Test datetime conversion
convertsToTime()           # Test time conversion
convertsTo(type)           # Generic type conversion test
```

#### FHIR-Specific Functions
```
extension(url)             # FHIR extension extraction
getResourceKey()           # Resource ID extraction
getReferenceKey()          # Reference ID extraction
getValue()                 # Extract primitive values
resolve()                  # Resolve references
hasValue()                 # Check for value
ofType(type)               # Type filtering
conformsTo()               # Conformance checking
memberOf()                 # Terminology membership
```

#### Terminology Functions
```
subsumes(code)             # Terminology subsumption
subsumedBy(code)           # Reverse subsumption
comparable(quantity)       # Quantity comparison
```

#### Advanced Functions
```
elementDefinition()        # Element definition access
slice(structure, name)     # Profile slice access
checkModifiers(modifier)   # Modifier extension check
```

### Logical Operations
```
active = true              # Equality comparison
age > 18                   # Numeric comparison
name.exists() and active   # Logical AND
gender = 'male' or gender = 'female'  # Logical OR
not(deceased.exists())     # Logical NOT
```

---

## Clinical Quality Language (CQL) Support

FHIR4DS includes comprehensive **Clinical Quality Language (CQL)** support with **80-85% language compliance**, enabling sophisticated clinical quality measure development and population health analytics.

### CQL Engine Usage

```python
from fhir4ds.cql.core.engine import CQLEngine

# Initialize CQL engine  
engine = CQLEngine(dialect="duckdb", initial_context="Population")

# Basic CQL expressions
sql = engine.evaluate_expression('[Patient] P where P.active = true')

# Advanced CQL constructs (Phase 6)
advanced_cql = '''
[Patient] P
  with [Condition: "Diabetes"] D such that D.subject references P
  without [Encounter: "Emergency"] E such that E.subject references P
'''
result_sql = engine.evaluate_expression(advanced_cql)
```

### CQL Functions Coverage (82 Total)

#### Mathematical Functions (17 implemented)
- **Arithmetic**: `+`, `-`, `*`, `/`, `%`
- **Functions**: `Abs()`, `Max()`, `Min()`, `Round()`, `Sqrt()`, `Power()`, `Ln()`, `Log()`
- **Aggregates**: `Sum()`, `Avg()`, `Count()`, `Min()`, `Max()`

#### DateTime Functions (36 implemented)
- **Component extraction**: `year from`, `month from`, `day from`, `hour from`
- **Date arithmetic**: `+ days`, `+ months`, `+ years`, `years between`, `months between`
- **Constructors**: `DateTime()`, `Date()`, `Time()`
- **Comparisons**: `same as`, `same or before`, `same or after`

#### Interval Functions (21 implemented - Allen's Algebra)
- **Relationships**: `overlaps`, `contains`, `in`, `includes`, `meets`, `starts`, `ends`
- **Operations**: `union`, `intersect`, `except`, `width`, `size`
- **Temporal logic**: Complete support for temporal relationship analysis

#### Nullological Functions (8 implemented - Three-valued Logic)
- **Null handling**: `Coalesce()`, `IsNull()`, `IsTrue()`, `IsFalse()`
- **Boolean logic**: Proper three-valued logic implementation

### Advanced CQL Constructs (Phase 6)

#### with/without Clauses
```python
# Complex relationship queries with EXISTS/NOT EXISTS SQL generation
diabetes_with_hba1c = '''
[Condition: "Diabetes mellitus"] Diabetes
  with [Observation: "HbA1c laboratory test"] HbA1c
    such that HbA1c.subject references Diabetes.subject
      and HbA1c.effective during "Measurement Period"
      and HbA1c.value as Quantity > 9.0 '%'
'''

# Exclusion logic
patients_without_insulin = '''
[Patient] P
  without [MedicationRequest: "Insulin"] Insulin
    such that Insulin.subject references P
'''
```

#### let Expressions  
```python
# Variable definitions with CTE-based SQL generation
population_query = '''
let measurementPeriod: Interval[@2023-01-01T00:00:00.000, @2023-12-31T23:59:59.999],
    diabetesValueSet: "Diabetes mellitus"
[Patient] P
  with [Condition: diabetesValueSet] D
    such that D.subject references P
      and D.recordedDate during measurementPeriod
'''
```

#### Multi-Resource Queries
```python
# Complex clinical scenarios across multiple FHIR resources
comprehensive_diabetes_care = '''
[Patient] P
  with [Condition: "Diabetes mellitus"] DM
    such that DM.subject references P
  with [Observation: "HbA1c laboratory test"] A1C
    such that A1C.subject references P
      and A1C.effective during "Measurement Period"
  with [MedicationRequest: "Diabetes medications"] DM_Meds
    such that DM_Meds.subject references P
      and DM_Meds.authoredOn during "Measurement Period"
  without [Encounter: "Emergency department visit"] ED
    such that ED.subject references P
      and ED.period during "Measurement Period"
'''
```

### Clinical Use Cases

#### Quality Measure Development
```python
# CMS/HEDIS measure implementation
cms_diabetes_measure = '''
define "Numerator":
  [Patient] P
    with [Condition: "Diabetes mellitus"] DM
      such that DM.subject references P
    with [Observation: "HbA1c laboratory test"] A1C
      such that A1C.subject references P
        and A1C.effective during "Measurement Period"
        and A1C.value as Quantity < 7.0 '%'
'''
```

#### Population Health Analytics
```python
# Risk stratification across multiple conditions
cardiovascular_risk = '''
let riskPeriod: Interval[@2023-01-01T00:00:00.000, @2023-12-31T23:59:59.999]
[Patient] P
  with [Condition: "Hypertension"] HTN such that HTN.subject references P
  with [Condition: "Hyperlipidemia"] HLD such that HLD.subject references P
  with [Observation: "Blood pressure"] BP
    such that BP.subject references P and BP.effective during riskPeriod
  without [Procedure: "Cardiac intervention"] CARD
    such that CARD.subject references P and CARD.performed during riskPeriod
'''
```

### Performance Characteristics
- **Sub-millisecond response times** for advanced CQL constructs
- **Cross-dialect compatibility** (DuckDB/PostgreSQL)
- **Production-ready** with comprehensive caching
- **Scalable** for complex clinical scenarios

### Terminology Integration
- **VSAC integration** with production caching
- **Database-integrated** terminology operations
- **Multi-tier caching** for 10-100x performance improvement

---

## ViewDefinition Format

### Basic Structure
```json
{
    "name": "patient_summary",
    "resource": "Patient",
    "description": "Patient demographics summary",
    "select": [
        {
            "column": [
                {
                    "name": "patient_id",
                    "path": "id",
                    "type": "id"
                },
                {
                    "name": "family_name",
                    "path": "name.family",
                    "type": "string"
                }
            ]
        }
    ],
    "where": [
        {
            "path": "active = true",
            "description": "Only active patients"
        }
    ]
}
```

### Advanced Features
```json
{
    "select": [
        {
            "forEach": "contact",
            "column": [
                {"name": "contact_name", "path": "name.family", "type": "string"}
            ]
        },
        {
            "unionAll": [
                {"column": [{"name": "type", "path": "'primary'", "type": "string"}]},
                {"column": [{"name": "type", "path": "'secondary'", "type": "string"}]}
            ]
        }
    ]
}
```

---

## Performance Tuning

### Resource Loading Optimization
```python
# Use parallel processing for large datasets
db.load_resources(resources, parallel=True)

# Batch size optimization for parallel loading
db.load_resources(resources, parallel=True, batch_size=500)

# Use DuckDB's native JSON reader for files
db.load_from_json_file("large_bundle.json", use_native_json=True)
```

### ViewDefinition Optimization
```python
# Use specific resource filtering
{
    "where": [{"path": "resourceType = 'Patient'"}]  # Filter early
}

# Avoid deep nesting when possible
{
    "column": [
        {"name": "simple_path", "path": "gender", "type": "string"},  # Good
        {"name": "complex_path", "path": "contact.name.family.first()", "type": "string"}  # Slower
    ]
}

# Use collection: false when appropriate
{
    "column": [
        {"name": "family", "path": "name.family", "type": "string", "collection": false}  # Single value
    ]
}
```

### Database Optimization
```python
# Create materialized views for frequently-used analytics
db.create_view(complex_analytics, "patient_summary", materialized=True)

# Use database-specific indexing
# PostgreSQL
db.execute_sql("CREATE INDEX idx_resource_type ON fhir_resources USING GIN ((resource->>'resourceType'))")

# DuckDB  
db.execute_sql("CREATE INDEX idx_id ON fhir_resources (json_extract_string(resource, '$.id'))")
```

---

## Error Handling

### Common Issues and Solutions

#### Collection Constraint Violations
```python
# Problem: Multiple values returned for collection: false
try:
    result = db.execute_to_dataframe(view_def)
except ValueError as e:
    if "Collection value" in str(e):
        # Solution: Set collection: true or use first()
        view_def["select"][0]["column"][0]["collection"] = True
```

#### FHIRPath Syntax Errors
```python
from fhir4ds.fhirpath import validate_fhirpath

# Validate before execution
fhirpath_expr = "name.family.first()"
if not validate_fhirpath(fhirpath_expr):
    print(f"Invalid FHIRPath: {fhirpath_expr}")
```

#### Database Connection Issues
```python
try:
    db = QuickConnect.postgresql("postgresql://invalid:connection@localhost:5432/db")
except Exception as e:
    print(f"Connection failed: {e}")
    # Fallback to DuckDB
    db = QuickConnect.duckdb(":memory:")
```

---

## Testing

### Run Official Tests
```bash
# SQL-on-FHIR v2.0 compliance tests (117 tests)
uv run python tests/run_tests.py --test-dir tests/official

# Expected: 117/117 tests passing
```

### Dual Dialect Testing
```bash
# Test both DuckDB and PostgreSQL compatibility
uv run python test_dual_dialect_coverage.py

# Expected: 100% compatibility for both databases
```

### Integration Testing
```bash
# Test dialect integration and SQL translation
uv run python tests/run_tests_for_all_dialects.py
```

---

## Compliance

FHIR4DS provides comprehensive healthcare analytics standards compliance:

### SQL-on-FHIR v2.0 Compliance
- ✅ **117/117 official tests passing** (100% compliance)
- ✅ **100% FHIRPath specification coverage** (91/91 functions implemented)
- ✅ **Dual database compatibility** (DuckDB + PostgreSQL)
- ✅ **Production-ready performance** with enterprise features

### Clinical Quality Language (CQL) Compliance  
- ✅ **80-85% CQL language compliance** (82 functions + advanced constructs)
- ✅ **Mathematical Functions**: 17/17 implemented (95%+ compliance)
- ✅ **DateTime Functions**: 36 implemented (80%+ compliance, up from 30%)
- ✅ **Interval Functions**: 21 implemented (80%+ compliance, up from 13%)
- ✅ **Advanced Constructs**: with/without clauses, let expressions, multi-resource queries
- ✅ **Production-ready terminology integration** with VSAC caching
- ✅ **Clinical use case validation** for quality measures and population health

### Supported SQL-on-FHIR Features
- All ViewDefinition structures (select, column, forEach, unionAll)
- Complete WHERE clause support with boolean expressions
- Constants and parameterization
- Collection handling with proper constraint validation
- Reference resolution functions
- Extension processing
- Type system compliance

---

## Additional Resources

### Interactive Learning
- **[Jupyter Notebooks](./examples/notebooks/)** - Interactive tutorials and walkthroughs
- **[🧪 CQL Walkthrough](./examples/notebooks/cql_walkthrough.ipynb)** - Experimental CQL functionality tutorial
- **[Historical Examples](../archive/examples/)** - Historical ViewDefinition examples

### Example ViewDefinitions
- **[Archive Examples](../archive/examples/)** - Ready-to-use ViewDefinitions for:
  - OMOP CDM mappings (condition, observation, person)
  - Patient demographics
  - Simple field extractions

### Community and Support
- **GitHub Repository:** [fhir4ds/fhir4ds](https://github.com/fhir4ds/fhir4ds)
- **Issues and Feature Requests:** GitHub Issues
- **Discussions:** GitHub Discussions

---

**Ready to transform FHIR data into actionable healthcare insights!** 🏥📊