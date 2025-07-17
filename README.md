# FHIR for Data Science (FHIR4DS)

Production-ready healthcare analytics platform providing 100% SQL-on-FHIR v2.0 compliance and 100% FHIRPath specification coverage with dual database support (DuckDB + PostgreSQL).

**Current Version**: 0.6.1  
**FHIRPath Coverage**: 100% (91/91 functions implemented)
**SQL-on-FHIR Compliance**: 100% (117/117 tests passing)
**Database Support**: DuckDB 100% + PostgreSQL 100%  

## Key Features

- **100% SQL-on-FHIR v2.0 compliance** - All 117 official tests passing
- **100% FHIRPath specification coverage** - All 91 functions implemented
- **Dual database support** - DuckDB and PostgreSQL with identical functionality
- **Advanced FHIRPath parsing** - Complete parser with 187 choice type mappings
- **High-performance SQL generation** - Optimized for complex healthcare queries
- **Multi-format export** - Pandas, JSON, CSV, Excel, Parquet
- **Database object creation** - Views, tables, schemas from ViewDefinitions

## Quick Start

### Installation
```bash
pip install fhir4ds
```

### Database Setup
```python
from fhir4ds.datastore import QuickConnect

# DuckDB (recommended for analytics)
db = QuickConnect.duckdb("./healthcare_data.db")

# PostgreSQL (enterprise-grade)
db = QuickConnect.postgresql("postgresql://user:pass@host:5432/db")

# Auto-detect from connection string
db = QuickConnect.auto("./local.db")  # → DuckDB
```

### Load Data
```python
# Load FHIR resources with parallel processing
db.load_resources(fhir_resources, parallel=True)

# Load from JSON files (optimized for large files)
db.load_from_json_file("fhir_bundle.json", use_native_json=True)
```

### Execute Queries and Export
```python
# Execute ViewDefinitions with immediate export
df = db.execute_to_dataframe(view_definition)
db.execute_to_excel([query1, query2], "report.xlsx", parallel=True)
db.execute_to_parquet(analytics_query, "dataset.parquet")

# Batch processing with progress monitoring
results = db.execute_batch(queries, parallel=True, show_progress=True)
```

### Create Database Objects
```python
# Create analytics infrastructure
db.create_schema("clinical_analytics")
db.create_view(patient_view, "patient_demographics")
db.create_table(observation_view, "vital_signs_table")

# List created objects
tables = db.list_tables()
views = db.list_views()
```

For more details, please see the [documentation](./docs/README.md).

## Testing

Run the comprehensive test suite:

```bash
# Test both DuckDB and PostgreSQL dialects
python tests/run_tests.py --dialect all

# Test specific dialect
python tests/run_tests.py --dialect duckdb
python tests/run_tests.py --dialect postgresql
```

Test results: 117/117 tests passing on both DuckDB and PostgreSQL.

## License

GNU General Public License v3 (GPLv3)

## Related Projects

- [SQL-on-FHIR Specification](https://github.com/FHIR/sql-on-fhir-v2) - Official specification
- [FHIR R4 Specification](https://hl7.org/fhir/R4/) - FHIR standard
- [DuckDB](https://duckdb.org/) - High-performance analytics database
- [PostgreSQL](https://www.postgresql.org/) - Enterprise-grade relational database