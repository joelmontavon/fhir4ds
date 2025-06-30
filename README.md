# FHIR for Data Science (FHIR4DS)

Production-ready healthcare analytics platform providing 100% SQL-on-FHIR v2.0 compliance with dual database support (DuckDB + PostgreSQL).

**Current Version**: 0.4.0 (Advanced Architecture & Performance Optimizations)  
**Test Compliance**: 100% (117/117 tests passing) ✅ Complete Dual Dialect Compliance  
**Database Support**: DuckDB 100% + PostgreSQL 100% ✅ Production Ready  
**User Experience**: ✅ One-line setup, multi-format export, batch processing

## Key Features

- **100% SQL-on-FHIR v2.0 compliance** - All 117 official tests passing
- **Dual database support** - DuckDB and PostgreSQL with identical functionality
- **Advanced FHIRPath parsing** - Complete parser with 187 choice type mappings
- **CTE-based SQL generation** - 95% reduction in SQL complexity
- **Parallel processing** - High-performance data loading and query execution
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

GNU GLP v3.0

## Related Projects

- [SQL-on-FHIR Specification](https://github.com/FHIR/sql-on-fhir-v2) - Official specification
- [FHIR R4 Specification](https://hl7.org/fhir/R4/) - FHIR standard
- [DuckDB](https://duckdb.org/) - High-performance analytics database
- [PostgreSQL](https://www.postgresql.org/) - Enterprise-grade relational database