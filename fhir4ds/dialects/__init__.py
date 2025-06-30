"""
FHIR4DS Database Dialects Package

This package provides database-specific implementations for FHIR data storage
and querying. Each dialect optimizes operations for its target database.

Available Dialects:
- DuckDBDialect: Optimized for DuckDB with excellent JSON support
- PostgreSQLDialect: Optimized for PostgreSQL with JSONB operations

Usage:
    from fhir4ds.dialects import DuckDBDialect, PostgreSQLDialect
    
    # DuckDB (default, in-memory)
    duckdb_dialect = DuckDBDialect()
    
    # PostgreSQL
    postgres_dialect = PostgreSQLDialect("postgresql://user:pass@localhost/dbname")
"""

from .base import DatabaseDialect
from .duckdb import DuckDBDialect
from .postgresql import PostgreSQLDialect

__all__ = [
    "DatabaseDialect",
    "DuckDBDialect", 
    "PostgreSQLDialect"
]