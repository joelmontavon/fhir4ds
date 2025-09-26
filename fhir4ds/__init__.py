"""
FHIR4DS: A FHIRPath-to-SQL Compiler
"""
from typing import Union

from .engine import FHIRPathEngine
from .sql.dialect import SQLDialect, DuckDBDialect

__all__ = ["compile"]

def compile(
    fhirpath_expression: str,
    dialect: Union[str, SQLDialect] = "duckdb",
) -> str:
    """
    Compiles a FHIRPath expression to a SQL query.

    This is the main entry point for the FHIR4DS library. It orchestrates the
    entire compilation process, from parsing the FHIRPath expression to
    generating the final SQL query in the specified dialect.

    Args:
        fhirpath_expression: The FHIRPath expression to compile.
        dialect: The SQL dialect to target. Can be a string ("duckdb") or
                 an instance of a SQLDialect subclass.

    Returns:
        The compiled SQL query as a string.
    """
    engine = FHIRPathEngine(dialect=dialect)
    return engine.compile(fhirpath_expression)