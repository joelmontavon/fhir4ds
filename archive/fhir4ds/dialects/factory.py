"""
Database dialect factory for creating dialect instances.
"""

from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .base import DatabaseDialect

def create_dialect(dialect_spec: Union[str, "DatabaseDialect"]) -> "DatabaseDialect":
    """
    Create a dialect instance from a dialect specification.

    Args:
        dialect_spec: Either a string dialect name or an existing dialect instance

    Returns:
        DatabaseDialect instance

    Raises:
        ValueError: If dialect_spec is not supported
    """
    if hasattr(dialect_spec, 'name'):
        # Already a dialect instance
        return dialect_spec

    if isinstance(dialect_spec, str):
        dialect_name = dialect_spec.lower()

        if dialect_name == "postgresql":
            from .postgresql import PostgreSQLDialect
            from ..config import get_database_url
            conn_str = get_database_url('postgresql')
            return PostgreSQLDialect(conn_str)
        elif dialect_name in ("duckdb", ""):
            from .duckdb import DuckDBDialect
            return DuckDBDialect()
        else:
            raise ValueError(f"Unsupported dialect: {dialect_spec}")

    raise ValueError(f"Invalid dialect specification: {dialect_spec}")