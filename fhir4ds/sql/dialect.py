from abc import ABC, abstractmethod
from typing import Union

class SQLDialect(ABC):
    """
    Abstract base class for SQL dialects.

    This class defines an interface for generating database-specific SQL syntax.
    By subclassing it, the engine can support different database backends
    without changing the core CTE generation logic.
    """

    @abstractmethod
    def json_extract(self, column: str, path: str) -> str:
        """
        Returns the database-specific syntax for extracting a value from a
        JSON object.

        Args:
            column: The name of the column containing the JSON data.
            path: The JSONPath expression to extract.
        """
        pass


class DuckDBDialect(SQLDialect):
    """
    SQL dialect for DuckDB.
    """

    def json_extract(self, column: str, path: str) -> str:
        """
        Implements JSON extraction for DuckDB using its `json_extract_string`
        function for performance.
        """
        # DuckDB's json_extract_string is often faster for string results
        return f"json_extract_string({column}, '{path}')"


# A registry of available dialects
_DIALECTS = {
    "duckdb": DuckDBDialect,
}

def get_dialect(dialect: Union[str, SQLDialect]) -> SQLDialect:
    """
    Factory function to get a dialect instance.

    Args:
        dialect: Either a string name of a dialect (e.g., "duckdb") or
                 a SQLDialect instance.

    Returns:
        An instance of the requested SQLDialect.

    Raises:
        ValueError: If the dialect is an unknown string.
    """
    if isinstance(dialect, SQLDialect):
        return dialect
    if isinstance(dialect, str):
        if dialect in _DIALECTS:
            return _DIALECTS[dialect]()
        else:
            raise ValueError(
                f"Unknown dialect: '{dialect}'. "
                f"Available dialects are: {', '.join(_DIALECTS.keys())}"
            )
    raise TypeError(
        f"Dialect must be a string or SQLDialect instance, not {type(dialect).__name__}"
    )