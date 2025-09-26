from typing import List

from .generator import CTE
from .dialect import SQLDialect

class SQLAssembler:
    """
    Assembles a chain of CTEs into a final, monolithic SQL query.
    """

    def __init__(self, dialect: SQLDialect):
        self.dialect = dialect

    def assemble(self, cte_chain: List[CTE]) -> str:
        """
        Constructs the final SQL query from a list of CTEs.

        Args:
            cte_chain: A list of CTE objects, ordered by dependency.

        Returns:
            A single SQL query string.
        """
        if not cte_chain:
            return ""

        # The WITH clause joins all CTEs
        with_clauses = ",\n".join(
            f"  {cte.name} AS (\n    {cte.sql.replace(chr(10), chr(10) + '    ')}\n  )"
            for cte in cte_chain
        )

        # The final query selects from the last CTE in the chain
        last_cte_name = cte_chain[-1].name
        final_query = f"SELECT * FROM {last_cte_name};"

        return f"WITH\n{with_clauses}\n{final_query}"