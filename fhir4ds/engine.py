from typing import Union

from .parser.parser import FHIRPathParser
from .sql.generator import CTEGenerator
from .sql.assembler import SQLAssembler
from .sql.dialect import SQLDialect, get_dialect

class FHIRPathEngine:
    """
    The core FHIRPath-to-SQL compilation engine.

    This class orchestrates the entire compilation process, from parsing the
    FHIRPath expression to generating the final SQL query. It is designed to
    be extensible, allowing for different SQL dialects and custom processing
    steps.
    """

    def __init__(self, dialect: Union[str, SQLDialect] = "duckdb"):
        """
        Initializes the FHIRPathEngine with a specific SQL dialect.

        Args:
            dialect: The SQL dialect to target. Can be a string (e.g., "duckdb")
                     or an instance of a SQLDialect subclass.
        """
        self.dialect = get_dialect(dialect)
        self.parser = FHIRPathParser()
        self.cte_generator = CTEGenerator(self.dialect)
        self.sql_assembler = SQLAssembler(self.dialect)

    def compile(self, fhirpath_expression: str) -> str:
        """
        Compiles a FHIRPath expression to a SQL query.

        This method follows the core architectural steps:
        1. Parse the expression into an Abstract Syntax Tree (AST).
        2. Walk the AST to generate a series of Common Table Expressions (CTEs).
        3. Assemble the CTEs into a final, monolithic SQL query.

        Args:
            fhirpath_expression: The FHIRPath expression string to compile.

        Returns:
            The compiled SQL query as a string.
        """
        # 1. Parse the FHIRPath expression into an AST
        ast = self.parser.parse(fhirpath_expression)

        # 2. Generate CTEs from the AST
        # This will be a list of CTEs, each representing a step in the expression
        cte_chain = self.cte_generator.generate(ast)

        # 3. Assemble the final SQL query from the CTEs
        sql_query = self.sql_assembler.assemble(cte_chain)

        return sql_query