from dataclasses import dataclass
from typing import List, Any

from ..ast import nodes
from ..ast.visitors import ASTVisitor
from .dialect import SQLDialect

@dataclass
class CTE:
    """Represents a single Common Table Expression (CTE)."""
    name: str
    sql: str

class CTEGenerator(ASTVisitor[Any]):
    """
    Generates a chain of SQL CTEs from a FHIRPath AST.

    This visitor walks the AST and, for each node, generates a corresponding
    SQL snippet that builds upon the previous one. It is a stateful visitor,
    tracking the last generated CTE to chain them correctly.
    """

    def __init__(self, dialect: SQLDialect):
        self.dialect = dialect
        self.ctes: List[CTE] = []
        self.cte_counter = 0
        # State tracking for chaining
        self.current_input_cte: str = None
        self.current_column: str = "resource"

    def _new_cte_name(self, prefix: str = "cte") -> str:
        """Generates a unique name for a new CTE."""
        name = f"{prefix.lower()}_{self.cte_counter}"
        self.cte_counter += 1
        return name

    def generate(self, node: nodes.FHIRPathNode) -> List[CTE]:
        """Generates the full list of CTEs for the given AST."""
        self.visit(node)
        return self.ctes

    def visit_path_expression(self, node: nodes.PathExpression) -> None:
        """Visits a path expression, chaining CTEs for each part."""
        for part in node.path:
            self.visit(part)

    def visit_identifier(self, node: nodes.Identifier) -> None:
        """
        Handles identifiers, which can be a root resource type or a field access.
        """
        if self.current_input_cte is None:
            # This is the root of the expression, e.g., 'Patient'.
            # It creates the first CTE that selects the base resource.
            cte_name = self._new_cte_name(node.value)
            sql = (
                f"SELECT resource FROM fhir_resources WHERE "
                f"{self.dialect.json_extract('resource', '$.resourceType')} = '{node.value}'"
            )
            self.ctes.append(CTE(name=cte_name, sql=sql))
            self.current_input_cte = cte_name
            self.current_column = "resource"
        else:
            # This is a member access, e.g., '.name'.
            # It extracts the property from the previous CTE's result.
            cte_name = self._new_cte_name(node.value)
            sql = (
                f"SELECT {self.dialect.json_extract(self.current_column, f'$.{node.value}')} as result "
                f"FROM {self.current_input_cte}"
            )
            self.ctes.append(CTE(name=cte_name, sql=sql))
            self.current_input_cte = cte_name
            self.current_column = "result"

    def visit_function_call(self, node: nodes.FunctionCall) -> None:
        """Handles function calls, dispatching to specific handlers."""
        if self.current_input_cte is None:
            raise ValueError("Function call must be part of a path expression.")

        func_name = node.name.value
        handler_name = f"_handle_{func_name}"

        if not hasattr(self, handler_name):
            raise NotImplementedError(f"The function '{func_name}' is not supported.")

        handler = getattr(self, handler_name)
        handler(node)

    def _handle_first(self, node: nodes.FunctionCall) -> None:
        """Handles the .first() function."""
        cte_name = self._new_cte_name("first")
        # Assumes the input is a JSON array in the current column.
        sql = (
            f"SELECT {self.dialect.json_extract(self.current_column, '$[0]')} as result "
            f"FROM {self.current_input_cte}"
        )
        self.ctes.append(CTE(name=cte_name, sql=sql))
        self.current_input_cte = cte_name
        self.current_column = "result"

    def visit_string_literal(self, node: "nodes.StringLiteral") -> str:
        # Literals are not expected to be visited in this simplified generator
        # except as arguments to functions, which would be handled inside the
        # function handler. For now, we can raise an error.
        raise NotImplementedError("Literals cannot be at the root of a CTE-generating expression.")

    # Add placeholders for other node types to satisfy the abstract base class
    def visit_number_literal(self, node: "nodes.NumberLiteral") -> Any:
        raise NotImplementedError("Not yet implemented.")

    def visit_boolean_literal(self, node: "nodes.BooleanLiteral") -> Any:
        raise NotImplementedError("Not yet implemented.")

    def visit_date_literal(self, node: "nodes.DateLiteral") -> Any:
        raise NotImplementedError("Not yet implemented.")

    def visit_datetime_literal(self, node: "nodes.DateTimeLiteral") -> Any:
        raise NotImplementedError("Not yet implemented.")

    def visit_time_literal(self, node: "nodes.TimeLiteral") -> Any:
        raise NotImplementedError("Not yet implemented.")

    def visit_quantity_literal(self, node: "nodes.QuantityLiteral") -> Any:
        raise NotImplementedError("Not yet implemented.")

    def visit_collection_literal(self, node: "nodes.CollectionLiteral") -> Any:
        raise NotImplementedError("Not yet implemented.")

    def visit_binary_operation(self, node: "nodes.BinaryOperation") -> Any:
        raise NotImplementedError("Not yet implemented.")

    def visit_unary_operation(self, node: "nodes.UnaryOperation") -> Any:
        raise NotImplementedError("Not yet implemented.")