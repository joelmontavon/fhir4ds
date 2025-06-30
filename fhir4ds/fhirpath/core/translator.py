"""
FHIRPath to SQL Translator

This module contains the main translation logic that coordinates between
the FHIRPath parser and SQL generator to produce complete SQL queries.
"""

import re
from typing import List, Dict, Any, Optional, Union

from ..parser import FHIRPathLexer, FHIRPathParser
from .generator import SQLGenerator
from ..parser.ast_nodes import (
    ASTNode, ThisNode, IdentifierNode, PathNode, BinaryOpNode, UnaryOpNode, 
    FunctionCallNode, IndexerNode
)


class FHIRPathToSQL:
    """Main class for translating FHIRPath to SQL"""
    
    def __init__(self, table_name: str = "fhir_resources", json_column: str = "resource", dialect=None):
        self.table_name = table_name
        self.json_column = json_column
        self.dialect = dialect
    
    def translate(
        self,
        expressions: Union[str, List[str], List[tuple[str, Optional[str]]]],
        resource_type_context: Optional[str] = None,
        where_criteria: Optional[List[str]] = None
    ) -> str:
        """
        Translates one or more FHIRPath expressions to a single SQL SELECT statement.

        Args:
            expressions: Can be:
                - A single FHIRPath expression string.
                - A list of FHIRPath expression strings (default aliases 'col_1', 'col_2', ... will be used).
                - A list of (FHIRPath expression string, Optional alias string) tuples.
            resource_type_context: The FHIR resource type context for all expressions.
            where_criteria: An optional list of FHIRPath expression strings, each of which
                            should evaluate to a boolean and will be translated into an
                            SQL WHERE condition, combined with AND.

        Returns:
            A string containing the combined SQL SELECT statement.
        """
        select_clauses_str: str
        determined_resource_type_filter: Optional[str] = None
        
        # Create a master generator to collect all CTEs from expressions and criteria
        master_generator = SQLGenerator(self.table_name, self.json_column, resource_type=resource_type_context, dialect=self.dialect)

        if isinstance(expressions, str):
            parts = self.translate_to_parts_with_generator(expressions, resource_type_context, master_generator)
            select_clauses_str = f"({parts['expression_sql']}) as result"
            if parts.get("resource_type_filter"):
                determined_resource_type_filter = parts["resource_type_filter"]
        else: # List of expressions
            expr_list = expressions 
            if not expr_list:
                if not where_criteria: # No expressions and no criteria
                    return f"-- No FHIRPath expressions or where_criteria provided for table {self.table_name}"
                # If only where_criteria, select_clauses_str will be a placeholder
                select_clauses_str = "1 -- No specific columns selected, only applying WHERE criteria"
            
            select_clauses_list = []
            for i, expr_item in enumerate(expr_list):
                fhirpath_str: str
                alias: Optional[str] = None
                if isinstance(expr_item, tuple):
                    fhirpath_str, alias = expr_item
                else: # isinstance(expr_item, str)
                    fhirpath_str = expr_item 
                
                parts = self.translate_to_parts_with_generator(fhirpath_str, resource_type_context, master_generator)
                if i == 0 and parts.get("resource_type_filter"): # Filter from the first main expression
                    determined_resource_type_filter = parts["resource_type_filter"]
                
                actual_alias = alias if alias else f"col_{i+1}"
                select_clauses_list.append(f"({parts['expression_sql']}) AS {actual_alias}")
            
            if select_clauses_list:
                select_clauses_str = ",\n    ".join(select_clauses_list)
            elif not where_criteria: # No select clauses from expr_list and no where_criteria
                 return f"-- No FHIRPath expressions or where_criteria provided for table {self.table_name}"
            # If expr_list was empty but where_criteria exist, select_clauses_str remains as initialized placeholder


        all_where_conditions = []
        if determined_resource_type_filter:
            all_where_conditions.append(determined_resource_type_filter)

        if where_criteria:
            for criterion_fhirpath in where_criteria:
                lexer = FHIRPathLexer(criterion_fhirpath)
                tokens = lexer.tokenize()
                parser = FHIRPathParser(tokens)
                original_criterion_ast = parser.parse()

                # Criteria are evaluated in the main resource_type_context
                criterion_gen_resource_type = resource_type_context

                # Rewrite the AST for this criterion to strip the resource_type_context 
                # if it's present at the start of paths within the criterion.
                # This ensures that if a criterion is "Patient.active" and context is "Patient",
                # the generator sees "active" (or an equivalent AST leading to $this.active).
                processed_criterion_ast = _rewrite_ast_for_resource_context(
                    original_criterion_ast, 
                    criterion_gen_resource_type # The resource type to strip if found as a prefix
                )
                
                # Use the master generator to accumulate CTEs from criteria too
                # Set WHERE context flag to generate inline expressions instead of CTEs
                master_generator.in_where_context = True
                criterion_sql = master_generator.visit(processed_criterion_ast)
                master_generator.in_where_context = False
                
                # Check for potential date parsing issues in WHERE criteria
                self._check_for_date_parsing_issues(criterion_fhirpath, criterion_sql)
                
                all_where_conditions.append(f"({criterion_sql})")
        
        final_where_clause = ""
        if all_where_conditions:
            final_where_clause = "WHERE " + " AND ".join(all_where_conditions)
            
        select_statement = f"SELECT\n    {select_clauses_str}\nFROM {self.table_name}\n{final_where_clause}".strip()
        
        # Build final query with all accumulated CTEs
        if master_generator.ctes:
            final_query = master_generator._build_final_query_with_ctes(select_statement)
        else:
            final_query = select_statement
            
        final_query += ";"
        return final_query
        
    def translate_to_parts(
        self, fhirpath_expression: str, resource_type_context: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Translate FHIRPath to SQL expression, an optional resource_type filter,
        and internal logging details.
        Returns a dictionary:
        {
            "expression_sql": str,
            "resource_type_filter": Optional[str],
            "processed_ast_type": str,
            "generator_resource_type": Optional[str],
            "generator_debug_steps": List[Dict[str, str]]
        }
        """
        # Create a new generator for this standalone translation
        generator = SQLGenerator(self.table_name, self.json_column, resource_type=resource_type_context, dialect=self.dialect)
        return self.translate_to_parts_with_generator(fhirpath_expression, resource_type_context, generator)
    
    def translate_to_expression_only(
        self, fhirpath_expression: str, resource_type_context: Optional[str] = None
    ) -> str:
        """
        Translate FHIRPath to just the SQL expression part (no SELECT/FROM/CTEs).
        This is useful for embedding the expression in a larger query composition.
        
        For simple expressions, this returns the expression directly.
        For complex expressions that would normally use CTEs, this forces 
        non-CTE generation to return an embeddable expression.
        
        Returns:
            A string containing just the SQL expression
        """
        try:
            # Create a generator with CTEs disabled for composition use
            generator = SQLGenerator(
                self.table_name, 
                self.json_column, 
                resource_type=resource_type_context, 
                dialect=self.dialect
            )
            
            # Temporarily disable all CTE features to force simple expression generation
            # Store original CTE flag and disable it
            original_enable_cte = generator.enable_cte
            generator.enable_cte = False
            
            try:
                # Parse the FHIRPath expression
                from ..parser import FHIRPathLexer, FHIRPathParser
                lexer = FHIRPathLexer(fhirpath_expression)
                tokens = lexer.tokenize()
                parser = FHIRPathParser(tokens)
                ast = parser.parse()
                
                # Generate the SQL expression (without CTEs)
                expression_sql = generator.visit(ast)
                
                # Check for potential date parsing issues
                self._check_for_date_parsing_issues(fhirpath_expression, expression_sql)
                
                return expression_sql
                
            finally:
                # Restore original CTE flag
                generator.enable_cte = original_enable_cte
                    
        except Exception as e:
            raise ValueError(f"Failed to translate FHIRPath expression '{fhirpath_expression}': {str(e)}")
    
    def translate_to_parts_with_generator(
        self, fhirpath_expression: str, resource_type_context: Optional[str], generator: SQLGenerator
    ) -> Dict[str, Any]:
        """
        Translate FHIRPath to SQL expression using a provided generator to accumulate CTEs.
        Returns a dictionary with expression_sql that may reference CTEs accumulated in the generator.
        """
        internal_logs = {}
        try:
            lexer = FHIRPathLexer(fhirpath_expression)
            tokens = lexer.tokenize()
            parser = FHIRPathParser(tokens)
            ast = parser.parse()

            processed_ast = ast
            internal_logs["original_ast_type"] = type(ast).__name__

            # Determine the resource type context for the SQLGenerator and potential filter.
            # Start with the externally provided context.
            current_generator_resource_type: Optional[str] = resource_type_context
            resource_type_filter: Optional[str] = None

            if isinstance(ast, PathNode) and ast.segments:
                first_segment_node = ast.segments[0]
                if isinstance(first_segment_node, IdentifierNode) and _is_potential_resource_type(first_segment_node.name):
                    # Path starts with an explicit ResourceType (e.g., "Patient.name")
                    explicit_resource_type = first_segment_node.name
                    current_generator_resource_type = explicit_resource_type
                    # Use the generator's dialect which is always initialized
                    resource_type_filter = f"{generator.dialect.extract_json_field(self.json_column, '$.resourceType')} = '{explicit_resource_type}'"
                    
                    if len(ast.segments) == 1:  # Path was just "Patient"
                        processed_ast = ThisNode()
                    else:  # Path was "Patient.gender"
                        processed_ast = PathNode(ast.segments[1:])
            elif isinstance(ast, IdentifierNode) and _is_potential_resource_type(ast.name):
                # Expression is just a ResourceType (e.g., "Patient")
                explicit_resource_type = ast.name
                current_generator_resource_type = explicit_resource_type
                # Use the generator's dialect which is always initialized
                resource_type_filter = f"{generator.dialect.extract_json_field(self.json_column, '$.resourceType')} = '{explicit_resource_type}'"
                processed_ast = ThisNode()

            internal_logs["processed_ast_type"] = type(processed_ast).__name__
            internal_logs["generator_resource_type"] = current_generator_resource_type

            # Update the generator's resource_type if we determined one from the expression
            if current_generator_resource_type:
                generator.resource_type = current_generator_resource_type
                
            expression_sql = generator.visit(processed_ast)
            # Note: Don't resolve placeholders here - CTEs will be handled at the final query level
            internal_logs["generator_debug_steps"] = generator.debug_steps # Capture debug steps
            
            # Check for potential date parsing issues
            self._check_for_date_parsing_issues(fhirpath_expression, expression_sql)
            
            return {
                "expression_sql": expression_sql,
                "resource_type_filter": resource_type_filter,
                **internal_logs  # Merge internal logs

            }
        except Exception as e:
            raise ValueError(f"Failed to translate FHIRPath to parts '{fhirpath_expression}': {str(e)}")

    def _check_for_date_parsing_issues(self, fhirpath_expression: str, expression_sql: str) -> None:
        """Check for potential date parsing issues and provide helpful error messages"""
        import re
        
        # Check for patterns that suggest unquoted dates were parsed as arithmetic
        # Look for CAST((YYYY - M) AS DOUBLE) - D patterns in the SQL (with flexible parentheses)
        arithmetic_date_pattern = r'\(?\s*CAST\(\((\d{4})\s*-\s*\d+\)\s*AS\s+DOUBLE\)\s*-\s*\d+\s*\)?'
        
        if re.search(arithmetic_date_pattern, expression_sql):
            # Find potential date literals in the original expression
            unquoted_date_pattern = r'\b(\d{4}-\d{1,2}-\d{1,2})\b'
            dates_found = re.findall(unquoted_date_pattern, fhirpath_expression)
            
            if dates_found:
                dates_str = ", ".join(dates_found)
                raise ValueError(
                    f"Date literals must be quoted. Found unquoted date(s): {dates_str}. "
                    f"Use quoted dates like 'YYYY-MM-DD' instead of YYYY-MM-DD. "
                    f"For example: birthDate > '2000-01-01' instead of birthDate > 2000-01-01"
                )
        
        # Check for other common date parsing issues
        if "CAST" in expression_sql and "DOUBLE" in expression_sql and any(field in fhirpath_expression.lower() for field in ['date', 'birth', 'effective']):
            # Look for patterns like date field compared to numbers
            if re.search(r'\b\d{4}\b', fhirpath_expression) and not re.search(r"'\d{4}", fhirpath_expression):
                raise ValueError(
                    "Date comparisons require quoted date literals. "
                    "Use 'YYYY-MM-DD' format with quotes, e.g., birthDate > '2000-01-01'"
                )


# Helper functions for AST processing

def _is_potential_resource_type(name: str) -> bool:
    """Check if an identifier name might be a FHIR resource type."""
    # Basic heuristic: starts with an uppercase letter and not a special variable like $this
    return name and name[0].isupper() and not name.startswith('$')

def _collect_path_resource_types(node: ASTNode, collected_types: set):
    """Recursively find all potential resource types mentioned at the start of paths in an AST."""
    if isinstance(node, PathNode) and node.segments:
        first_segment = node.segments[0]
        if isinstance(first_segment, IdentifierNode) and _is_potential_resource_type(first_segment.name):
            collected_types.add(first_segment.name)
    
    # Recurse for other node types that can contain paths
    if isinstance(node, BinaryOpNode):
        _collect_path_resource_types(node.left, collected_types)
        _collect_path_resource_types(node.right, collected_types)
    elif isinstance(node, UnaryOpNode):
        _collect_path_resource_types(node.operand, collected_types)
    elif isinstance(node, FunctionCallNode):
        # If the function is part of a path (e.g., Patient.name.where()), PathNode rule handles 'Patient'.
        # Arguments to the function might contain their own paths.
        for arg in node.args:
            _collect_path_resource_types(arg, collected_types)
    elif isinstance(node, IndexerNode):
        _collect_path_resource_types(node.expression, collected_types)

def _rewrite_ast_for_resource_context(node: ASTNode, resource_type: str) -> ASTNode:
    """Recursively rewrite an AST to remove a common resource type prefix from paths."""
    if isinstance(node, PathNode) and node.segments:
        first_segment = node.segments[0]
        if isinstance(first_segment, IdentifierNode) and first_segment.name == resource_type:
            if len(node.segments) == 1: # Path was just "Patient"
                return ThisNode() 
            else: # Path was "Patient.gender", return "gender" (as PathNode or IdentifierNode)
                return PathNode(node.segments[1:])
    elif isinstance(node, IdentifierNode) and node.name == resource_type: # Identifier was just "Patient"
        return ThisNode()

    # Recurse and rebuild for other node types
    if isinstance(node, BinaryOpNode):
        return BinaryOpNode(left=_rewrite_ast_for_resource_context(node.left, resource_type), operator=node.operator, right=_rewrite_ast_for_resource_context(node.right, resource_type))
    elif isinstance(node, UnaryOpNode):
        return UnaryOpNode(operator=node.operator, operand=_rewrite_ast_for_resource_context(node.operand, resource_type))
    elif isinstance(node, FunctionCallNode):
        return FunctionCallNode(name=node.name, args=[_rewrite_ast_for_resource_context(arg, resource_type) for arg in node.args])
    elif isinstance(node, IndexerNode):
        return IndexerNode(expression=_rewrite_ast_for_resource_context(node.expression, resource_type), index=_rewrite_ast_for_resource_context(node.index, resource_type))
    return node # Return other nodes (Literals, ThisNode, etc.) as is
