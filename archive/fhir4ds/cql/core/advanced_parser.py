"""
CQL Advanced Parser - Enhanced CQL parsing with support for advanced constructs.

Adds support for Phase 6 advanced CQL constructs including:
- with/without clauses
- let expressions  
- enhanced multi-resource queries
"""

import logging
import re
from typing import List, Dict, Any, Optional, Union, Tuple
from enum import Enum

# Import base parser infrastructure
from .parser import CQLParser, CQLASTNode, TokenType
from ...fhirpath.parser.ast_nodes import ASTNode

logger = logging.getLogger(__name__)


class AdvancedCQLTokenType(Enum):
    """Advanced CQL token types for Phase 6 constructs."""
    # Relationship operators
    WITH = "WITH"
    WITHOUT = "WITHOUT"
    SUCH = "SUCH"
    THAT = "THAT"
    
    # Variable definitions
    LET = "LET"
    
    # Enhanced query constructs
    ALL = "ALL"
    ANY = "ANY"
    EXISTS = "EXISTS"
    
    # Multi-resource constructs
    RELATED = "RELATED"
    REFERENCES = "REFERENCES"


class WithClauseNode(CQLASTNode):
    """AST node for CQL 'with' clause relationship."""

    def __init__(self, source_alias: str, related_query: Any, condition: Any,
                 is_without: bool = False):
        """
        Initialize with clause node.

        Args:
            source_alias: Alias for the source resource in the relationship
            related_query: The related resource query expression
            condition: The relationship condition (such that ...) - can be string or SuchThatConditionNode
            is_without: True for 'without' clause, False for 'with' clause
        """
        self.source_alias = source_alias
        self.related_query = related_query
        self.is_without = is_without
        self.clause_type = "without" if is_without else "with"

        # Enhanced: Parse condition into structured form if it's a string
        if isinstance(condition, str):
            self.condition = SuchThatConditionNode(condition)
        else:
            self.condition = condition

    def has_references(self) -> bool:
        """Check if the condition contains reference expressions."""
        return hasattr(self.condition, 'has_references') and self.condition.has_references()

    def has_temporal_conditions(self) -> bool:
        """Check if the condition contains temporal operators."""
        return hasattr(self.condition, 'has_temporal_conditions') and self.condition.has_temporal_conditions()

    def is_complex_condition(self) -> bool:
        """Check if the condition is complex (has logical operators)."""
        return hasattr(self.condition, 'is_compound_condition') and self.condition.is_compound_condition()

    def __str__(self):
        clause = "without" if self.is_without else "with"
        return f"{clause} {self.related_query} {self.source_alias} such that {self.condition}"


class LetExpressionNode(CQLASTNode):
    """AST node for CQL 'let' expression (variable definition)."""

    def __init__(self, variable_name: str, expression: Any):
        """
        Initialize let expression node.

        Args:
            variable_name: The name of the variable being defined
            expression: The expression that defines the variable value
        """
        self.variable_name = variable_name
        self.expression = expression

    def __str__(self):
        return f"let {self.variable_name}: {self.expression}"


class SuchThatConditionNode(CQLASTNode):
    """AST node for complex 'such that' conditions in relationship clauses."""

    def __init__(self, condition_text: str):
        """
        Initialize such that condition node.

        Args:
            condition_text: The raw condition text
        """
        self.condition_text = condition_text
        self.parsed_conditions = []
        self.reference_patterns = []
        self.temporal_conditions = []
        self.logical_operators = []

        # Parse the condition into components
        self._parse_condition_components()

    def _parse_condition_components(self):
        """Parse condition text into structured components."""
        import re

        # Extract reference patterns (e.g., "A.subject references B.subject")
        reference_pattern = r'(\w+)\.(\w+)\s+references\s+(\w+)\.(\w+)'
        for match in re.finditer(reference_pattern, self.condition_text, re.IGNORECASE):
            self.reference_patterns.append({
                'source_alias': match.group(1),
                'source_field': match.group(2),
                'target_alias': match.group(3),
                'target_field': match.group(4)
            })

        # Extract temporal conditions (e.g., "during", "overlaps", "before", "after")
        # Pattern 1: quoted operands
        temporal_pattern_quoted = r'(\w+)\.(\w+)\s+(during|overlaps|before|after|meets|starts|ends)\s+"([^"]+)"'
        for match in re.finditer(temporal_pattern_quoted, self.condition_text, re.IGNORECASE):
            self.temporal_conditions.append({
                'alias': match.group(1),
                'field': match.group(2),
                'operator': match.group(3),
                'operand': match.group(4)
            })

        # Pattern 2: unquoted operands (like field references)
        temporal_pattern_unquoted = r'(\w+)\.(\w+)\s+(during|overlaps|before|after|meets|starts|ends)\s+(\w+(?:\.\w+)*)'
        for match in re.finditer(temporal_pattern_unquoted, self.condition_text, re.IGNORECASE):
            # Only add if not already captured by quoted pattern
            if not any(t['alias'] == match.group(1) and t['field'] == match.group(2)
                      and t['operator'] == match.group(3) for t in self.temporal_conditions):
                self.temporal_conditions.append({
                    'alias': match.group(1),
                    'field': match.group(2),
                    'operator': match.group(3),
                    'operand': match.group(4)
                })

        # Extract logical operators
        logical_pattern = r'\b(and|or|not)\b'
        self.logical_operators = re.findall(logical_pattern, self.condition_text, re.IGNORECASE)

    def has_references(self) -> bool:
        """Check if condition contains reference patterns."""
        return len(self.reference_patterns) > 0

    def has_temporal_conditions(self) -> bool:
        """Check if condition contains temporal operators."""
        return len(self.temporal_conditions) > 0

    def is_compound_condition(self) -> bool:
        """Check if condition contains logical operators."""
        return len(self.logical_operators) > 0

    def __str__(self):
        return self.condition_text


class ReferenceExpressionNode(CQLASTNode):
    """AST node for reference expressions in CQL (e.g., A.subject references B.subject)."""

    def __init__(self, source_alias: str, source_field: str,
                 target_alias: str, target_field: str, operator: str = "references"):
        """
        Initialize reference expression node.

        Args:
            source_alias: Source resource alias
            source_field: Source field name
            target_alias: Target resource alias
            target_field: Target field name
            operator: Reference operator (references, same as, etc.)
        """
        self.source_alias = source_alias
        self.source_field = source_field
        self.target_alias = target_alias
        self.target_field = target_field
        self.operator = operator

    def __str__(self):
        return f"{self.source_alias}.{self.source_field} {self.operator} {self.target_alias}.{self.target_field}"


class TemporalConditionNode(CQLASTNode):
    """AST node for temporal conditions in CQL (e.g., A.effective during "Measurement Period")."""

    def __init__(self, alias: str, field: str, operator: str, operand: str):
        """
        Initialize temporal condition node.

        Args:
            alias: Resource alias
            field: Field name (e.g., effective, period)
            operator: Temporal operator (during, overlaps, before, after, etc.)
            operand: Operand (often a parameter or time interval)
        """
        self.alias = alias
        self.field = field
        self.operator = operator
        self.operand = operand

    def __str__(self):
        return f"{self.alias}.{self.field} {self.operator} {self.operand}"


class QueryWithRelationshipsNode(CQLASTNode):
    """AST node for queries with with/without relationships."""
    
    def __init__(self, primary_query: Any, alias: str):
        """
        Initialize query with relationships node.
        
        Args:
            primary_query: The main resource query
            alias: Alias for the primary query
        """
        self.primary_query = primary_query
        self.alias = alias
        self.with_clauses = []
        self.without_clauses = []
        self.let_expressions = []
        self.where_condition = None
        self.return_expression = None
    
    def add_with_clause(self, with_clause: WithClauseNode):
        """Add a 'with' clause to the query. Both 'with' and 'without' clauses are stored in with_clauses."""
        self.with_clauses.append(with_clause)
        # Also maintain separate lists for easy access
        if with_clause.is_without:
            self.without_clauses.append(with_clause)
    
    def add_let_expression(self, let_expr: LetExpressionNode):
        """Add a 'let' expression to the query."""
        self.let_expressions.append(let_expr)
    
    def set_where_condition(self, condition: Any):
        """Set the where condition for the query."""
        self.where_condition = condition
    
    def set_return_expression(self, expression: Any):
        """Set the return expression for the query."""
        self.return_expression = expression
    
    def __str__(self):
        parts = [f"{self.primary_query} {self.alias}"]
        
        for let_expr in self.let_expressions:
            parts.append(f"  {let_expr}")
        
        for with_clause in self.with_clauses:
            parts.append(f"  {with_clause}")
        
        for without_clause in self.without_clauses:
            parts.append(f"  {without_clause}")
        
        if self.where_condition:
            parts.append(f"  where {self.where_condition}")
        
        if self.return_expression:
            parts.append(f"  return {self.return_expression}")
        
        return "\n".join(parts)


class QueryExpressionNode(CQLASTNode):
    """AST node for advanced CQL query expressions."""

    def __init__(self, source_expression: Any, alias: str = None):
        """
        Initialize query expression node.

        Args:
            source_expression: The source expression (collection, resource, etc.)
            alias: Optional alias for the source
        """
        self.source_expression = source_expression
        self.alias = alias
        self.where_condition = None
        self.return_expression = None
        self.sort_clause = None
        self.aggregate_clause = None
        self.is_distinct = False
        self.is_all = False

    def set_where_condition(self, condition: Any):
        """Set the where condition for the query."""
        self.where_condition = condition

    def set_return_expression(self, expression: Any):
        """Set the return expression for the query."""
        self.return_expression = expression

    def set_sort_clause(self, sort_clause: Any):
        """Set the sort clause for the query."""
        self.sort_clause = sort_clause

    def set_aggregate_clause(self, aggregate_clause: Any):
        """Set the aggregate clause for the query."""
        self.aggregate_clause = aggregate_clause

    def set_distinct(self, distinct: bool = True):
        """Set whether to return distinct results."""
        self.is_distinct = distinct

    def set_all(self, all: bool = True):
        """Set whether to include all results."""
        self.is_all = all

    def __str__(self):
        parts = []

        # Source with alias
        if self.alias:
            parts.append(f"{self.source_expression} {self.alias}")
        else:
            parts.append(str(self.source_expression))

        # Where clause
        if self.where_condition:
            parts.append(f"where {self.where_condition}")

        # Return clause
        if self.return_expression:
            parts.append(f"return {self.return_expression}")

        # Sort clause
        if self.sort_clause:
            parts.append(f"sort {self.sort_clause}")

        # Aggregate clause
        if self.aggregate_clause:
            parts.append(f"aggregate {self.aggregate_clause}")

        return " ".join(parts)


class MultiSourceQueryNode(CQLASTNode):
    """AST node for multi-source CQL queries (from clause with multiple sources)."""

    def __init__(self):
        """Initialize multi-source query node."""
        self.sources = []  # List of (expression, alias) tuples
        self.where_condition = None
        self.return_expression = None
        self.sort_clause = None
        self.aggregate_clause = None
        self.is_distinct = False

    def add_source(self, expression: Any, alias: str):
        """Add a source expression with alias."""
        self.sources.append((expression, alias))

    def set_where_condition(self, condition: Any):
        """Set the where condition for the query."""
        self.where_condition = condition

    def set_return_expression(self, expression: Any):
        """Set the return expression for the query."""
        self.return_expression = expression

    def set_sort_clause(self, sort_clause: Any):
        """Set the sort clause for the query."""
        self.sort_clause = sort_clause

    def set_aggregate_clause(self, aggregate_clause: Any):
        """Set the aggregate clause for the query."""
        self.aggregate_clause = aggregate_clause

    def set_distinct(self, distinct: bool = True):
        """Set whether to return distinct results."""
        self.is_distinct = distinct

    def __str__(self):
        parts = []

        # From clause with multiple sources
        source_parts = []
        for expr, alias in self.sources:
            source_parts.append(f"{expr} {alias}")
        parts.append(f"from {', '.join(source_parts)}")

        # Where clause
        if self.where_condition:
            parts.append(f"where {self.where_condition}")

        # Return clause
        if self.return_expression:
            parts.append(f"return {self.return_expression}")

        # Sort clause
        if self.sort_clause:
            parts.append(f"sort {self.sort_clause}")

        # Aggregate clause
        if self.aggregate_clause:
            parts.append(f"aggregate {self.aggregate_clause}")

        return " ".join(parts)


class AggregateClauseNode(CQLASTNode):
    """AST node for CQL aggregate clauses."""

    def __init__(self, result_alias: str, starting_expression: Any = None,
                 aggregate_expression: Any = None, is_distinct: bool = False, is_all: bool = False):
        """
        Initialize aggregate clause node.

        Args:
            result_alias: Alias for the aggregate result
            starting_expression: Optional starting value expression
            aggregate_expression: The aggregate expression
            is_distinct: Whether to aggregate distinct values only
            is_all: Whether to include all values (default behavior)
        """
        self.result_alias = result_alias
        self.starting_expression = starting_expression
        self.aggregate_expression = aggregate_expression
        self.is_distinct = is_distinct
        self.is_all = is_all

    def __str__(self):
        parts = []

        if self.is_distinct:
            parts.append("distinct")
        elif self.is_all:
            parts.append("all")

        parts.append(self.result_alias)

        if self.starting_expression:
            parts.append(f"starting {self.starting_expression}")

        parts.append(":")

        if self.aggregate_expression:
            parts.append(str(self.aggregate_expression))

        return " ".join(parts)


class SortClauseNode(CQLASTNode):
    """AST node for CQL sort clauses."""

    def __init__(self, sort_expression: Any = None, direction: str = "asc"):
        """
        Initialize sort clause node.

        Args:
            sort_expression: Optional expression to sort by
            direction: Sort direction ('asc', 'desc', 'ascending', 'descending')
        """
        self.sort_expression = sort_expression
        self.direction = direction.lower()

    def __str__(self):
        if self.sort_expression:
            return f"by {self.sort_expression} {self.direction}"
        else:
            return self.direction


class CollectionOperationNode(CQLASTNode):
    """AST node for advanced collection operations (union, intersect, except, distinct, flatten)."""

    def __init__(self, operation_type: str, base_expression: Any,
                 operand_expression: Any = None, predicate_expression: Any = None):
        """
        Initialize collection operation node.

        Args:
            operation_type: Type of operation ('union', 'intersect', 'except', 'distinct', 'flatten', etc.)
            base_expression: The primary collection expression
            operand_expression: Secondary collection (for binary operations)
            predicate_expression: Optional predicate function (for operations like 'unionBy')
        """
        self.operation_type = operation_type.lower()
        self.base_expression = base_expression
        self.operand_expression = operand_expression
        self.predicate_expression = predicate_expression

        # Categorize operation
        self.is_unary_operation = operation_type.lower() in ['distinct', 'flatten', 'deepflatten']
        self.is_binary_operation = operation_type.lower() in ['union', 'intersect', 'except']
        self.is_predicate_operation = operation_type.lower().endswith('by')

    def has_predicate(self) -> bool:
        """Check if the operation uses a custom predicate function."""
        return self.predicate_expression is not None

    def is_set_operation(self) -> bool:
        """Check if this is a set operation (union, intersect, except)."""
        return self.operation_type in ['union', 'intersect', 'except', 'unionby', 'intersectby', 'exceptby']

    def requires_operand(self) -> bool:
        """Check if the operation requires a second operand."""
        return self.is_binary_operation or self.is_predicate_operation

    def __str__(self):
        if self.is_unary_operation:
            return f"{self.base_expression}.{self.operation_type}()"
        elif self.operand_expression and self.predicate_expression:
            return f"{self.base_expression}.{self.operation_type}({self.operand_expression}, {self.predicate_expression})"
        elif self.operand_expression:
            return f"{self.base_expression} {self.operation_type} {self.operand_expression}"
        else:
            return f"{self.base_expression}.{self.operation_type}()"


class SetOperationNode(CQLASTNode):
    """AST node specifically for set operations (union, intersect, except) with enhanced functionality."""

    def __init__(self, left_expression: Any, operator: str, right_expression: Any,
                 equality_predicate: Any = None):
        """
        Initialize set operation node.

        Args:
            left_expression: Left operand collection
            operator: Set operator ('union', 'intersect', 'except')
            right_expression: Right operand collection
            equality_predicate: Optional custom equality predicate for comparison
        """
        self.left_expression = left_expression
        self.operator = operator.lower()
        self.right_expression = right_expression
        self.equality_predicate = equality_predicate

        # Validate operator
        valid_operators = ['union', 'intersect', 'except']
        if self.operator not in valid_operators:
            raise ValueError(f"Invalid set operator: {operator}. Must be one of {valid_operators}")

    def has_custom_equality(self) -> bool:
        """Check if the operation uses a custom equality predicate."""
        return self.equality_predicate is not None

    def is_symmetric_operation(self) -> bool:
        """Check if the operation is commutative (union and intersect are, except is not)."""
        return self.operator in ['union', 'intersect']

    def __str__(self):
        if self.equality_predicate:
            return f"{self.left_expression} {self.operator}By({self.right_expression}, {self.equality_predicate})"
        else:
            return f"{self.left_expression} {self.operator} {self.right_expression}"


class CollectionQueryNode(CQLASTNode):
    """AST node for complex collection queries with filtering, sorting, and aggregation."""

    def __init__(self, source_expression: Any, alias: str = None):
        """
        Initialize collection query node.

        Args:
            source_expression: The source collection expression
            alias: Optional alias for the collection elements
        """
        self.source_expression = source_expression
        self.alias = alias
        self.where_conditions = []
        self.let_expressions = []
        self.return_expression = None
        self.sort_clauses = []
        self.aggregation_clause = None
        self.collection_operations = []

    def add_where_condition(self, condition: Any):
        """Add a where condition to filter collection elements."""
        self.where_conditions.append(condition)

    def add_let_expression(self, let_expr: LetExpressionNode):
        """Add a let expression for variable definition."""
        self.let_expressions.append(let_expr)

    def add_sort_clause(self, sort_clause: SortClauseNode):
        """Add a sort clause."""
        self.sort_clauses.append(sort_clause)

    def add_collection_operation(self, operation: CollectionOperationNode):
        """Add a collection operation to be applied."""
        self.collection_operations.append(operation)

    def set_return_expression(self, return_expr: Any):
        """Set the return expression for the collection query."""
        self.return_expression = return_expr

    def set_aggregation(self, agg_clause: AggregateClauseNode):
        """Set aggregation clause."""
        self.aggregation_clause = agg_clause

    def has_complex_operations(self) -> bool:
        """Check if the query has complex operations (multiple clauses)."""
        return (len(self.where_conditions) > 0 or
                len(self.let_expressions) > 0 or
                len(self.sort_clauses) > 0 or
                len(self.collection_operations) > 0 or
                self.aggregation_clause is not None)

    def __str__(self):
        parts = [str(self.source_expression)]

        if self.alias:
            parts.append(self.alias)

        for let_expr in self.let_expressions:
            parts.append(f"let {let_expr}")

        for where_cond in self.where_conditions:
            parts.append(f"where {where_cond}")

        for collection_op in self.collection_operations:
            parts.append(str(collection_op))

        if self.return_expression:
            parts.append(f"return {self.return_expression}")

        for sort_clause in self.sort_clauses:
            parts.append(f"sort {sort_clause}")

        if self.aggregation_clause:
            parts.append(str(self.aggregation_clause))

        return " ".join(parts)


class AdvancedCQLParser:
    """Enhanced CQL parser supporting advanced constructs."""

    def __init__(self, base_parser: CQLParser = None):
        """Initialize advanced parser with base CQL parser."""
        self.base_parser = base_parser  # Will be set when needed
        self.advanced_keywords = {
            'with', 'without', 'such', 'that', 'let', 'all', 'any',
            'exists', 'related', 'references', 'from', 'where', 'return',
            'sort', 'aggregate', 'distinct', 'ascending', 'descending',
            # Collection operation keywords
            'union', 'intersect', 'except', 'flatten', 'deepflatten',
            'unionby', 'intersectby', 'exceptby', 'distinctby', 'sortby',
            'groupby', 'partitionby', 'reduceby'
        }
    
    def parse_advanced_cql(self, cql_text: str) -> Any:
        """
        Parse CQL text with advanced construct support.

        Args:
            cql_text: CQL expression text to parse

        Returns:
            AST node representing the parsed CQL
        """
        # Normalize whitespace and handle line breaks
        normalized_text = self._normalize_cql_text(cql_text)

        # Check for different types of advanced constructs
        has_with_without = self._has_with_without_clauses(normalized_text)
        has_let_expressions = self._has_let_expressions(normalized_text)
        has_from_clause = self._has_from_clause(normalized_text)
        has_aggregate = self._has_aggregate_clause(normalized_text)
        has_query_alias = self._has_query_alias(normalized_text)
        has_collection_ops = self._has_collection_operations(normalized_text)

        # Route to appropriate parser based on detected patterns
        if has_with_without and has_let_expressions:
            # Combined let expressions with with/without clauses
            return self._parse_combined_advanced_query(normalized_text)
        elif has_with_without:
            return self._parse_query_with_relationships(normalized_text)
        elif has_let_expressions:
            return self._parse_query_with_let_expressions(normalized_text)
        elif has_collection_ops:
            # Handle collection operations like union, intersect, flatten, etc.
            return self._parse_collection_operation_query(normalized_text)
        elif has_from_clause:
            return self._parse_from_query(normalized_text)
        elif has_aggregate:
            return self._parse_aggregate_query(normalized_text)
        elif has_query_alias:
            return self._parse_aliased_query(normalized_text)
        else:
            # Fall back to base parser for standard CQL
            if self.base_parser:
                return self.base_parser.parse(normalized_text)
            else:
                # Return a simple representation for standard CQL
                return {"type": "standard_cql", "text": normalized_text}
    
    def _normalize_cql_text(self, text: str) -> str:
        """Normalize CQL text for parsing."""
        # Replace multiple whitespaces with single spaces
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Extract query body from define statements
        define_pattern = r'define\s+"[^"]+"\s*:\s*(.*)'
        define_match = re.match(define_pattern, text, re.IGNORECASE | re.DOTALL)
        if define_match:
            text = define_match.group(1).strip()
        
        # Ensure consistent keyword casing
        for keyword in self.advanced_keywords:
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + keyword + r'\b'
            text = re.sub(pattern, keyword, text, flags=re.IGNORECASE)
        
        return text
    
    def _has_with_without_clauses(self, text: str) -> bool:
        """Check if text contains with/without clauses."""
        with_pattern = r'\bwith\s+\[[^\]]+\]'
        without_pattern = r'\bwithout\s+\[[^\]]+\]'
        # Also check for malformed with/without clauses
        malformed_with_pattern = r'\b(with|without)\s+such\s+that'

        return bool(re.search(with_pattern, text, re.IGNORECASE) or
                   re.search(without_pattern, text, re.IGNORECASE) or
                   re.search(malformed_with_pattern, text, re.IGNORECASE))
    
    def _has_let_expressions(self, text: str) -> bool:
        """Check if text contains let expressions."""
        let_pattern = r'\blet\s+\w+\s*:'
        return bool(re.search(let_pattern, text, re.IGNORECASE))

    def _has_from_clause(self, text: str) -> bool:
        """Check if text contains from clause with multiple sources."""
        # Pattern: from (expression) alias, (expression) alias
        from_pattern = r'\bfrom\s+\([^)]+\)\s+\w+\s*,\s*\([^)]+\)\s+\w+'
        return bool(re.search(from_pattern, text, re.IGNORECASE))

    def _has_aggregate_clause(self, text: str) -> bool:
        """Check if text contains aggregate clause."""
        # Pattern: collection alias aggregate result_alias : expression
        aggregate_pattern = r'\)\s+\w+\s+aggregate\s+(all\s+|distinct\s+)?\w+'
        return bool(re.search(aggregate_pattern, text, re.IGNORECASE))

    def _has_query_alias(self, text: str) -> bool:
        """Check if text contains query with alias but no from clause."""
        # Pattern: (expression) alias [optional clauses]
        # But not a function call like DateTime(2012, 10, 5)
        alias_pattern = r'\([^)]+\)\s+[a-zA-Z][a-zA-Z0-9]*\s+(sort|return|where|aggregate)'
        simple_alias_pattern = r'^\([^)]+\)\s+[a-zA-Z][a-zA-Z0-9]*$'

        return (bool(re.search(alias_pattern, text, re.IGNORECASE)) or
                bool(re.search(simple_alias_pattern, text.strip(), re.IGNORECASE)))
    
    def _validate_with_without_clauses(self, text: str):
        """Validate that with/without clauses have proper syntax."""
        import re

        # Check for with/without clauses without "such that"
        invalid_with_pattern = r'\b(with|without)\s+\[[^\]]+\]\s+\w+\s*$'
        invalid_matches = re.findall(invalid_with_pattern, text.strip(), re.IGNORECASE)

        if invalid_matches:
            raise SyntaxError(f"with/without clause must be followed by 'such that' condition")

        # Check for "such that" without proper condition
        empty_such_that_pattern = r'\bsuch\s+that\s*$'
        empty_matches = re.search(empty_such_that_pattern, text.strip(), re.IGNORECASE)

        if empty_matches:
            raise SyntaxError("'such that' clause cannot be empty")

        # Check for "with/without" without resource specification
        invalid_resource_pattern = r'\b(with|without)\s+such\s+that'
        if re.search(invalid_resource_pattern, text, re.IGNORECASE):
            raise SyntaxError("with/without clause missing resource specification")
    
    def _validate_let_expressions(self, let_expressions: List[LetExpressionNode]):
        """Validate let expressions for circular references and other issues."""
        # Build dependency graph
        dependencies = {}
        for let_expr in let_expressions:
            var_name = let_expr.variable_name
            expression = let_expr.expression
            
            # Find variable references in the expression
            referenced_vars = []
            for other_let in let_expressions:
                if other_let.variable_name != var_name and other_let.variable_name in expression:
                    referenced_vars.append(other_let.variable_name)
            
            dependencies[var_name] = referenced_vars
        
        # Check for circular references using simple cycle detection
        for var_name in dependencies:
            if self._has_circular_dependency(var_name, dependencies, set()):
                raise ValueError(f"Circular reference detected in let expression: {var_name}")
    
    def _has_circular_dependency(self, var_name: str, dependencies: dict, visited: set) -> bool:
        """Check if a variable has circular dependencies."""
        if var_name in visited:
            return True
        
        visited.add(var_name)
        
        for dep in dependencies.get(var_name, []):
            if self._has_circular_dependency(dep, dependencies, visited.copy()):
                return True
        
        return False
    
    def _parse_query_with_relationships(self, text: str) -> QueryWithRelationshipsNode:
        """Parse a query containing with/without relationship clauses."""
        logger.debug(f"Parsing query with relationships: {text}")

        # Extract the primary query (resource retrieval)
        primary_match = re.match(r'^\s*(\[[^\]]+\])\s+(\w+)', text, re.IGNORECASE)
        if not primary_match:
            raise ValueError(f"Invalid query format: {text}")

        primary_query = primary_match.group(1)
        primary_alias = primary_match.group(2)

        query_node = QueryWithRelationshipsNode(primary_query, primary_alias)

        # Parse with/without clauses
        remaining_text = text[primary_match.end():].strip()

        # Validate that with/without clauses have proper "such that" conditions
        self._validate_with_without_clauses(remaining_text)

        # Enhanced pattern to match with/without clauses with better condition parsing
        # This pattern should match one clause at a time, handling multiline properly
        relationship_pattern = r'\b(with|without)\s+(\[[^\]]+\])\s+(\w+)\s+such\s+that\s+(.*?)(?=\s*\b(?:with|without|where|return)\b|$)'

        for match in re.finditer(relationship_pattern, remaining_text, re.IGNORECASE | re.DOTALL):
            clause_type = match.group(1).lower()
            related_query = match.group(2)
            related_alias = match.group(3)
            condition = match.group(4).strip()

            # Clean up the condition (remove trailing keywords if they accidentally got captured)
            condition = re.sub(r'\s+(with|without|where|return).*$', '', condition, flags=re.IGNORECASE | re.DOTALL)

            # Remove extra whitespace and newlines
            condition = ' '.join(condition.split())

            # Validate that condition is not empty
            if not condition.strip():
                raise ValueError(f"Empty condition in {clause_type} clause")

            # Enhanced: Create with clause with structured condition parsing
            is_without = clause_type == 'without'
            with_clause = WithClauseNode(related_alias, related_query, condition, is_without)

            # Additional analysis for complex conditions
            if self._contains_complex_condition(condition):
                with_clause.complexity_level = "complex"
                with_clause.parsed_condition = self._parse_complex_condition(condition)
            else:
                with_clause.complexity_level = "simple"

            query_node.add_with_clause(with_clause)

        # Parse where clause if present
        where_match = re.search(r'\bwhere\s+(.+?)(?:\breturn\s|$)', remaining_text, re.IGNORECASE)
        if where_match:
            query_node.set_where_condition(where_match.group(1).strip())

        # Parse return clause if present
        return_match = re.search(r'\breturn\s+(.+)$', remaining_text, re.IGNORECASE)
        if return_match:
            query_node.set_return_expression(return_match.group(1).strip())

        return query_node
    
    def _parse_query_with_let_expressions(self, text: str) -> Any:
        """Parse a query containing let expressions."""
        logger.debug(f"Parsing query with let expressions: {text}")
        
        # Extract the primary query (resource retrieval) - look for pattern like [ResourceType] Alias
        primary_match = re.match(r'^\s*(\[[^\]]+\])\s+(\w+)', text, re.IGNORECASE)
        if not primary_match:
            # Fallback: create a generic container
            container = QueryWithRelationshipsNode("[Query]", "Q")
        else:
            primary_query = primary_match.group(1)
            primary_alias = primary_match.group(2)
            container = QueryWithRelationshipsNode(primary_query, primary_alias)
        
        # Find all let expressions in the text
        let_expressions = []
        let_block_pattern = r'let\s+(.*?)(?=where|return|$)'
        let_block_match = re.search(let_block_pattern, text, re.IGNORECASE | re.DOTALL)
        
        if let_block_match:
            let_block = let_block_match.group(1)
            # Split by comma and parse each let statement
            let_statements = re.split(r',\s*(?=\w+\s*:)', let_block)
            
            for statement in let_statements:
                statement = statement.strip()
                if statement:
                    # Parse individual let statement: variable: expression
                    let_pattern = r'(\w+)\s*:\s*(.+?)(?=,|\s*$)'
                    match = re.match(let_pattern, statement, re.DOTALL)
                    if match:
                        var_name = match.group(1).strip()
                        expression = match.group(2).strip().rstrip(',')
                        let_expressions.append(LetExpressionNode(var_name, expression))
        
        # Validate let expressions for circular references
        if let_expressions:
            self._validate_let_expressions(let_expressions)
        
        # Add let expressions to the container
        for let_expr in let_expressions:
            container.add_let_expression(let_expr)
        
        return container
    
    def _parse_combined_advanced_query(self, text: str) -> QueryWithRelationshipsNode:
        """Parse a query containing both let expressions and with/without relationships."""
        logger.debug(f"Parsing combined advanced query: {text}")

        # First extract let expressions
        let_expressions = []
        let_block_pattern = r'let\s+(.*?)(?=\[|\bdefine|$)'
        let_block_match = re.search(let_block_pattern, text, re.IGNORECASE | re.DOTALL)

        if let_block_match:
            let_block = let_block_match.group(1)
            # Split by comma and parse each let statement
            let_statements = re.split(r',\s*(?=\w+\s*:)', let_block)

            for statement in let_statements:
                statement = statement.strip()
                if statement:
                    # Parse individual let statement: variable: expression
                    let_pattern = r'(\w+)\s*:\s*(.+?)(?=,|\s*$)'
                    match = re.match(let_pattern, statement, re.DOTALL)
                    if match:
                        var_name = match.group(1).strip()
                        expression = match.group(2).strip().rstrip(',')
                        let_expressions.append(LetExpressionNode(var_name, expression))

        # Now extract the query part (after let expressions)
        query_part_pattern = r'(\[[^\]]+\]\s+\w+(?:\s+with|\s+without).*)'
        query_part_match = re.search(query_part_pattern, text, re.IGNORECASE | re.DOTALL)

        if query_part_match:
            query_text = query_part_match.group(1)
            query_node = self._parse_query_with_relationships(query_text)

            # Add let expressions to the query node
            for let_expr in let_expressions:
                query_node.add_let_expression(let_expr)

            return query_node
        else:
            # Fallback: create a simple container with let expressions
            container = QueryWithRelationshipsNode("[Query]", "Q")
            for let_expr in let_expressions:
                container.add_let_expression(let_expr)
            return container

    def _parse_from_query(self, text: str) -> MultiSourceQueryNode:
        """Parse a multi-source from query."""
        logger.debug(f"Parsing from query: {text}")

        query_node = MultiSourceQueryNode()

        # Extract from clause: from (expr1) alias1, (expr2) alias2
        from_pattern = r'from\s+(.+?)(?=\s+where|\s+return|\s+sort|\s+aggregate|$)'
        from_match = re.search(from_pattern, text, re.IGNORECASE | re.DOTALL)

        if from_match:
            sources_text = from_match.group(1).strip()

            # Split by commas that are not inside parentheses
            sources = self._split_preserving_parentheses(sources_text, ',')

            for source in sources:
                source = source.strip()
                # Parse (expression) alias
                source_pattern = r'\(([^)]+)\)\s+(\w+)'
                match = re.match(source_pattern, source)
                if match:
                    expression_text = match.group(1)
                    alias = match.group(2)
                    query_node.add_source(expression_text, alias)

        # Parse optional clauses
        self._parse_optional_clauses(text, query_node)

        return query_node

    def _parse_aggregate_query(self, text: str) -> QueryExpressionNode:
        """Parse a query with aggregate clause."""
        logger.debug(f"Parsing aggregate query: {text}")

        # Pattern: (expression) alias aggregate [distinct|all] result_alias [starting expr]: agg_expr
        query_pattern = r'\(([^)]+)\)\s+(\w+)\s+aggregate\s+((?:distinct\s+|all\s+)?)'
        aggregate_pattern = r'aggregate\s+((?:distinct\s+|all\s+)?)(\w+)(?:\s+starting\s+([^:]+))?\s*:\s*(.+?)(?=\s+where|\s+return|\s+sort|$)'

        query_match = re.match(query_pattern, text, re.IGNORECASE)
        if not query_match:
            raise ValueError(f"Invalid aggregate query format: {text}")

        source_expr = query_match.group(1)
        source_alias = query_match.group(2)

        # Create basic query node
        query_node = QueryExpressionNode(source_expr, source_alias)

        # Parse aggregate clause
        agg_match = re.search(aggregate_pattern, text, re.IGNORECASE | re.DOTALL)
        if agg_match:
            qualifier = agg_match.group(1).strip()
            result_alias = agg_match.group(2)
            starting_expr = agg_match.group(3)
            agg_expr = agg_match.group(4)

            is_distinct = 'distinct' in qualifier.lower()
            is_all = 'all' in qualifier.lower()

            aggregate_clause = AggregateClauseNode(
                result_alias=result_alias,
                starting_expression=starting_expr.strip() if starting_expr else None,
                aggregate_expression=agg_expr.strip(),
                is_distinct=is_distinct,
                is_all=is_all
            )
            query_node.set_aggregate_clause(aggregate_clause)

        return query_node

    def _parse_aliased_query(self, text: str) -> QueryExpressionNode:
        """Parse a query with alias and optional clauses."""
        logger.debug(f"Parsing aliased query: {text}")

        # Pattern: (expression) alias [optional clauses]
        query_pattern = r'\(([^)]+)\)\s+(\w+)'
        match = re.match(query_pattern, text)

        if not match:
            raise ValueError(f"Invalid aliased query format: {text}")

        source_expr = match.group(1)
        alias = match.group(2)

        query_node = QueryExpressionNode(source_expr, alias)

        # Parse optional clauses
        self._parse_optional_clauses(text, query_node)

        return query_node

    def _parse_collection_operation_query(self, text: str) -> Any:
        """Parse a query with collection operations (union, intersect, except, flatten, etc.)."""
        logger.debug(f"Parsing collection operation query: {text}")

        # Determine the type of collection operation
        operation_type = self._detect_collection_operation_type(text)

        # Handle different types of collection operations
        if operation_type in ['union', 'intersect', 'except']:
            # Binary set operations
            return self.parse_set_operation(text)
        elif operation_type in ['flatten', 'distinct']:
            # Unary collection operations
            return self.parse_collection_operation(text)
        elif 'by(' in text.lower():
            # Predicate-based operations (unionBy, distinctBy, etc.)
            return self.parse_collection_operation(text)
        elif any(keyword in text.lower() for keyword in ['where', 'let', 'sort', 'return']):
            # Complex collection query with multiple clauses
            return self.parse_collection_query(text)
        else:
            # Simple collection operation
            return self.parse_collection_operation(text)

    def _parse_optional_clauses(self, text: str, query_node):
        """Parse optional clauses (where, return, sort) for query nodes."""

        # Parse where clause
        where_match = re.search(r'\bwhere\s+(.+?)(?=\s+return|\s+sort|\s+aggregate|$)', text, re.IGNORECASE | re.DOTALL)
        if where_match:
            query_node.set_where_condition(where_match.group(1).strip())

        # Parse return clause
        return_match = re.search(r'\breturn\s+(.+?)(?=\s+sort|\s+aggregate|$)', text, re.IGNORECASE | re.DOTALL)
        if return_match:
            query_node.set_return_expression(return_match.group(1).strip())

        # Parse sort clause
        sort_match = re.search(r'\bsort\s+(.+?)(?=\s+aggregate|$)', text, re.IGNORECASE | re.DOTALL)
        if sort_match:
            sort_text = sort_match.group(1).strip()
            sort_clause = self._parse_sort_clause(sort_text)
            query_node.set_sort_clause(sort_clause)

    def _parse_sort_clause(self, sort_text: str) -> SortClauseNode:
        """Parse sort clause text into SortClauseNode."""

        # Check for 'by' keyword
        if sort_text.lower().startswith('by '):
            # Pattern: by expression direction
            by_pattern = r'by\s+(.+?)\s+(asc|desc|ascending|descending)$'
            match = re.match(by_pattern, sort_text, re.IGNORECASE)
            if match:
                sort_expr = match.group(1).strip()
                direction = match.group(2)
                return SortClauseNode(sort_expr, direction)
            else:
                # Just by expression (default ascending)
                expr_pattern = r'by\s+(.+)$'
                match = re.match(expr_pattern, sort_text, re.IGNORECASE)
                if match:
                    sort_expr = match.group(1).strip()
                    return SortClauseNode(sort_expr, "asc")
        else:
            # Just direction (asc/desc/ascending/descending)
            if sort_text.lower() in ['asc', 'desc', 'ascending', 'descending']:
                return SortClauseNode(None, sort_text)

        # Fallback
        return SortClauseNode(None, sort_text)

    def _split_preserving_parentheses(self, text: str, delimiter: str) -> List[str]:
        """Split text by delimiter while preserving content inside parentheses."""
        parts = []
        current_part = ""
        paren_count = 0

        for char in text:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == delimiter and paren_count == 0:
                parts.append(current_part)
                current_part = ""
                continue

            current_part += char

        if current_part:
            parts.append(current_part)

        return parts

    def _contains_complex_condition(self, condition_text: str) -> bool:
        """Check if a condition contains complex patterns requiring structured parsing."""
        # Check for reference patterns
        reference_pattern = r'\w+\.\w+\s+references\s+\w+\.\w+'
        if re.search(reference_pattern, condition_text, re.IGNORECASE):
            return True

        # Check for temporal conditions
        temporal_pattern = r'\w+\.\w+\s+(during|overlaps|before|after|meets|starts|ends)\s+'
        if re.search(temporal_pattern, condition_text, re.IGNORECASE):
            return True

        # Check for logical operators
        logical_pattern = r'\b(and|or|not)\b'
        if re.search(logical_pattern, condition_text, re.IGNORECASE):
            return True

        return False

    def _parse_complex_condition(self, condition_text: str) -> Dict[str, Any]:
        """Parse complex condition into structured components."""
        result = {
            'reference_patterns': [],
            'temporal_conditions': [],
            'logical_operators': [],
            'other_conditions': []
        }

        # Extract reference patterns
        reference_pattern = r'(\w+)\.(\w+)\s+references\s+(\w+)\.(\w+)'
        for match in re.finditer(reference_pattern, condition_text, re.IGNORECASE):
            result['reference_patterns'].append({
                'source_alias': match.group(1),
                'source_field': match.group(2),
                'target_alias': match.group(3),
                'target_field': match.group(4),
                'full_text': match.group(0)
            })

        # Extract temporal conditions (both quoted and unquoted operands)
        temporal_pattern_quoted = r'(\w+)\.(\w+)\s+(during|overlaps|before|after|meets|starts|ends)\s+"([^"]+)"'
        for match in re.finditer(temporal_pattern_quoted, condition_text, re.IGNORECASE):
            result['temporal_conditions'].append({
                'alias': match.group(1),
                'field': match.group(2),
                'operator': match.group(3),
                'operand': match.group(4),
                'full_text': match.group(0)
            })

        temporal_pattern_unquoted = r'(\w+)\.(\w+)\s+(during|overlaps|before|after|meets|starts|ends)\s+(\w+(?:\.\w+)*)'
        for match in re.finditer(temporal_pattern_unquoted, condition_text, re.IGNORECASE):
            # Only add if not already captured by quoted pattern
            if not any(t['alias'] == match.group(1) and t['field'] == match.group(2)
                      and t['operator'] == match.group(3) for t in result['temporal_conditions']):
                result['temporal_conditions'].append({
                    'alias': match.group(1),
                    'field': match.group(2),
                    'operator': match.group(3),
                    'operand': match.group(4),
                    'full_text': match.group(0)
                })

        # Extract logical operators
        logical_pattern = r'\b(and|or|not)\b'
        result['logical_operators'] = re.findall(logical_pattern, condition_text, re.IGNORECASE)

        return result

    def _contains_temporal_conditions(self, condition_text: str) -> bool:
        """Check if condition contains temporal operators."""
        temporal_pattern = r'\w+\.\w+\s+(during|overlaps|before|after|meets|starts|ends)\s+'
        return bool(re.search(temporal_pattern, condition_text, re.IGNORECASE))

    def _extract_temporal_conditions(self, condition_text: str) -> List[Dict[str, str]]:
        """Extract temporal conditions from condition text."""
        temporal_conditions = []

        # Pattern 1: quoted operands
        temporal_pattern_quoted = r'(\w+)\.(\w+)\s+(during|overlaps|before|after|meets|starts|ends)\s+"([^"]+)"'
        for match in re.finditer(temporal_pattern_quoted, condition_text, re.IGNORECASE):
            temporal_conditions.append({
                'alias': match.group(1),
                'field': match.group(2),
                'operator': match.group(3),
                'operand': match.group(4)
            })

        # Pattern 2: unquoted operands
        temporal_pattern_unquoted = r'(\w+)\.(\w+)\s+(during|overlaps|before|after|meets|starts|ends)\s+(\w+(?:\.\w+)*)'
        for match in re.finditer(temporal_pattern_unquoted, condition_text, re.IGNORECASE):
            # Only add if not already captured by quoted pattern
            if not any(t['alias'] == match.group(1) and t['field'] == match.group(2)
                      and t['operator'] == match.group(3) for t in temporal_conditions):
                temporal_conditions.append({
                    'alias': match.group(1),
                    'field': match.group(2),
                    'operator': match.group(3),
                    'operand': match.group(4)
                })

        return temporal_conditions

    def _contains_reference_patterns(self, condition_text: str) -> bool:
        """Check if condition contains reference patterns."""
        reference_pattern = r'\w+\.\w+\s+references\s+\w+\.\w+'
        return bool(re.search(reference_pattern, condition_text, re.IGNORECASE))

    def _extract_reference_patterns(self, condition_text: str) -> List[Dict[str, str]]:
        """Extract reference patterns from condition text."""
        reference_patterns = []
        reference_pattern = r'(\w+)\.(\w+)\s+references\s+(\w+)\.(\w+)'

        for match in re.finditer(reference_pattern, condition_text, re.IGNORECASE):
            reference_patterns.append({
                'source_alias': match.group(1),
                'source_field': match.group(2),
                'target_alias': match.group(3),
                'target_field': match.group(4)
            })

        return reference_patterns
    
    def parse_with_clause(self, clause_text: str) -> WithClauseNode:
        """
        Parse a single with/without clause.
        
        Args:
            clause_text: Text of the with/without clause
            
        Returns:
            WithClauseNode representing the parsed clause
        """
        # Pattern: (with|without) [ResourceType] Alias such that condition
        pattern = r'\b(with|without)\s+(\[[^\]]+\])\s+(\w+)\s+such\s+that\s+(.+)'
        match = re.match(pattern, clause_text.strip(), re.IGNORECASE)
        
        if not match:
            raise ValueError(f"Invalid with/without clause format: {clause_text}")
        
        clause_type = match.group(1).lower()
        resource_query = match.group(2)
        alias = match.group(3)
        condition = match.group(4)
        
        is_without = clause_type == 'without'
        return WithClauseNode(alias, resource_query, condition, is_without)
    
    def parse_let_expression(self, let_text: str) -> LetExpressionNode:
        """
        Parse a single let expression.
        
        Args:
            let_text: Text of the let expression
            
        Returns:
            LetExpressionNode representing the parsed expression
        """
        # Pattern: let variableName: expression
        pattern = r'\blet\s+(\w+)\s*:\s*(.+)'
        match = re.match(pattern, let_text.strip(), re.IGNORECASE)
        
        if not match:
            raise ValueError(f"Invalid let expression format: {let_text}")
        
        variable_name = match.group(1)
        expression = match.group(2)
        
        return LetExpressionNode(variable_name, expression)

    def parse_collection_operation(self, operation_text: str) -> CollectionOperationNode:
        """
        Parse collection operation expression.

        Args:
            operation_text: Collection operation text (e.g., "flatten()", "union({1,2,3})")

        Returns:
            CollectionOperationNode representing the parsed operation
        """
        # Handle predicate operations like unionBy(), distinctBy() (check first due to more specific pattern)
        predicate_pattern = r'(.*)\.(\w+[Bb]y)\(\s*(.+?)\s*,\s*(.+?)\s*\)$'
        predicate_match = re.match(predicate_pattern, operation_text.strip(), re.IGNORECASE)
        if predicate_match:
            base_expr = predicate_match.group(1).strip()
            operation_type = predicate_match.group(2).lower()
            operand_expr = predicate_match.group(3).strip()
            predicate_expr = predicate_match.group(4).strip()
            return CollectionOperationNode(operation_type, base_expr, operand_expr, predicate_expr)

        # Handle single-argument predicate operations like distinctBy(x -> x)
        single_predicate_pattern = r'(.*)\.(\w+[Bb]y)\(\s*(.*)\s*\)$'
        single_predicate_match = re.match(single_predicate_pattern, operation_text.strip(), re.IGNORECASE)
        if single_predicate_match:
            base_expr = single_predicate_match.group(1).strip()
            operation_type = single_predicate_match.group(2).lower()
            predicate_expr = single_predicate_match.group(3).strip()
            return CollectionOperationNode(operation_type, base_expr, None, predicate_expr)

        # Handle unary operations like flatten(), distinct()
        unary_pattern = r'(.*)\.(\w+)\(\s*\)$'
        unary_match = re.match(unary_pattern, operation_text.strip())
        if unary_match:
            base_expr = unary_match.group(1).strip()
            operation_type = unary_match.group(2).lower()
            return CollectionOperationNode(operation_type, base_expr)

        # Handle binary operations like union, intersect, except
        binary_pattern = r'(.+?)\s+(union|intersect|except)\s+(.+)'
        binary_match = re.match(binary_pattern, operation_text.strip(), re.IGNORECASE)
        if binary_match:
            left_expr = binary_match.group(1).strip()
            operation_type = binary_match.group(2).lower()
            right_expr = binary_match.group(3).strip()
            return CollectionOperationNode(operation_type, left_expr, right_expr)

        # Fallback: treat as unary operation
        return CollectionOperationNode("unknown", operation_text.strip())

    def parse_set_operation(self, operation_text: str) -> SetOperationNode:
        """
        Parse set operation expression.

        Args:
            operation_text: Set operation text (e.g., "A union B", "A intersectBy(B, predicate)")

        Returns:
            SetOperationNode representing the parsed operation
        """
        # Handle operations with custom equality predicates
        predicate_pattern = r'(.+?)\s+(\w+by)\(\s*(.+?)\s*,\s*(.+?)\s*\)'
        predicate_match = re.match(predicate_pattern, operation_text.strip(), re.IGNORECASE)
        if predicate_match:
            left_expr = predicate_match.group(1).strip()
            operation_type = predicate_match.group(2).lower().replace('by', '')
            right_expr = predicate_match.group(3).strip()
            predicate_expr = predicate_match.group(4).strip()
            return SetOperationNode(left_expr, operation_type, right_expr, predicate_expr)

        # Handle standard binary set operations
        binary_pattern = r'(.+?)\s+(union|intersect|except)\s+(.+)'
        binary_match = re.match(binary_pattern, operation_text.strip(), re.IGNORECASE)
        if binary_match:
            left_expr = binary_match.group(1).strip()
            operator = binary_match.group(2).lower()
            right_expr = binary_match.group(3).strip()
            return SetOperationNode(left_expr, operator, right_expr)

        raise ValueError(f"Unable to parse set operation: {operation_text}")

    def parse_collection_query(self, query_text: str) -> CollectionQueryNode:
        """
        Parse complex collection query with multiple clauses.

        Args:
            query_text: Collection query text with where, let, sort, etc.

        Returns:
            CollectionQueryNode representing the parsed query
        """
        # Extract source expression and alias
        lines = [line.strip() for line in query_text.strip().split('\n') if line.strip()]
        if not lines:
            raise ValueError("Empty collection query")

        first_line = lines[0]
        source_match = re.match(r'(.+?)\s+(\w+)$', first_line)
        if source_match:
            source_expr = source_match.group(1).strip()
            alias = source_match.group(2).strip()
        else:
            source_expr = first_line
            alias = None

        query_node = CollectionQueryNode(source_expr, alias)

        # Process remaining lines for clauses
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue

            # Parse let expressions
            if line.lower().startswith('let '):
                let_expr = self.parse_let_expression(line)
                query_node.add_let_expression(let_expr)

            # Parse where conditions
            elif line.lower().startswith('where '):
                condition = line[6:].strip()  # Remove 'where '
                query_node.add_where_condition(condition)

            # Parse return expressions
            elif line.lower().startswith('return '):
                return_expr = line[7:].strip()  # Remove 'return '
                query_node.set_return_expression(return_expr)

            # Parse sort clauses
            elif line.lower().startswith('sort ') or line.lower().startswith('by '):
                sort_clause = self._parse_sort_clause(line)
                query_node.add_sort_clause(sort_clause)

            # Parse aggregation clauses
            elif 'aggregate' in line.lower():
                # This would be handled by existing aggregate parsing
                pass

            # Parse collection operations
            elif any(op in line.lower() for op in ['union', 'intersect', 'except', 'flatten', 'distinct']):
                collection_op = self.parse_collection_operation(line)
                query_node.add_collection_operation(collection_op)

        return query_node

    def _has_collection_operations(self, cql_text: str) -> bool:
        """Check if CQL text contains collection operations."""
        collection_keywords = ['union', 'intersect', 'except', 'flatten', 'distinct']
        text_lower = cql_text.lower()
        return any(keyword in text_lower for keyword in collection_keywords)

    def _detect_collection_operation_type(self, cql_text: str) -> str:
        """Detect the type of collection operation in CQL text."""
        text_lower = cql_text.lower()

        if 'flatten(' in text_lower:
            return 'flatten'
        elif 'distinct(' in text_lower or 'distinctby(' in text_lower:
            return 'distinct'
        elif 'union' in text_lower:
            return 'union'
        elif 'intersect' in text_lower:
            return 'intersect'
        elif 'except' in text_lower:
            return 'except'
        else:
            return 'unknown'

    def get_supported_constructs(self) -> List[str]:
        """Get list of supported advanced CQL constructs."""
        return [
            "with clauses - Complex resource relationships",
            "without clauses - Exclusion relationships",
            "such that conditions - Relationship conditions",
            "let expressions - Variable definitions (basic support)",
            "Enhanced resource queries - Multi-resource support",
            "Collection operations - union, intersect, except",
            "Set operations with predicates - unionBy, intersectBy, exceptBy",
            "Collection functions - flatten, distinct, sort",
            "Collection queries - Complex multi-clause collection operations"
        ]


# Example usage and testing functions
def test_advanced_parser():
    """Test the advanced CQL parser with sample expressions."""
    parser = AdvancedCQLParser()

    test_cases = [
        # Original with/without/let tests
        {
            "name": "Diabetes with HbA1c",
            "cql": """
            [Condition: "Diabetes mellitus"] Diabetes
              with [Observation: "HbA1c laboratory test"] HbA1c
                such that HbA1c.subject references Diabetes.subject
                  and HbA1c.effective during "Measurement Period"
                  and HbA1c.value as Quantity > 9.0 '%'
            """,
            "description": "Diabetes patients with recent poor HbA1c results"
        },
        {
            "name": "Medication adherence without",
            "cql": """
            [Condition: "Hypertension"] HTN
              without [MedicationRequest: "ACE inhibitors"] Meds
                such that Meds.subject references HTN.subject
                  and Meds.authoredOn during "Measurement Period"
            """,
            "description": "Hypertension patients without ACE inhibitor prescriptions"
        },
        {
            "name": "Let expression example",
            "cql": """
            let ageInYears: AgeInYears(),
                riskScore: CalculateRiskScore()
            define "High Risk Patients": ...
            """,
            "description": "Query with variable definitions using let expressions"
        },

        # New query expression tests (from official CQL tests)
        {
            "name": "Multi-source query",
            "cql": "from ({2, 3}) A, ({5, 6}) B",
            "description": "Multi-source query with Cartesian product"
        },
        {
            "name": "Simple aliased query",
            "cql": "(4) l",
            "description": "Simple query with alias"
        },
        {
            "name": "Aliased query with return",
            "cql": "(4) l return 'Hello World'",
            "description": "Query with alias and return expression"
        },
        {
            "name": "Integer sort descending",
            "cql": "({1, 2, 3}) l sort desc",
            "description": "Collection query with descending sort"
        },
        {
            "name": "Integer sort ascending",
            "cql": "({1, 3, 2}) l sort ascending",
            "description": "Collection query with ascending sort"
        },
        {
            "name": "Aggregate multiply",
            "cql": "({1, 2, 3, 3, 4}) L aggregate A starting 1: A * L",
            "description": "Aggregate query with starting value"
        },
        {
            "name": "Aggregate distinct",
            "cql": "({1, 2, 3, 3, 4}) L aggregate distinct A starting 1: A * L",
            "description": "Distinct aggregate query"
        },
        {
            "name": "Multi-source aggregate",
            "cql": "from ({1, 2, 3}) B, (4) C aggregate A : A + B + C",
            "description": "Multi-source query with aggregate"
        }
    ]
    
    print("🧪 Testing Advanced CQL Parser")
    print("=" * 60)
    
    for test_case in test_cases:
        print(f"\n📋 Test: {test_case['name']}")
        print(f"Description: {test_case['description']}")
        print("CQL:")
        for line in test_case['cql'].strip().split('\n'):
            print(f"  {line.strip()}")
        
        try:
            parsed_result = parser.parse_advanced_cql(test_case['cql'])
            print(f"✅ Parsing: Success")
            print(f"Result Type: {type(parsed_result).__name__}")
            
            if hasattr(parsed_result, '__str__'):
                print("Parsed Structure:")
                for line in str(parsed_result).split('\n'):
                    print(f"  {line}")
        
        except Exception as e:
            print(f"❌ Parsing: Failed - {e}")
    
    print(f"\n🎯 Supported Constructs:")
    for construct in parser.get_supported_constructs():
        print(f"  • {construct}")


if __name__ == "__main__":
    test_advanced_parser()