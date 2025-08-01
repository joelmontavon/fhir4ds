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
            condition: The relationship condition (such that ...)
            is_without: True for 'without' clause, False for 'with' clause
        """
        self.source_alias = source_alias
        self.related_query = related_query
        self.condition = condition
        self.is_without = is_without
        self.clause_type = "without" if is_without else "with"
    
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
        """Add a 'with' clause to the query."""
        if with_clause.is_without:
            self.without_clauses.append(with_clause)
        else:
            self.with_clauses.append(with_clause)
    
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


class AdvancedCQLParser:
    """Enhanced CQL parser supporting advanced constructs."""
    
    def __init__(self, base_parser: CQLParser = None):
        """Initialize advanced parser with base CQL parser."""
        self.base_parser = base_parser  # Will be set when needed
        self.advanced_keywords = {
            'with', 'without', 'such', 'that', 'let', 'all', 'any', 
            'exists', 'related', 'references'
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
        
        # Check for advanced constructs
        has_with_without = self._has_with_without_clauses(normalized_text)
        has_let_expressions = self._has_let_expressions(normalized_text)
        
        if has_with_without and has_let_expressions:
            # Combined let expressions with with/without clauses
            return self._parse_combined_advanced_query(normalized_text)
        elif has_with_without:
            return self._parse_query_with_relationships(normalized_text)
        elif has_let_expressions:
            return self._parse_query_with_let_expressions(normalized_text)
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
        
        return bool(re.search(with_pattern, text, re.IGNORECASE) or 
                   re.search(without_pattern, text, re.IGNORECASE))
    
    def _has_let_expressions(self, text: str) -> bool:
        """Check if text contains let expressions."""
        let_pattern = r'\blet\s+\w+\s*:'
        return bool(re.search(let_pattern, text, re.IGNORECASE))
    
    def _validate_with_without_clauses(self, text: str):
        """Validate that with/without clauses have proper syntax."""
        import re
        
        # Check for with/without clauses without "such that"
        invalid_with_pattern = r'\b(with|without)\s+\[[^\]]+\]\s+\w+(?!\s+such\s+that)'
        invalid_matches = re.findall(invalid_with_pattern, text, re.IGNORECASE)
        
        if invalid_matches:
            raise SyntaxError(f"with/without clause must be followed by 'such that' condition")
        
        # Check for "such that" without proper condition
        empty_such_that_pattern = r'\bsuch\s+that\s*(?:\s*$|\s*(?:with|without|where|return))'
        empty_matches = re.search(empty_such_that_pattern, text, re.IGNORECASE)
        
        if empty_matches:
            raise SyntaxError("'such that' clause cannot be empty")
    
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
        
        # Pattern to match with/without clauses (improved for multiline)
        relationship_pattern = r'\b(with|without)\s+(\[[^\]]+\])\s+(\w+)\s+such\s+that\s+(.*?)(?=\s*(?:with|without|where|return|$))'
        
        for match in re.finditer(relationship_pattern, remaining_text, re.IGNORECASE | re.DOTALL):
            clause_type = match.group(1).lower()
            related_query = match.group(2)
            related_alias = match.group(3)
            condition = match.group(4).strip()
            
            # Clean up the condition (remove trailing keywords)
            condition = re.sub(r'\s+(with|without|where|return).*$', '', condition, flags=re.IGNORECASE)
            
            # Validate that condition is not empty
            if not condition.strip():
                raise ValueError(f"Empty condition in {clause_type} clause")
            
            is_without = clause_type == 'without'
            with_clause = WithClauseNode(related_alias, related_query, condition, is_without)
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
    
    def get_supported_constructs(self) -> List[str]:
        """Get list of supported advanced CQL constructs."""
        return [
            "with clauses - Complex resource relationships",
            "without clauses - Exclusion relationships", 
            "such that conditions - Relationship conditions",
            "let expressions - Variable definitions (basic support)",
            "Enhanced resource queries - Multi-resource support"
        ]


# Example usage and testing functions
def test_advanced_parser():
    """Test the advanced CQL parser with sample expressions."""
    parser = AdvancedCQLParser()
    
    test_cases = [
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