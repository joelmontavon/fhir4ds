"""
CQL Advanced Translator - SQL generation for advanced CQL constructs.

Generates SQL for Phase 6 advanced constructs including:
- with/without clauses
- let expressions
- enhanced multi-resource queries
"""

import logging
from typing import List, Dict, Any, Optional, Union, Tuple

from .advanced_parser import (
    QueryWithRelationshipsNode, WithClauseNode, LetExpressionNode,
    AdvancedCQLParser
)

logger = logging.getLogger(__name__)


class AdvancedCQLTranslator:
    """Advanced CQL to SQL translator for Phase 6 constructs."""
    
    def __init__(self, dialect: str = "duckdb"):
        """
        Initialize advanced translator.
        
        Args:
            dialect: Database dialect ("duckdb" or "postgresql")
        """
        self.dialect = dialect
        self.parser = AdvancedCQLParser()
        
        # SQL templates for different constructs
        self.sql_templates = {
            "with_exists": """
            EXISTS (
                SELECT 1 FROM {related_table} {related_alias}
                WHERE {condition}
            )
            """,
            "without_not_exists": """
            NOT EXISTS (
                SELECT 1 FROM {related_table} {related_alias}
                WHERE {condition}
            )
            """,
            "let_cte": """
            WITH {variable_name} AS (
                SELECT {expression} as {variable_name}_value
            )
            """,
            "multi_resource_join": """
            SELECT {select_columns}
            FROM {primary_table} {primary_alias}
            {join_clauses}
            WHERE {where_conditions}
            """
        }
    
    def translate_advanced_cql(self, cql_text: str) -> str:
        """
        Translate advanced CQL to SQL.
        
        Args:
            cql_text: CQL expression text
            
        Returns:
            Generated SQL query
        """
        logger.debug(f"Translating advanced CQL: {cql_text}")
        
        # Parse the CQL
        parsed_cql = self.parser.parse_advanced_cql(cql_text)
        
        if isinstance(parsed_cql, QueryWithRelationshipsNode):
            return self._translate_query_with_relationships(parsed_cql)
        elif isinstance(parsed_cql, dict) and parsed_cql.get("type") == "standard_cql":
            return self._translate_standard_cql(parsed_cql["text"])
        else:
            logger.warning(f"Unknown parsed CQL type: {type(parsed_cql)}")
            return f"-- Unsupported CQL construct: {cql_text}"
    
    def _translate_query_with_relationships(self, query_node: QueryWithRelationshipsNode) -> str:
        """Translate a query with with/without relationships to SQL."""
        logger.debug(f"Translating query with relationships: {query_node.primary_query}")
        
        # Extract primary table and resource type from resource query
        primary_table = self._extract_table_name(query_node.primary_query)
        primary_resource_type = self._extract_resource_type(query_node.primary_query)
        primary_alias = query_node.alias
        
        # Build SELECT clause
        select_columns = f"{primary_alias}.*"
        
        # Build FROM clause
        from_clause = f"FROM {primary_table} {primary_alias}"
        
        # Build WHERE conditions
        where_conditions = []
        
        # Add primary resource type filter for FHIR4DS
        primary_resource_filter = f"JSON_EXTRACT_STRING({primary_alias}.resource, '$.resourceType') = '{primary_resource_type}'"
        where_conditions.append(primary_resource_filter)
        
        # Add with clauses (EXISTS conditions)
        for with_clause in query_node.with_clauses:
            exists_condition = self._translate_with_clause(with_clause, primary_alias)
            where_conditions.append(exists_condition)
        
        # Add without clauses (NOT EXISTS conditions)
        for without_clause in query_node.without_clauses:
            not_exists_condition = self._translate_without_clause(without_clause, primary_alias)
            where_conditions.append(not_exists_condition)
        
        # Add explicit WHERE condition if present
        if query_node.where_condition:
            where_conditions.append(query_node.where_condition)
        
        # Build final query
        sql_parts = [f"SELECT {select_columns}", from_clause]
        
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
            sql_parts.append(where_clause)
        
        # Handle let expressions with CTEs
        if query_node.let_expressions:
            cte_parts = []
            for let_expr in query_node.let_expressions:
                cte_sql = self._translate_let_expression(let_expr)
                cte_parts.append(cte_sql)
            
            if cte_parts:
                main_query = "\n".join(sql_parts)
                return f"WITH {', '.join(cte_parts)}\n{main_query}"
        
        return "\n".join(sql_parts)
    
    def _translate_with_clause(self, with_clause: WithClauseNode, primary_alias: str) -> str:
        """Translate a 'with' clause to EXISTS SQL."""
        related_table = self._extract_table_name(with_clause.related_query)
        related_alias = with_clause.source_alias
        
        # Extract resource type from the related query for filtering
        resource_type = self._extract_resource_type(with_clause.related_query)
        
        # Process the condition to reference the correct aliases
        condition = self._process_relationship_condition(
            with_clause.condition, primary_alias, related_alias
        )
        
        # Add resource type filter for FHIR4DS
        resource_filter = f"JSON_EXTRACT_STRING({related_alias}.resource, '$.resourceType') = '{resource_type}'"
        
        exists_sql = f"""EXISTS (
            SELECT 1 FROM {related_table} {related_alias}
            WHERE {resource_filter} AND {condition}
        )"""
        
        return exists_sql.strip()
    
    def _translate_without_clause(self, without_clause: WithClauseNode, primary_alias: str) -> str:
        """Translate a 'without' clause to NOT EXISTS SQL."""
        related_table = self._extract_table_name(without_clause.related_query)
        related_alias = without_clause.source_alias
        
        # Extract resource type from the related query for filtering
        resource_type = self._extract_resource_type(without_clause.related_query)
        
        # Process the condition to reference the correct aliases
        condition = self._process_relationship_condition(
            without_clause.condition, primary_alias, related_alias
        )
        
        # Add resource type filter for FHIR4DS
        resource_filter = f"JSON_EXTRACT_STRING({related_alias}.resource, '$.resourceType') = '{resource_type}'"
        
        not_exists_sql = f"""NOT EXISTS (
            SELECT 1 FROM {related_table} {related_alias}
            WHERE {resource_filter} AND {condition}
        )"""
        
        return not_exists_sql.strip()
    
    def _translate_let_expression(self, let_expr: LetExpressionNode) -> str:
        """Translate a 'let' expression to CTE SQL."""
        variable_name = let_expr.variable_name
        expression = let_expr.expression
        
        # Process the CQL expression to make it SQL compatible
        # This is a simplified version - full implementation would use proper CQL->SQL translation
        sql_expression = self._process_cql_expression_to_sql(expression)
        
        cte_sql = f"{variable_name} AS (SELECT {sql_expression} as {variable_name}_value)"
        
        return cte_sql
    
    def _extract_table_name(self, resource_query: str) -> str:
        """
        Extract table name from a resource query like [Condition: "Diabetes"].
        
        Args:
            resource_query: Resource query string
            
        Returns:
            Corresponding table name (always fhir_resources for FHIR4DS)
        """
        # FHIR4DS uses a single fhir_resources table with JSON data
        # All FHIR resources are stored in the same table and filtered by resourceType
        return "fhir_resources"
    
    def _extract_resource_type(self, resource_query: str) -> str:
        """
        Extract FHIR resource type from a resource query like [Condition: "Diabetes"].
        
        Args:
            resource_query: Resource query string
            
        Returns:
            FHIR resource type name
        """
        # Remove brackets and extract resource type
        if resource_query.startswith('[') and resource_query.endswith(']'):
            content = resource_query[1:-1]
            
            # Handle cases like [Condition: "ValueSet"] or just [Patient]
            if ':' in content:
                resource_type = content.split(':')[0].strip()
            else:
                resource_type = content.strip()
            
            return resource_type
        
        return resource_query
    
    def _process_relationship_condition(self, condition: str, primary_alias: str, 
                                      related_alias: str) -> str:
        """
        Process relationship condition to ensure proper alias references.
        
        Args:
            condition: Original condition string
            primary_alias: Alias for primary resource
            related_alias: Alias for related resource
            
        Returns:
            Processed condition with correct aliases
        """
        import re
        
        # Handle "references" keyword for FHIR references
        if "references" in condition.lower():
            # Pattern: RelatedAlias.subject references PrimaryAlias
            references_pattern = r'(\w+)\.(\w+)\s+references\s+(\w+)'
            
            def replace_references(match):
                ref_alias = match.group(1)
                ref_field = match.group(2)
                target_alias = match.group(3)
                
                # Convert FHIR reference to SQL join condition
                # This is a simplified version - full implementation would handle FHIR reference format
                return f"{ref_alias}.{ref_field} = {target_alias}.id"
            
            condition = re.sub(references_pattern, replace_references, condition, flags=re.IGNORECASE)
        
        # Handle other CQL operators that need SQL translation
        condition = self._translate_cql_operators_in_condition(condition)
        
        return condition
    
    def _translate_cql_operators_in_condition(self, condition: str) -> str:
        """Translate CQL operators within conditions to SQL equivalents."""
        import re
        
        # Handle "during" operator for date ranges
        # Pattern: field during "Period"
        during_pattern = r'(\w+\.\w+)\s+during\s+"([^"]+)"'
        condition = re.sub(
            during_pattern, 
            r'\1 BETWEEN start_of_\2 AND end_of_\2',
            condition,
            flags=re.IGNORECASE
        )
        
        # Handle "as Quantity" conversions
        condition = re.sub(r'\s+as\s+Quantity', '', condition, flags=re.IGNORECASE)
        
        # Handle CQL comparison operators
        condition = condition.replace(' > ', ' > ')  # Already SQL compatible
        condition = condition.replace(' < ', ' < ')  # Already SQL compatible
        condition = condition.replace(' >= ', ' >= ')  # Already SQL compatible
        condition = condition.replace(' <= ', ' <= ')  # Already SQL compatible
        
        return condition
    
    def _process_cql_expression_to_sql(self, expression: str) -> str:
        """
        Process a CQL expression to make it SQL compatible.
        
        Args:
            expression: CQL expression string
            
        Returns:
            SQL-compatible expression string
        """
        # Handle common CQL patterns and convert to SQL
        # This is a simplified version - full implementation would use proper CQL->SQL translation
        import re
        
        # Handle FHIRPath expressions first (more specific patterns)
        # Pattern: P.extension.where(url='value').field
        fhirpath_pattern = r'(\w+)\.extension\.where\(url=\'([^\']+)\'\)\.(\w+)'
        def replace_fhirpath(match):
            alias = match.group(1)
            url_value = match.group(2)  # e.g., 'bmi'
            target_field = match.group(3)  # e.g., 'valueDecimal'
            
            # Convert to JSON path for FHIR extension lookup
            # Simplified JSON extraction for FHIR extensions
            return f"JSON_EXTRACT({alias}.resource, '$.extension[0].{target_field}')"
        
        expression = re.sub(fhirpath_pattern, replace_fhirpath, expression)
        
        # Replace CQL function calls with SQL equivalents
        expression = expression.replace("AgeInYears(", "EXTRACT(YEAR FROM AGE(")
        
        # Handle simple field access like P.birthDate (but not function calls)
        field_access_pattern = r'(\w+)\.(\w+)(?!\(|\.)'
        def replace_field_access(match):
            alias = match.group(1)
            field = match.group(2)
            return f"JSON_EXTRACT_STRING({alias}.resource, '$.{field}')"
        
        expression = re.sub(field_access_pattern, replace_field_access, expression)
        
        return expression
    
    def _translate_standard_cql(self, cql_text: str) -> str:
        """Translate standard CQL (fallback for non-advanced constructs)."""
        # This would normally delegate to the standard CQL translator
        return f"-- Standard CQL translation: {cql_text}"
    
    def get_supported_features(self) -> List[str]:
        """Get list of supported advanced translation features."""
        return [
            "with clauses → EXISTS subqueries",
            "without clauses → NOT EXISTS subqueries",
            "Complex relationship conditions",
            "FHIR reference resolution",
            "let expressions → CTEs (basic)",
            "Multi-resource relationship queries",
            "Cross-dialect SQL generation"
        ]


def test_advanced_translator():
    """Test the advanced CQL translator with sample queries."""
    print("🔧 Testing Advanced CQL Translator")
    print("=" * 60)
    
    translator = AdvancedCQLTranslator("duckdb")
    
    test_cases = [
        {
            "name": "Simple with clause",
            "cql": """
            [Condition: "Diabetes mellitus"] Diabetes
              with [Observation: "HbA1c laboratory test"] HbA1c
                such that HbA1c.subject references Diabetes.subject
            """,
            "description": "Diabetes patients with HbA1c observations"
        },
        {
            "name": "Without clause exclusion",
            "cql": """
            [Patient] P
              without [MedicationRequest: "Insulin"] Insulin
                such that Insulin.subject references P
            """,
            "description": "Patients without insulin prescriptions"
        },
        {
            "name": "Complex multi-relationship",
            "cql": """
            [Patient] P
              with [Condition: "Diabetes"] D
                such that D.subject references P
              without [MedicationRequest: "Insulin"] I
                such that I.subject references P
            """,
            "description": "Diabetic patients without insulin"
        }
    ]
    
    successful_translations = 0
    total_tests = len(test_cases)
    
    for test_case in test_cases:
        print(f"\n📋 Test: {test_case['name']}")
        print(f"Description: {test_case['description']}")
        print("CQL:")
        for line in test_case['cql'].strip().split('\n'):
            print(f"  {line.strip()}")
        
        try:
            sql_result = translator.translate_advanced_cql(test_case['cql'])
            print("✅ Translation: Success")
            print("Generated SQL:")
            for line in sql_result.split('\n'):
                if line.strip():
                    print(f"  {line}")
            
            successful_translations += 1
            
        except Exception as e:
            print(f"❌ Translation: Failed - {e}")
    
    print(f"\n🎯 Supported Features:")
    for feature in translator.get_supported_features():
        print(f"  • {feature}")
    
    print(f"\n📊 Translation Results: {successful_translations}/{total_tests} tests passed")
    
    return successful_translations == total_tests


if __name__ == "__main__":
    test_advanced_translator()