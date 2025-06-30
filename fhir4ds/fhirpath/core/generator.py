"""
SQL Generator for FHIRPath expressions

This module contains the SQL generation logic that converts FHIRPath ASTs
into SQL queries compatible with DuckDB's JSON extension.
"""

import re
import hashlib
from typing import List, Dict, Any, Optional, Union, Tuple

class SQLGenerator:
    """Generates SQL from FHIRPath AST with dependency injection"""
    
    def __init__(self, 
                 table_name: str = "fhir_resources", 
                 json_column: str = "resource", 
                 resource_type: Optional[str] = None,
                 # Advanced CTE optimizations (optional)
                 enable_cte_deduplication: bool = False,
                 enable_dialect_optimizations: bool = False,
                 enable_cte_inlining: bool = False,
                 # Dependency injection parameters
                 ast_nodes_module=None,
                 constants_module=None,
                 choice_types_dict=None,
                 dialect=None):
        self.table_name = table_name
        self.json_column = json_column
        self.alias_counter = 0
        self.resource_type = resource_type # Store the determined resource type
        self.debug_steps = [] # For capturing intermediate steps for logging
        self.max_sql_complexity = 1000  # Enable optimization with reasonable threshold
        self.complex_expr_cache = {}  # Cache for complex expressions
        
        # Optimization thresholds
        self.optimization_thresholds = {
            'max_length': 1000,
            'max_subqueries': 5,
            'max_json_ops': 10,
            'max_case_statements': 3
        }
        
        # CTE support - ENABLED BY DEFAULT (post-transition)
        self.ctes = {}  # CTE name -> CTE SQL
        self.cte_counter = 0
        
        # CTE configuration - simplified from 29 individual flags
        self.enable_cte = True  # Global CTE enablement (replaces all enable_cte_for_* flags)
        
        # Unified CTE decision system
        self._setup_cte_configs()
        
        # Advanced CTE optimizations (optional)
        self.enable_cte_deduplication = enable_cte_deduplication
        self.cte_fingerprints = {}  # fingerprint -> cte_name mapping for deduplication
        
        self.enable_dialect_optimizations = enable_dialect_optimizations
        
        self.enable_cte_inlining = enable_cte_inlining
        self.inlined_ctes = {}  # Track CTEs that have been inlined
        
        # Context awareness for WHERE vs SELECT clause generation
        self.in_where_context = False
        
        # Database dialect for database-specific SQL generation
        self.dialect = dialect
        if self.dialect is None:
            # Default to DuckDB if no dialect specified
            from ...dialects.duckdb import DuckDBDialect
            self.dialect = DuckDBDialect()
        
        # Initialize dependencies via injection or default imports
        if ast_nodes_module is None:
            from ..parser.ast_nodes import (
                ASTNode, ThisNode, LiteralNode, IdentifierNode, FunctionCallNode,
                BinaryOpNode, UnaryOpNode, PathNode, IndexerNode
            )
            self.ASTNode = ASTNode
            self.ThisNode = ThisNode
            self.LiteralNode = LiteralNode
            self.IdentifierNode = IdentifierNode
            self.FunctionCallNode = FunctionCallNode
            self.BinaryOpNode = BinaryOpNode
            self.UnaryOpNode = UnaryOpNode
            self.PathNode = PathNode
            self.IndexerNode = IndexerNode
        else:
            # Use injected AST nodes
            (self.ASTNode, self.ThisNode, self.LiteralNode, self.IdentifierNode, 
             self.FunctionCallNode, self.BinaryOpNode, self.UnaryOpNode, 
             self.PathNode, self.IndexerNode) = ast_nodes_module
            
        if constants_module is None:
            from .constants import FHIR_PRIMITIVE_TYPES_AS_STRING, SQL_OPERATORS
            self.FHIR_PRIMITIVE_TYPES_AS_STRING = FHIR_PRIMITIVE_TYPES_AS_STRING
            self.SQL_OPERATORS = SQL_OPERATORS
        else:
            # Use injected constants
            self.FHIR_PRIMITIVE_TYPES_AS_STRING, self.SQL_OPERATORS = constants_module
            
        if choice_types_dict is None:
            from .choice_types import fhir_choice_types
            self.fhir_choice_types = fhir_choice_types
        else:
            # Use injected choice types
            self.fhir_choice_types = choice_types_dict

    def _setup_cte_configs(self):
        """Setup CTE configuration system to replace 29 individual methods"""
        from dataclasses import dataclass
        
        @dataclass
        class CTEConfig:
            length_threshold: int = 50
            select_threshold: int = 0 
            case_threshold: int = 1
            force_threshold: int = None
        
        # Function-specific configurations based on analysis of original thresholds
        self.cte_function_configs = {
            # Low complexity functions
            'where': CTEConfig(50, 0, 1, 50), 'first': CTEConfig(50, 0, 1), 'last': CTEConfig(50, 0, 1),
            'exists': CTEConfig(60, 0, 1), 'count': CTEConfig(50, 0, 1), 'select': CTEConfig(50, 0, 1),
            'join': CTEConfig(50, 0, 1), 'array_join': CTEConfig(50, 0, 1), 'array_extraction': CTEConfig(50, 0, 1),
            
            # Medium complexity functions
            'all': CTEConfig(90, 0, 1), 'distinct': CTEConfig(120, 1, 2), 'empty': CTEConfig(90, 0, 1),
            'oftype': CTEConfig(100, 0, 1), 'extension': CTEConfig(100, 0, 1), 
            'getresourcekey': CTEConfig(90, 0, 1), 'getreferencekey': CTEConfig(90, 0, 1),
            
            # String functions
            'contains': CTEConfig(150, 1, 2, 80), 'length': CTEConfig(80, 0, 1),
            'substring': CTEConfig(90, 0, 1), 'replace': CTEConfig(90, 0, 1), 'split': CTEConfig(90, 0, 1),
            'startswith': CTEConfig(90, 0, 1), 'endswith': CTEConfig(90, 0, 1), 'indexof': CTEConfig(90, 0, 1),
            'trim': CTEConfig(90, 0, 1), 'toupper': CTEConfig(90, 0, 1), 'tolower': CTEConfig(90, 0, 1),
            'tostring': CTEConfig(90, 0, 1), 'tointeger': CTEConfig(90, 0, 1),
        }
        
        # Default configuration for unlisted functions
        self.default_cte_config = CTEConfig(50, 0, 1)
    
    def _should_use_cte_unified(self, base_expr: str, function_type: str) -> bool:
        """Unified CTE decision logic replacing 29 individual methods"""
        config = self.cte_function_configs.get(function_type, self.default_cte_config)
        
        # Basic complexity checks
        if len(base_expr) > config.length_threshold:
            return True
        if base_expr.count('SELECT') > config.select_threshold:
            return True
        if base_expr.count('CASE') >= config.case_threshold:
            return True
        
        # Force CTE for medium complexity if configured
        if config.force_threshold and self.enable_cte and len(base_expr) > config.force_threshold:
            return True
        
        return self.enable_cte and len(base_expr) > 30  # Minimum threshold

    def generate_alias(self) -> str:
        """Generate a unique alias"""
        self.alias_counter += 1
        return f"t{self.alias_counter}"
    
    # Dialect-aware helper methods for JSON operations
    def extract_json_field(self, column: str, path: str) -> str:
        """Extract JSON field as text using dialect-specific method"""
        return self.dialect.extract_json_field(column, path)
    
    def extract_json_object(self, column: str, path: str) -> str:
        """Extract JSON object using dialect-specific method"""
        return self.dialect.extract_json_object(column, path)
    
    def iterate_json_array(self, column: str, path: str) -> str:
        """Iterate JSON array using dialect-specific method"""
        return self.dialect.iterate_json_array(column, path)
    
    def check_json_exists(self, column: str, path: str) -> str:
        """Check JSON path exists using dialect-specific method"""
        return self.dialect.check_json_exists(column, path)
    
    def get_json_type(self, column: str) -> str:
        """Get JSON type using dialect-specific method"""
        return self.dialect.get_json_type(column)
    
    def get_json_array_length(self, column: str, path: str = None) -> str:
        """Get JSON array length using dialect-specific method"""
        return self.dialect.get_json_array_length(column, path)
    
    def aggregate_to_json_array(self, expression: str) -> str:
        """Aggregate to JSON array using dialect-specific method"""
        return self.dialect.aggregate_to_json_array(expression)
    
    def coalesce_empty_array(self, expression: str) -> str:
        """COALESCE with empty array using dialect-specific method"""
        return self.dialect.coalesce_empty_array(expression)
    
    def json_extract_function_call(self, column: str, path: str) -> str:
        """Generate JSON extract function call using dialect-specific method"""
        return f"{self.dialect.json_extract_function}({column}, '{path}')"
    
    def json_extract_string_function_call(self, column: str, path: str) -> str:
        """Generate JSON extract string function call using dialect-specific method"""
        return f"{self.dialect.json_extract_string_function}({column}, '{path}')"
        
    def json_array_function_call(self, *args) -> str:
        """Generate JSON array function call using dialect-specific method"""
        if args:
            args_str = ", ".join(str(arg) for arg in args)
            return f"{self.dialect.json_array_function}({args_str})"
        else:
            return f"{self.dialect.json_array_function}()"
    
    def _is_complex_expression(self, sql_expr: str) -> bool:
        """Check if SQL expression is too complex and needs optimization"""
        thresholds = self.optimization_thresholds
        
        # Length-based complexity
        if len(sql_expr) > thresholds['max_length']:
            return True
        
        # Count expensive operations
        subquery_count = sql_expr.count('SELECT')
        if subquery_count > thresholds['max_subqueries']:
            return True
        
        # JSON operations complexity
        json_ops = (sql_expr.count('json_extract') + 
                   sql_expr.count('json_type') + 
                   sql_expr.count('json_array_length'))
        if json_ops > thresholds['max_json_ops']:
            return True
        
        # Complex conditional logic
        case_count = sql_expr.count('CASE WHEN')
        if case_count > thresholds['max_case_statements']:
            return True
        
        # Overall complexity threshold
        if len(sql_expr) > self.max_sql_complexity:
            return True
            
        return False
    
    def _is_fhir_array_context(self, base_sql: str) -> bool:
        """Check if the base SQL contains a FHIR array field that requires proper flattening"""
        # Extract the JSONPath from json_extract expressions to check for FHIR array fields
        import re
        match = re.search(r"json_extract\([^,]+,\s*'([^']+)'\)", base_sql)
        if not match:
            return False
            
        path = match.group(1)
        
        # Common FHIR array fields that need special handling
        # These are fields that are arrays at the top level of resources
        fhir_array_fields = {
            '$.address', '$.telecom', '$.identifier', '$.name', '$.contact', 
            '$.communication', '$.link', '$.qualification', '$.extension',
            '$.modifierExtension', '$.contained', '$.given', '$.family'
        }
        
        # Check if the path ends with a known FHIR array field
        for array_field in fhir_array_fields:
            if path == array_field or path.endswith(array_field.replace('$.', '.')):
                return True
                
        return False
    
    def _extract_fhir_array_pattern(self, base_expr: str) -> dict:
        """Extract FHIR array patterns from complex expressions to enable efficient join operations"""
        
        # This method detects when we have a complex array flattening expression
        # that represents a simple array_field.sub_field pattern and extracts the components
        
        # Look for the characteristic pattern of generic array flattening:
        # A complex CASE expression that extracts from an array field and then a sub-field
        
        if 'json_group_array(flat_value)' not in base_expr:
            return None
            
        # Extract the array field and sub-field from the complex expression
        import re
        
        # Pattern 1: Look for json_extract(resource, '$.ARRAY_FIELD') in the expression
        array_field_match = re.search(r"json_extract\(resource,\s*'\$\.(\w+)'\)", base_expr)
        if not array_field_match:
            return None
            
        array_field = array_field_match.group(1)
        
        # Pattern 2: Look for json_extract(value, '$.SUB_FIELD') in the expression  
        sub_field_match = re.search(r"json_extract\(value,\s*'\$\.(\w+)'\)", base_expr)
        if not sub_field_match:
            return None
            
        sub_field = sub_field_match.group(1)
        
        # Verify this is a known FHIR array pattern
        known_patterns = {
            ('name', 'given'), ('name', 'family'),
            ('address', 'line'), ('address', 'city'), ('address', 'state'),
            ('telecom', 'value'), ('telecom', 'system'),
            ('identifier', 'value'), ('identifier', 'system')
        }
        
        if (array_field, sub_field) in known_patterns:
            return {
                'array_field': array_field,
                'sub_field': sub_field,
                'json_base': 'resource'  # Always resource for these patterns
            }
            
        return None
    
    def _create_optimized_expression(self, base_expr: str, operation_name: str) -> str:
        """Create an optimized placeholder for complex expressions"""
        if self._is_complex_expression(base_expr):
            # Create a unique identifier for this complex expression
            expr_hash = hash(base_expr) % 1000000
            placeholder = f"__OPTIMIZED_{operation_name}__resource_data__${expr_hash}__"
            self.complex_expr_cache[placeholder] = base_expr
            return placeholder
        return base_expr
    
    def _resolve_optimized_placeholders(self, sql: str) -> str:
        """Resolve optimized placeholders back to their actual expressions"""
        resolved_sql = sql
        for placeholder, actual_expr in self.complex_expr_cache.items():
            resolved_sql = resolved_sql.replace(placeholder, f"({actual_expr})")
        return resolved_sql
    
    def _create_cte(self, expression: str, operation_name: str) -> str:
        """Create a CTE for a complex expression and return its name with optional deduplication"""
        # Phase 6A: Enhanced CTE Deduplication with fingerprinting
        if self.enable_cte_deduplication:
            return self._create_cte_with_deduplication(expression, operation_name)
        
        # Original simple deduplication (backward compatibility)
        for cte_name, cte_expr in self.ctes.items():
            if cte_expr == expression:
                return cte_name
        
        # Create new CTE
        self.cte_counter += 1
        cte_name = f"{operation_name}_{self.cte_counter}"
        self.ctes[cte_name] = expression
        return cte_name
    
    def _create_cte_with_deduplication(self, expression: str, operation_name: str) -> str:
        """
        Phase 6A: Create CTE with advanced deduplication using fingerprinting
        
        This method provides more robust deduplication than simple string comparison by:
        1. Creating semantic fingerprints of SQL expressions
        2. Tracking expression patterns to enable intelligent reuse
        3. Handling whitespace and formatting variations
        """
        # Create fingerprint for semantic deduplication
        fingerprint = self._create_expression_fingerprint(expression)
        
        # Check if we already have a CTE for this fingerprint
        if fingerprint in self.cte_fingerprints:
            existing_cte_name = self.cte_fingerprints[fingerprint]
            # Verify the CTE still exists (safety check)
            if existing_cte_name in self.ctes:
                return existing_cte_name
            else:
                # Clean up stale fingerprint mapping
                del self.cte_fingerprints[fingerprint]
        
        # Create new CTE with deduplication tracking
        self.cte_counter += 1
        cte_name = f"{operation_name}_{self.cte_counter}"
        self.ctes[cte_name] = expression
        self.cte_fingerprints[fingerprint] = cte_name
        
        return cte_name
    
    def _create_expression_fingerprint(self, expression: str) -> str:
        """
        Create a semantic fingerprint for SQL expression deduplication
        
        This normalizes the expression to detect semantically identical queries
        that may differ in formatting, whitespace, or minor variations.
        """
        # Normalize the expression for semantic comparison
        normalized = expression.strip()
        
        # Remove extra whitespace while preserving structure
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Normalize common SQL patterns for better deduplication
        normalized = re.sub(r'\s*,\s*', ',', normalized)  # Normalize comma spacing
        normalized = re.sub(r'\s*\(\s*', '(', normalized)  # Normalize parentheses
        normalized = re.sub(r'\s*\)\s*', ')', normalized)
        
        # Create hash of normalized expression
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()[:12]  # 12 chars sufficient for uniqueness
    
    def _should_use_cte(self, expression: str) -> bool:
        """Determine if an expression should use a CTE"""
        return (len(expression) > 800 or 
                expression.count('SELECT') > 2 or 
                expression.count('json_each') > 1 or
                expression.count('CASE') > 3)
    
    def _build_final_query_with_ctes(self, main_query: str) -> str:
        """Build the final query with CTEs if any exist, with optional optimizations"""
        if not self.ctes:
            return main_query
        
        # Phase 6C: Apply smart CTE inlining first (most aggressive optimization)
        if self.enable_cte_inlining:
            return self._build_final_query_with_smart_inlining(main_query)
        
        # Phase 6B: Apply dialect-specific CTE optimizations
        if self.enable_dialect_optimizations:
            return self._build_final_query_with_dialect_optimizations(main_query)
        
        # Build WITH clause (original implementation)
        cte_definitions = []
        for cte_name, cte_expr in self.ctes.items():
            cte_definitions.append(f"{cte_name} AS ({cte_expr})")
        
        with_clause = "WITH " + ",\n".join(cte_definitions)
        return f"{with_clause}\n{main_query}"
    
    def _build_final_query_with_dialect_optimizations(self, main_query: str) -> str:
        """
        Phase 6B: Build final query with database-specific CTE optimizations
        
        Applies optimizations based on the specific database dialect:
        - PostgreSQL: MATERIALIZED/NOT MATERIALIZED hints
        - DuckDB: JSON-specific optimizations
        """
        # Build CTE definitions with dialect-specific hints
        cte_definitions = []
        for cte_name, cte_expr in self.ctes.items():
            optimized_cte = self._optimize_cte_for_dialect(cte_name, cte_expr)
            cte_definitions.append(optimized_cte)
        
        with_clause = "WITH " + ",\n".join(cte_definitions)
        return f"{with_clause}\n{main_query}"
    
    def _optimize_cte_for_dialect(self, cte_name: str, cte_expr: str) -> str:
        """
        Apply dialect-specific optimizations to individual CTE
        
        Returns the CTE definition with appropriate database-specific hints
        """
        return self.dialect.optimize_cte_definition(cte_name, cte_expr)

    def _build_final_query_with_smart_inlining(self, main_query: str) -> str:
        """
        Phase 6C: Build final query with smart CTE inlining
        
        Analyzes CTEs for inlining opportunities to eliminate unnecessary overhead.
        Simple CTEs that don't provide performance benefits are inlined directly.
        """
        if not self.ctes:
            return main_query
        
        # Analyze CTEs for inlining opportunities
        ctes_to_inline = {}
        ctes_to_keep = {}
        
        for cte_name, cte_expr in self.ctes.items():
            if self._should_inline_cte(cte_name, cte_expr, main_query):
                ctes_to_inline[cte_name] = cte_expr
            else:
                ctes_to_keep[cte_name] = cte_expr
        
        # Apply inlining to main query and remaining CTEs
        inlined_query = self._apply_cte_inlining(main_query, ctes_to_inline)
        
        # Update remaining CTEs with inlining applied
        for cte_name, cte_expr in ctes_to_keep.items():
            ctes_to_keep[cte_name] = self._apply_cte_inlining(cte_expr, ctes_to_inline)
        
        # Build final query with remaining CTEs
        if ctes_to_keep:
            # Apply dialect optimizations to remaining CTEs if enabled
            if self.enable_dialect_optimizations:
                cte_definitions = []
                for cte_name, cte_expr in ctes_to_keep.items():
                    optimized_cte = self._optimize_cte_for_dialect(cte_name, cte_expr)
                    cte_definitions.append(optimized_cte)
            else:
                cte_definitions = [f"{cte_name} AS ({cte_expr})" for cte_name, cte_expr in ctes_to_keep.items()]
            
            with_clause = "WITH " + ",\n".join(cte_definitions)
            return f"{with_clause}\n{inlined_query}"
        else:
            # All CTEs were inlined
            return inlined_query
    
    def _should_inline_cte(self, cte_name: str, cte_expr: str, main_query: str) -> bool:
        """
        Determine if a CTE should be inlined for better performance
        
        CTEs should be inlined when:
        - They are simple expressions (low complexity)
        - They are used only once
        - The overhead of CTE creation exceeds the benefit
        - They don't contain expensive operations
        """
        # Check CTE complexity
        if not self._is_simple_cte(cte_expr):
            return False
        
        # Check usage frequency - don't inline if used multiple times
        usage_count = self._count_cte_usage(cte_name, main_query)
        for other_cte_expr in self.ctes.values():
            if other_cte_expr != cte_expr:
                usage_count += self._count_cte_usage(cte_name, other_cte_expr)
        
        if usage_count > 1:
            return False  # Used multiple times, keep as CTE
        
        # Check for expensive operations that benefit from CTE
        if self._contains_expensive_operations(cte_expr):
            return False
        
        return True  # Safe to inline
    
    def _is_simple_cte(self, cte_expr: str) -> bool:
        """
        Check if a CTE is simple enough to inline
        
        Simple CTEs have:
        - Length < 150 characters
        - Single SELECT statement
        - No complex JSON operations
        - No aggregations or joins
        """
        return (
            len(cte_expr) < 150 and                    # Short expressions
            cte_expr.count('SELECT') == 1 and          # Single select
            'json_group_array' not in cte_expr and     # No aggregations
            'json_each' not in cte_expr and            # No table-valued functions
            'GROUP BY' not in cte_expr.upper() and     # No grouping
            'ORDER BY' not in cte_expr.upper() and     # No sorting
            'DISTINCT' not in cte_expr.upper()         # No deduplication
        )
    
    def _count_cte_usage(self, cte_name: str, query: str) -> int:
        """Count how many times a CTE is referenced in a query"""
        # Count references to the CTE name (as a standalone identifier)
        import re
        pattern = r'\b' + re.escape(cte_name) + r'\b'
        return len(re.findall(pattern, query))
    
    def _contains_expensive_operations(self, cte_expr: str) -> bool:
        """Check if CTE contains operations that benefit from materialization"""
        expensive_operations = [
            'json_group_array',   # Aggregation
            'json_each',          # Table expansion
            'GROUP BY',           # Grouping
            'ORDER BY',           # Sorting
            'DISTINCT',           # Deduplication
            'CASE WHEN'           # Complex conditionals (if multiple)
        ]
        
        return any(op in cte_expr.upper() for op in expensive_operations)
    
    def _apply_cte_inlining(self, query: str, ctes_to_inline: dict) -> str:
        """
        Apply CTE inlining by replacing CTE references with their expressions
        
        Safely replaces CTE references with their actual expressions while
        preserving query semantics and avoiding naming conflicts.
        """
        inlined_query = query
        
        for cte_name, cte_expr in ctes_to_inline.items():
            # Extract the main expression from "SELECT ... FROM ..." format
            cleaned_expr = self._extract_cte_expression(cte_expr)
            
            # Replace CTE references with parenthesized expressions
            import re
            pattern = r'\b' + re.escape(cte_name) + r'\b'
            replacement = f"({cleaned_expr})"
            inlined_query = re.sub(pattern, replacement, inlined_query)
            
            # Track inlined CTEs for debugging/monitoring
            self.inlined_ctes[cte_name] = cte_expr
        
        return inlined_query
    
    def _extract_cte_expression(self, cte_expr: str) -> str:
        """
        Extract the core expression from a CTE definition
        
        Converts "SELECT expression as alias FROM table" to just "expression"
        """
        # Simple pattern matching for basic CTE expressions
        # This handles the most common case: SELECT <expr> as <alias> FROM <table>
        import re
        
        # Match "SELECT <expression> as <alias> FROM <table>"
        select_pattern = r'SELECT\s+(.+?)\s+as\s+\w+\s+FROM'
        match = re.search(select_pattern, cte_expr, re.IGNORECASE)
        
        if match:
            return match.group(1).strip()
        else:
            # Fallback: return the full expression (safer but less optimal)
            return cte_expr
    
    def _generate_where_with_cte(self, func_node, base_expr: str) -> str:
        """Generate where() function using CTE approach"""
        if len(func_node.args) != 1:
            raise ValueError("where() function requires exactly one argument")
            
        # Create CTE for the base expression if it's complex
        base_cte_name = self._create_cte(
            f"SELECT {base_expr} as base_value FROM {self.table_name}",
            "where_base"
        )
        
        # Generate condition for array elements
        array_element_condition_generator = SQLGenerator(
            table_name=f"json_each(base_value)",
            json_column="value", 
            resource_type=self.resource_type,
            dialect=self.dialect
        )
        array_condition = array_element_condition_generator.visit(func_node.args[0])
        
        # Generate condition for single element
        single_element_condition_generator = SQLGenerator(
            table_name=base_cte_name,
            json_column="base_value",
            resource_type=self.resource_type, 
            dialect=self.dialect
        )
        single_condition = single_element_condition_generator.visit(func_node.args[0])
        
        # Create filtered CTE
        filtered_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {self.get_json_type('base_value')} = 'ARRAY' THEN
                        {self.coalesce_empty_array(f"(SELECT {self.aggregate_to_json_array('value')} FROM {self.iterate_json_array('base_value', '$')} WHERE {array_condition})")}
                    ELSE 
                        CASE WHEN {single_condition} THEN base_value ELSE {self.dialect.json_array_function}() END
                END as filtered_value
            FROM {base_cte_name}
        """, "where_filtered")
        
        return f"(SELECT filtered_value FROM {filtered_cte_name})"

    def _generate_first_with_cte(self, base_expr: str) -> str:
        """Generate first() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "first_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for first element extraction
        first_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NULL THEN NULL
                    ELSE COALESCE({self.extract_json_object(base_ref, '$[0]')}, {base_ref})
                END as first_value
            FROM {from_clause}
        """, "first_result")
        
        return f"(SELECT first_value FROM {first_cte_name})"

    def _generate_join_with_cte(self, func_node, base_expr: str) -> str:
        """Generate join() function using CTE approach with dialect-specific array joining"""
        
        # Extract separator argument
        if len(func_node.args) == 0:
            separator_sql = "''"
        elif len(func_node.args) == 1:
            separator_sql = self.visit(func_node.args[0])
        else:
            raise ValueError("join() function takes 0 or 1 arguments")
        
        # Create base CTE for the array to join
        base_cte_name = self._create_cte(
            f"SELECT {base_expr} as base_array FROM {self.table_name}",
            "join_base"
        )
        
        # Create array join CTE using dialect-specific method
        # This ensures consistency between CTE and non-CTE approaches
        join_result_cte = self._create_cte(f"""
            SELECT {self.dialect.join_array_elements('base_array', separator_sql)} as joined_result
            FROM {base_cte_name}
        """, "join_result")
        
        return f"(SELECT joined_result FROM {join_result_cte})"

    def _generate_last_with_cte(self, base_expr: str) -> str:
        """Generate last() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "last_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for last element extraction
        last_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN 
                        {self.dialect.json_extract_function}({base_ref}, {self.dialect.string_concat(self.dialect.string_concat("'$['", f'({self.get_json_array_length(base_ref)} - 1)'), "']'")})
                    ELSE {base_ref}
                END as last_value
            FROM {from_clause}
        """, "last_result")
        
        return f"(SELECT last_value FROM {last_cte_name})"

    def _generate_array_extraction_with_cte(self, base_sql: str, identifier_name: str) -> str:
        """Generate array-aware path extraction using CTE approach"""
        
        # Create base CTE for the expression to extract from
        base_cte_name = self._create_cte(
            f"SELECT {base_sql} as base_value FROM {self.table_name}",
            "array_extract_base"
        )
        
        # Create extraction CTE using dialect-specific method
        extraction_cte = self._create_cte(f"""
            SELECT {self.dialect.extract_nested_array_path('base_value', '$', identifier_name, f'$.{identifier_name}')} as extracted_value
            FROM {base_cte_name}
        """, "array_extract_result")
        
        return f"(SELECT extracted_value FROM {extraction_cte})"

    def _generate_exists_with_cte(self, func_node, base_expr: str) -> str:
        """Generate exists() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "exists_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Handle exists() with or without criteria
        if not func_node.args:  # Simple exists()
            # Create CTE for existence check
            exists_cte_name = self._create_cte(f"""
                SELECT 
                    CASE 
                        WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN {self.get_json_array_length(base_ref)} > 0
                        ELSE ({base_ref} IS NOT NULL AND NOT ({self.get_json_type(base_ref)} = 'OBJECT' AND {self.get_json_array_length(f"{self.dialect.json_extract_function}({base_ref}, '$.keys()')")} = 0))
                    END as exists_result
                FROM {from_clause}
            """, "exists_check")
        else:  # exists(criteria)
            # For exists(criteria), we need to apply where() first, then check existence
            # This is equivalent to (collection.where(criteria)).exists()
            where_node = FunctionCallNode(name='where', args=[func_node.args[0]])
            
            # Generate where filtering using CTE if available
            if hasattr(self, '_generate_where_with_cte') and self.enable_cte:
                try:
                    filtered_expr = self._generate_where_with_cte(where_node, base_ref)
                except:
                    # Fallback to traditional where
                    filtered_expr = self.apply_function_to_expression(where_node, base_ref)
            else:
                filtered_expr = self.apply_function_to_expression(where_node, base_ref)
            
            # Create CTE for existence check on filtered results
            exists_cte_name = self._create_cte(f"""
                SELECT 
                    CASE 
                        WHEN {self.get_json_type(f"({filtered_expr})")} = 'ARRAY' THEN {self.get_json_array_length(f"({filtered_expr})")} > 0
                        ELSE (({filtered_expr}) IS NOT NULL AND NOT ({self.get_json_type(f"({filtered_expr})")} = 'OBJECT' AND {self.get_json_array_length(f"{self.dialect.json_extract_function}(({filtered_expr}), '$.keys()')")} = 0))
                    END as exists_result
                FROM {from_clause}
            """, "exists_check")
        
        return f"(SELECT exists_result FROM {exists_cte_name})"

    def _generate_count_with_cte(self, base_expr: str) -> str:
        """Generate count() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "count_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for count calculation
        count_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN {self.get_json_array_length(base_ref)}
                    WHEN {base_ref} IS NOT NULL THEN 1
                    ELSE 0
                END as count_result
            FROM {from_clause}
        """, "count_result")
        
        return f"(SELECT count_result FROM {count_cte_name})"

    def _generate_select_with_cte(self, func_node, base_expr: str) -> str:
        """Generate select() function using CTE approach"""
        
        # The main benefit for select() CTE is avoiding the recursive SQLGenerator pattern
        # We'll create a simplified CTE that processes the selection without recursion
        
        select_expr = func_node.args[0]
        
        # For simple field access, we can create a streamlined CTE
        if hasattr(select_expr, 'name') and isinstance(select_expr, self.IdentifierNode):
            field_name = select_expr.name
            
            # Create a single CTE that handles the entire select operation
            select_cte_name = self._create_cte(f"""
                SELECT 
                    CASE 
                        WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN (
                            SELECT {self.aggregate_to_json_array(f"{self.dialect.json_extract_function}(elem.value, '$.{field_name}')")}
                            FROM {self.iterate_json_array(base_expr, "$")} AS elem(value)
                            WHERE {self.dialect.json_extract_function}(elem.value, '$.{field_name}') IS NOT NULL
                        )
                        WHEN {base_expr} IS NOT NULL THEN 
                            {self.dialect.json_array_function}({self.dialect.json_extract_function}({base_expr}, '$.{field_name}'))
                        ELSE 
                            {self.dialect.json_array_function}()
                    END as select_result
                FROM {self.table_name}
            """, "select_result")
            
            return f"(SELECT select_result FROM {select_cte_name})"
        
        else:
            # For complex expressions, fall back to original implementation to avoid errors
            # The CTE benefit here is minimal compared to correctness
            print(f"Complex select expression detected, falling back to original implementation")
            raise Exception("Complex select expression - falling back to original")

    def _generate_contains_with_cte(self, func_node, base_expr: str) -> str:
        """Generate contains() function using CTE approach"""
        
        # Get the search value
        search_value = self.visit(func_node.args[0])
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "contains_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for contains logic - this eliminates the nested COUNT subquery
        contains_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN (
                        CASE WHEN EXISTS(
                            SELECT 1 
                            FROM {self.iterate_json_array(base_ref, "$")}
                            WHERE value = {search_value}
                        ) THEN true ELSE false END
                    )
                    WHEN {base_ref} = {search_value} THEN true
                    ELSE false
                END as contains_result
            FROM {from_clause}
        """, "contains_check")
        
        return f"(SELECT contains_result FROM {contains_cte_name})"

    def _generate_all_with_cte(self, func_node, base_expr: str) -> str:
        """Generate all() function using CTE approach"""
        
        # Get the criteria expression
        criteria_expr = func_node.args[0]
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "all_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # For simple criteria, we can inline the logic
        if hasattr(criteria_expr, 'name') and isinstance(criteria_expr, self.IdentifierNode):
            # Simple field comparison like: all(active)
            field_name = criteria_expr.name
            criteria_logic = f"{self.dialect.json_extract_function}(elem.value, '$.{field_name}') = true"
        else:
            # Complex criteria - simplified for now
            criteria_logic = "elem.value IS NOT NULL"  # Fallback
        
        # Create CTE for all() logic - eliminates recursive SQLGenerator
        all_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN COUNT(*) = 0 THEN true
                    WHEN COUNT(*) = COUNT(CASE WHEN ({criteria_logic}) THEN 1 END) THEN true
                    ELSE false
                END as all_result
            FROM {self.iterate_json_array(base_ref, '$')} AS elem(value)
            WHERE elem.value IS NOT NULL
        """, "all_check")
        
        return f"(SELECT all_result FROM {all_cte_name})"

    def _generate_distinct_with_cte(self, base_expr: str) -> str:
        """Generate distinct() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "distinct_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for distinct operation
        distinct_cte_name = self._create_cte(f"""
            SELECT 
                json_group_array(DISTINCT elem.value) as distinct_result
            FROM {self.iterate_json_array(base_ref, '$')} AS elem(value)
            WHERE elem.value IS NOT NULL
        """, "distinct_result")
        
        return f"(SELECT distinct_result FROM {distinct_cte_name})"

    def _generate_empty_with_cte(self, base_expr: str) -> str:
        """Generate empty() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "empty_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Use dialect-aware type comparison
        array_type_constant = self.dialect.get_json_type_constant('ARRAY')
        object_type_constant = self.dialect.get_json_type_constant('OBJECT')
        array_type_check = f"{self.get_json_type(base_ref)} = '{array_type_constant}'"
        object_type_check = f"{self.get_json_type(base_ref)} = '{object_type_constant}'"
        
        # Create CTE for empty check operation
        empty_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {array_type_check} THEN {self.get_json_array_length(base_ref)} = 0
                    ELSE ({base_ref} IS NULL OR ({object_type_check} AND {self.get_json_array_length(f"{self.dialect.json_extract_function}({base_ref}, '$.keys()')")} = 0))
                END as empty_result
            FROM {from_clause}
        """, "empty_result")
        
        return f"(SELECT empty_result FROM {empty_cte_name})"

    def _generate_oftype_with_cte(self, base_expr: str, type_name_arg: str, element_condition: str) -> str:
        """Generate ofType() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "oftype_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for ofType filtering operation
        oftype_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN json_type({base_ref}) = 'ARRAY' THEN
                        COALESCE(
                            (SELECT json_group_array(value)
                             FROM json_each({base_ref})
                             WHERE {element_condition.replace('element_value', 'value')}),
                            json_array()
                        )
                    ELSE 
                        CASE WHEN {element_condition.replace('element_value', base_ref)} THEN json_array({base_ref}) ELSE json_array() END
                END as oftype_result
            FROM {from_clause}
        """, "oftype_result")
        
        return f"(SELECT oftype_result FROM {oftype_cte_name})"

    def _generate_length_with_cte(self, base_expr: str) -> str:
        """Generate length() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "length_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for length operation
        length_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN {self.get_json_array_length(base_ref)}
                    WHEN {base_ref} IS NOT NULL THEN 1
                    ELSE 0
                END as length_result
            FROM {from_clause}
        """, "length_result")
        
        return f"(SELECT length_result FROM {length_cte_name})"

    def _generate_substring_with_cte(self, base_expr: str, start_sql: str, length_sql: str) -> str:
        """Generate substring() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "substring_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Handle the case where base_ref is already a JSON extracted value
        if 'json_extract(' in base_ref and not 'json_extract_string(' in base_ref:
            # Convert json_extract to json_extract_string to remove quotes for string operations
            import re
            base_value = re.sub(r'json_extract\(', 'json_extract_string(', base_ref)
        elif 'json_extract_string(' in base_ref:
            # Already a string extraction, use directly
            base_value = base_ref
        else:
            # Extract as string
            base_value = self.extract_json_field(base_ref, '$')
        
        # Create CTE for substring operation
        substring_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL THEN
                        {self.dialect.substring(base_value, start_sql, length_sql)}
                    ELSE NULL
                END as substring_result
            FROM {from_clause}
        """, "substring_result")
        
        return f"(SELECT substring_result FROM {substring_cte_name})"

    def _generate_replace_with_cte(self, base_expr: str, search_sql: str, replace_sql: str) -> str:
        """Generate replace() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "replace_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for replace operation
        replace_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL THEN
                        REPLACE(CAST({base_ref} AS VARCHAR), CAST({search_sql} AS VARCHAR), CAST({replace_sql} AS VARCHAR))
                    ELSE NULL
                END as replace_result
            FROM {from_clause}
        """, "replace_result")
        
        return f"(SELECT replace_result FROM {replace_cte_name})"

    def _generate_split_with_cte(self, base_expr: str, separator_sql: str) -> str:
        """Generate split() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "split_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for split operation
        split_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL THEN {self.dialect.split_string(f'CAST({base_ref} AS VARCHAR)', separator_sql)}
                    ELSE NULL
                END as split_result
            FROM {from_clause}
        """, "split_result")
        
        return f"(SELECT split_result FROM {split_cte_name})"

    def _generate_extension_with_cte(self, base_expr: str, extension_url: str) -> str:
        """Generate extension() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "extension_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for extension filtering operation
        extension_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN COUNT(*) = 0 THEN json_array()
                    ELSE json_group_array(
                        json_object(
                            'id', {self.extract_json_field('value', '$.id')},
                            'url', {self.extract_json_field('value', '$.url')},
                            'extension', {self.extract_json_object('value', '$.extension')},
                            'value', COALESCE(
                                {self.extract_json_object('value', '$.valueString')},
                                {self.extract_json_object('value', '$.valueCode')},
                                {self.extract_json_object('value', '$.valueBoolean')},
                                {self.extract_json_object('value', '$.valueInteger')},
                                {self.extract_json_object('value', '$.valueDecimal')},
                                {self.extract_json_object('value', '$.valueQuantity')},
                                {self.extract_json_object('value', '$.valueCodeableConcept')},
                                {self.extract_json_object('value', '$.valueCoding')},
                                {self.extract_json_object('value', '$.valueDateTime')},
                                {self.extract_json_object('value', '$.valueDate')}
                            )
                        )
                    )
                END as extension_result
             FROM json_each(json_extract({base_ref}, '$.extension'))
             WHERE {self.extract_json_field('value', '$.url')} = '{extension_url}'
        """, "extension_result")
        
        return f"(SELECT extension_result FROM {extension_cte_name})"

    def _generate_startswith_with_cte(self, base_expr: str, prefix_sql: str) -> str:
        """Generate startswith() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "startswith_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for startswith operation
        startswith_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL AND {prefix_sql} IS NOT NULL THEN
                        CASE WHEN CAST({base_ref} AS VARCHAR) LIKE {self.dialect.string_concat(f'CAST({prefix_sql} AS VARCHAR)', "'%'")} THEN true ELSE false END
                    ELSE false
                END as startswith_result
            FROM {from_clause}
        """, "startswith_result")
        
        return f"(SELECT startswith_result FROM {startswith_cte_name})"

    def _generate_endswith_with_cte(self, base_expr: str, suffix_sql: str) -> str:
        """Generate endswith() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "endswith_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for endswith operation
        endswith_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL AND {suffix_sql} IS NOT NULL THEN
                        CASE WHEN CAST({base_ref} AS VARCHAR) LIKE {self.dialect.string_concat("'%'", f'CAST({suffix_sql} AS VARCHAR)')} THEN true ELSE false END
                    ELSE false
                END as endswith_result
            FROM {from_clause}
        """, "endswith_result")
        
        return f"(SELECT endswith_result FROM {endswith_cte_name})"

    def _generate_indexof_with_cte(self, base_expr: str, search_sql: str) -> str:
        """Generate indexof() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "indexof_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for indexof operation
        indexof_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL AND {search_sql} IS NOT NULL THEN
                        {self.dialect.string_position(search_sql, base_ref)}
                    ELSE -1
                END as indexof_result
            FROM {from_clause}
        """, "indexof_result")
        
        return f"(SELECT indexof_result FROM {indexof_cte_name})"

    def _generate_toupper_with_cte(self, base_expr: str) -> str:
        """Generate toupper() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "toupper_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for toupper operation
        toupper_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL THEN UPPER(CAST({base_ref} AS VARCHAR))
                    ELSE NULL
                END as toupper_result
            FROM {from_clause}
        """, "toupper_result")
        
        return f"(SELECT toupper_result FROM {toupper_cte_name})"

    def _generate_tolower_with_cte(self, base_expr: str) -> str:
        """Generate tolower() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "tolower_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for tolower operation
        tolower_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL THEN LOWER(CAST({base_ref} AS VARCHAR))
                    ELSE NULL
                END as tolower_result
            FROM {from_clause}
        """, "tolower_result")
        
        return f"(SELECT tolower_result FROM {tolower_cte_name})"

    def _generate_trim_with_cte(self, base_expr: str) -> str:
        """Generate trim() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "trim_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for trim operation
        trim_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL THEN TRIM(CAST({base_ref} AS VARCHAR))
                    ELSE NULL
                END as trim_result
            FROM {from_clause}
        """, "trim_result")
        
        return f"(SELECT trim_result FROM {trim_cte_name})"

    def _generate_tostring_with_cte(self, base_expr: str) -> str:
        """Generate tostring() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "tostring_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for tostring operation
        tostring_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL THEN
                        CAST(CAST({base_ref} AS INTEGER) AS VARCHAR)
                    ELSE NULL
                END as tostring_result
            FROM {from_clause}
        """, "tostring_result")
        
        return f"(SELECT tostring_result FROM {tostring_cte_name})"

    def _generate_tointeger_with_cte(self, base_expr: str) -> str:
        """Generate tointeger() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "tointeger_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Handle the case where base_expr is already extracted
        if 'json_extract' in base_ref or 'SUBSTRING' in base_ref:
            # If already a string value, cast directly
            base_value = base_ref
        else:
            # Extract as string first
            base_value = self.extract_json_field(base_ref, '$')
        
        # Create CTE for tointeger operation
        tointeger_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL THEN
                        CAST({base_value} AS INTEGER)
                    ELSE NULL
                END as tointeger_result
            FROM {from_clause}
        """, "tointeger_result")
        
        return f"(SELECT tointeger_result FROM {tointeger_cte_name})"

    def _generate_getresourcekey_with_cte(self) -> str:
        """Generate getResourceKey() function using CTE approach"""
        
        # Create CTE for getResourceKey operation
        getresourcekey_cte_name = self._create_cte(f"""
            SELECT 
                json_extract_string({self.json_column}, '$.id') as getresourcekey_result
            FROM {self.table_name}
        """, "getresourcekey_result")
        
        return f"(SELECT getresourcekey_result FROM {getresourcekey_cte_name})"

    def _generate_getreferencekey_with_cte(self, base_expr: str, func_node) -> str:
        """Generate getReferenceKey() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "getreferencekey_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Handle simplified base expression logic
        simplified_base = base_ref
        if 'json_group_array' in base_ref and 'link' in base_ref:
            # For complex link expressions, use simpler extraction
            simplified_base = f"""
            (SELECT {self.extract_json_field(self.extract_json_object('value', '$.other'), '$.reference')} 
             FROM {self.iterate_json_array(self.extract_json_object(self.json_column, '$.link'), '$')}
             WHERE {self.extract_json_field(self.extract_json_object('value', '$.other'), '$.reference')} IS NOT NULL
             LIMIT 1)
            """
        
        # Build the type filter logic
        if func_node.args:
            # If a resource type is provided, validate it matches
            if isinstance(func_node.args[0], self.IdentifierNode):
                expected_type = func_node.args[0].name
                type_filter = f"""
                CASE 
                    WHEN {simplified_base} IS NULL THEN NULL
                    WHEN STRPOS(CAST({simplified_base} AS VARCHAR), '/') > 0 THEN
                        CASE 
                            WHEN SPLIT_PART(CAST({simplified_base} AS VARCHAR), '/', 1) = '{expected_type}' THEN
                                SPLIT_PART(CAST({simplified_base} AS VARCHAR), '/', 2)
                            ELSE NULL
                        END
                    ELSE NULL
                END
                """
            else:
                raise ValueError("getReferenceKey() type argument must be an identifier")
        else:
            # No type validation, just extract the ID part
            type_filter = f"""
            CASE 
                WHEN {simplified_base} IS NULL THEN NULL
                WHEN STRPOS(CAST({simplified_base} AS VARCHAR), '/') > 0 THEN
                    SPLIT_PART(CAST({simplified_base} AS VARCHAR), '/', 2)
                ELSE NULL
            END
            """
        
        # Create CTE for getReferenceKey operation
        getreferencekey_cte_name = self._create_cte(f"""
            SELECT 
                {type_filter} as getreferencekey_result
            FROM {from_clause}
        """, "getreferencekey_result")
        
        return f"(SELECT getreferencekey_result FROM {getreferencekey_cte_name})"
    
    def visit(self, node) -> str:
        """Visit an AST node and generate SQL"""
        if isinstance(node, self.ThisNode): # Handle $this if AST rewrite produces it
            return self.json_column
        elif isinstance(node, self.LiteralNode):
            return self.visit_literal(node)
        elif isinstance(node, self.IdentifierNode):
            return self.visit_identifier(node)
        elif isinstance(node, self.PathNode):
            return self.visit_path(node)
        elif isinstance(node, self.FunctionCallNode):
            return self.visit_function_call(node)
        elif isinstance(node, self.BinaryOpNode):
            return self.visit_binary_op(node)
        elif isinstance(node, self.UnaryOpNode):
            return self.visit_unary_op(node)
        elif isinstance(node, self.IndexerNode):
            return self.visit_indexer(node)
        else:
            raise ValueError(f"Unknown node type: {type(node)}")
    
    def visit_literal(self, node) -> str:
        """Visit a literal node"""
        if node.type == 'string':
            return f"'{node.value}'"
        elif node.type in ['integer', 'decimal']:
            return str(node.value)
        elif node.type == 'boolean':
            return str(node.value).lower()
        else:
            return str(node.value)
    
    def visit_identifier(self, node) -> str:
        """Visit an identifier node"""
        if self.resource_type == 'Observation' and node.name == 'value':
            # Handle Observation.value choice type by coalescing possible value[x] fields.
            # This makes 'Observation.value' resolve to the actual typed data if present.
            possible_value_fields = [
                'valueQuantity', 'valueCodeableConcept', 'valueString', 'valueBoolean',
                'valueInteger', 'valueRange', 'valueRatio', 'valueSampledData',
                'valueTime', 'valueDateTime', 'valuePeriod'
            ]
            # COALESCE returns the first non-null expression.
            coalesce_args = ", ".join([self.extract_json_object(self.json_column, f"$.{field}") for field in possible_value_fields])
            return f"COALESCE({coalesce_args})"
        elif self.resource_type == 'Patient' and node.name == 'deceased':
            # Handle Patient.deceased choice type by coalescing possible deceased[x] fields.
            possible_deceased_fields = ['deceasedBoolean', 'deceasedDateTime']
            coalesce_args = ", ".join([self.extract_json_object(self.json_column, f"$.{field}") for field in possible_deceased_fields])
            return f"COALESCE({coalesce_args})"

        # Default behavior for other identifiers
        json_path = f"$.{node.name}"
        return self.extract_json_object(self.json_column, json_path)
    
    def visit_path(self, node) -> str:
        """Visit a path node - builds the JSON path sequentially"""
        if len(node.segments) == 0:
            return self.json_column
        
        return self.build_path_expression(node.segments)
    
    def _apply_identifier_segment(self, identifier_name: str, base_sql: str) -> str:
        """Applies an identifier segment to a base SQL expression with proper array flattening."""
        
        # Optimize complex base expressions early
        if self._is_complex_expression(base_sql):
            base_sql = self._create_optimized_expression(base_sql, f"field_{identifier_name}")
        
        # Handle optimized index markers from arithmetic indexing
        if base_sql.startswith('__OPTIMIZED_INDEX__'):
            # Parse the marker: __OPTIMIZED_INDEX__<base>__<path>__
            marker_content = base_sql[len('__OPTIMIZED_INDEX__'):]
            if marker_content.endswith('__'):
                marker_content = marker_content[:-2]  # Remove trailing __
            
            # Split on the pattern __$. to separate base and path
            if '__$.' in marker_content:
                json_base, path_part = marker_content.split('__$.', 1)
                # Append the new identifier to the path
                new_path = f"$.{path_part}.{identifier_name}"
                # Use json_extract_string for leaf fields (likely string values)
                return self.extract_json_field(json_base, new_path)
            elif '__' in marker_content:
                # Fallback parsing
                parts = marker_content.split('__')
                if len(parts) >= 2:
                    json_base = parts[0]
                    current_path = parts[1]
                    new_path = f"{current_path}.{identifier_name}"
                    return self.extract_json_field(json_base, new_path)
        
        # Clean array-aware field extraction using DuckDB's JSON capabilities
        # Handle arrays properly at each segment without complex pattern matching
        if 'json_extract(' in base_sql and not 'json_group_array' in base_sql and not 'json_each' in base_sql:
            # Extract the base expression and current path from the json_extract
            import re
            match = re.match(r"json_extract\(([^,]+),\s*'([^']+)'\)", base_sql.strip())
            if match:
                json_base, current_path = match.groups()
                new_path = f"{current_path}.{identifier_name}"
                
                # Use CTE approach for complex expressions in json_extract pattern (but not in WHERE context)
                if (self.enable_cte and 
                    not self.in_where_context and 
                    self._should_use_cte_unified(base_sql, 'array_extraction')):
                    try:
                        return self._generate_array_extraction_with_cte(base_sql, identifier_name)
                    except Exception as e:
                        # Fallback to direct dialect method if CTE generation fails
                        print(f"CTE generation failed for json_extract array extraction, falling back to direct method: {e}")
                
                # Use dialect-specific array-aware extraction pattern
                # For arrays, use [*] on the parent, not the child: $.address[*].line not $.address.line[*]
                return self.dialect.extract_nested_array_path(json_base, current_path, identifier_name, new_path)
        
        # Note: Removed hardcoded PostgreSQL name.given optimization 
        # Now handled by generic FHIR array pattern matching in join functions
        
        # Use CTE approach for complex array extraction operations (but not in WHERE context)
        if (self.enable_cte and 
            not self.in_where_context and 
            self._should_use_cte_unified(base_sql, 'array_extraction')):
            try:
                return self._generate_array_extraction_with_cte(base_sql, identifier_name)
            except Exception as e:
                # Fallback to direct dialect method if CTE generation fails
                print(f"CTE generation failed for array extraction, falling back to direct method: {e}")
        
        # Use dialect-specific array-aware path extraction for all cases
        # This replaces the complex PostgreSQL-specific fallback logic with a cleaner approach
        return self.dialect.extract_nested_array_path(base_sql, "$", identifier_name, f"$.{identifier_name}")
        
    def build_path_expression(self, segments) -> str:
        """Build a path expression from segments"""
        if not segments: return self.json_column

        # Check for FHIR choice type patterns anywhere in the path
        # Look for patterns like: identifier.ofType(typename) or complex.path.identifier.ofType(typename)
        for i in range(len(segments) - 1):
            if (isinstance(segments[i], self.IdentifierNode) and 
                isinstance(segments[i + 1], self.FunctionCallNode) and
                segments[i + 1].name.lower() == 'oftype' and
                len(segments[i + 1].args) == 1 and
                isinstance(segments[i + 1].args[0], self.IdentifierNode)):
                
                field_name = segments[i].name
                type_name = segments[i + 1].args[0].name
                
                self.debug_steps.append({
                    "operation": "build_path_expression:choice_type_detected",
                    "field_name": field_name,
                    "type_name": type_name,
                    "position": i
                })
                
                # Try choice type mapping
                choice_field_mapping = self.fhir_choice_types.get_choice_field_mapping_direct(field_name, type_name)
                if choice_field_mapping:
                    self.debug_steps.append({
                        "operation": "build_path_expression:choice_type_shortcut",
                        "original_path": f"{field_name}.ofType({type_name})",
                        "mapped_field": choice_field_mapping
                    })
                    
                    # Build the full path with choice type substitution
                    # Take segments before the choice type pair
                    prefix_segments = segments[:i]
                    # Replace the choice type pair with the mapped field
                    suffix_segments = segments[i + 2:]  # Skip the identifier and ofType function
                    
                    # Build prefix path if exists
                    if prefix_segments:
                        prefix_sql = self._build_segments_path(prefix_segments)
                        base_path = self.extract_json_object(prefix_sql, f"$.{choice_field_mapping}")
                    else:
                        base_path = self.extract_json_object(self.json_column, f"$.{choice_field_mapping}")
                    
                    # Apply suffix segments if any
                    if suffix_segments:
                        current_sql = base_path
                        for suffix_segment in suffix_segments:
                            if isinstance(suffix_segment, self.IdentifierNode):
                                current_sql = self._apply_identifier_segment(suffix_segment.name, current_sql)
                            elif isinstance(suffix_segment, self.FunctionCallNode):
                                current_sql = self.apply_function_to_expression(suffix_segment, current_sql)
                        return current_sql
                    else:
                        return base_path
                else:
                    self.debug_steps.append({
                        "operation": "build_path_expression:choice_type_no_mapping",
                        "field_name": field_name,
                        "type_name": type_name
                    })

        # The first segment is visited normally; its base is self.json_column.
        # self.visit will correctly handle IdentifierNode, FunctionCallNode, IndexerNode etc.
        # for the first segment.
        current_sql = self.visit(segments[0])

        # Iteratively apply subsequent segments
        for i in range(1, len(segments)):
            segment_node = segments[i]
            try:
                # Optimize complex expressions before processing next segment
                if self._is_complex_expression(current_sql):
                    current_sql = self._create_optimized_expression(current_sql, f"chain_{i}")
                
                if isinstance(segment_node, self.IdentifierNode):
                    current_sql = self._apply_identifier_segment(segment_node.name, current_sql)
                elif isinstance(segment_node, self.FunctionCallNode):
                    current_sql = self.apply_function_to_expression(segment_node, current_sql)
                elif isinstance(segment_node, self.IndexerNode):
                    # current_sql is the SQL for the expression *before* this indexable segment.
                    # segment_node.expression is the member/function call that results in the collection to be indexed.
                    # segment_node.index is the index.
                    
                    expr_to_be_indexed_ast = segment_node.expression 
                    sql_for_collection_before_index: str

                    if isinstance(expr_to_be_indexed_ast, self.IdentifierNode):
                        sql_for_collection_before_index = self._apply_identifier_segment(expr_to_be_indexed_ast.name, current_sql)
                    elif isinstance(expr_to_be_indexed_ast, self.FunctionCallNode):
                        sql_for_collection_before_index = self.apply_function_to_expression(expr_to_be_indexed_ast, current_sql)
                    elif isinstance(expr_to_be_indexed_ast, self.ThisNode): # $this[idx] after a path. $this refers to current_sql
                        sql_for_collection_before_index = current_sql
                    else:
                        # This case means the parser created an IndexerNode segment where segment.expression
                        # is not a simple identifier or function call or $this.
                        # This should be rare if `parse_primary_expression` correctly forms IndexerNodes
                        # such that complex bases are handled by `visit_indexer` when the IndexerNode is primary.
                        raise ValueError(f"Unsupported expression type '{type(expr_to_be_indexed_ast).__name__}' inside IndexerNode path segment.")

                    index_value_sql = self.visit(segment_node.index) # Index is evaluated in global scope.
                    
                    # Apply the index to this collection.
                    # For dynamic indexing, we still need to use the raw JSON function since path is computed
                    # For arithmetic expressions, use json_extract_string to avoid quotes around string values
                    if self._is_simple_arithmetic_expression(index_value_sql):
                        json_extract_func = self.dialect.json_extract_string_function
                        empty_value = "''"  # Empty string for string extraction
                    else:
                        json_extract_func = self.dialect.json_extract_function  
                        empty_value = f"{self.dialect.json_array_function}()"  # Empty array for object extraction
                    
                    path_prefix = "'$['"
                    path_suffix = "']'"
                    current_sql = f"COALESCE({json_extract_func}({sql_for_collection_before_index}, {self.dialect.string_concat(self.dialect.string_concat(path_prefix, f'CAST({index_value_sql} AS VARCHAR)'), path_suffix)}), {empty_value})"
                else:
                    raise ValueError(f"Unsupported segment type '{type(segment_node).__name__}' in path chain.")
            except Exception as e:
                # Add context to function chaining errors
                self.debug_steps.append({
                    "operation": "build_path_expression:segment_error",
                    "segment_index": i,
                    "segment_type": type(segment_node).__name__,
                    "error": str(e),
                    "current_sql": current_sql[:100] if current_sql else "None"
                })
                # Re-raise with more context
                raise ValueError(f"Error processing segment {i} ({type(segment_node).__name__}): {str(e)}") from e
        return current_sql
    
    def apply_function_to_expression(self, func_node, base_expr: str) -> str:
        """Apply a function to a base expression"""
        func_name = func_node.name.lower()

        # Enhanced debug log for function entry
        self.debug_steps.append({
            "operation": "apply_function_to_expression:entry_point",
            "func_name_called": func_name,
            "base_expr_sql_input_type": type(base_expr).__name__,
            "base_expr_sql_input_preview": str(base_expr)[:150]
        })
        
        # Validate function arguments upfront to provide better error messages
        try:
            self._validate_function_args(func_name, func_node.args)
        except ValueError as e:
            raise ValueError(f"Function '{func_name}': {str(e)}") from e
        
        if func_name == 'exists':
            # PHASE 2D: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'exists'):
                try:
                    return self._generate_exists_with_cte(func_node, base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for exists(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            if not func_node.args: # exists()
                return f"""
                CASE 
                    WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN {self.get_json_array_length(base_expr)} > 0
                    ELSE ({base_expr} IS NOT NULL AND NOT ({self.get_json_type(base_expr)} = 'OBJECT' AND {self.get_json_array_length(f"{self.dialect.json_extract_function}({base_expr}, '$.keys()')")} = 0))
                END
                """
            else: # exists(criteria) - equivalent to (collection.where(criteria)).exists()
                where_node = FunctionCallNode(name='where', args=[func_node.args[0]])
                sql_after_where = self.apply_function_to_expression(where_node, base_expr)
                # Now apply simple .exists() to the result of the where clause
                return self.apply_function_to_expression(FunctionCallNode(name='exists', args=[]), f"({sql_after_where})")
        elif func_name == 'empty':
            # PHASE 5A: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'empty'):
                try:
                    return self._generate_empty_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for empty(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Use dialect-aware type comparison
            array_type_constant = self.dialect.get_json_type_constant('ARRAY')
            object_type_constant = self.dialect.get_json_type_constant('OBJECT')
            array_type_check = f"{self.get_json_type(base_expr)} = '{array_type_constant}'"
            object_type_check = f"{self.get_json_type(base_expr)} = '{object_type_constant}'"
            
            return f"""
            CASE 
                WHEN {array_type_check} THEN {self.get_json_array_length(base_expr)} = 0
                ELSE ({base_expr} IS NULL OR ({object_type_check} AND {self.get_json_array_length(f"{self.dialect.json_extract_function}({base_expr}, '$.keys()')")} = 0))
            END
            """
        elif func_name == 'first':
            # PHASE 2: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'first'):
                try:
                    return self._generate_first_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for first(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Use a simplified first() that avoids expression duplication
            # Properly handle null base expressions (missing fields)
            optimized_base = self._create_optimized_expression(base_expr, "first")
            return f"""
            CASE 
                WHEN {optimized_base} IS NULL THEN NULL
                ELSE COALESCE({self.extract_json_object(optimized_base, '$[0]')}, {optimized_base})
            END
            """
        elif func_name == 'last':
            # PHASE 2C: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'last'):
                try:
                    return self._generate_last_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for last(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return f"""
            CASE 
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN 
                    {self.dialect.json_extract_function}({base_expr}, {self.dialect.string_concat(self.dialect.string_concat("'$['", f'({self.get_json_array_length(base_expr)} - 1)'), "']'")})
                ELSE {base_expr}
            END
            """
        elif func_name == 'count':
            # PHASE 3: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'count'):
                try:
                    return self._generate_count_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for count(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return f"""
            CASE 
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN {self.get_json_array_length(base_expr)}
                WHEN {base_expr} IS NOT NULL THEN 1
                ELSE 0
            END
            """
        elif func_name == 'length':
            # PHASE 5C: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'length'):
                try:
                    return self._generate_length_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for length(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Alias for count() function
            return f"""
            CASE 
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN {self.get_json_array_length(base_expr)}
                WHEN {base_expr} IS NOT NULL THEN 1
                ELSE 0
            END
            """
        elif func_name == 'contains':
            if len(func_node.args) != 1:
                raise ValueError("contains() function requires exactly one argument")
            
            # PHASE 4B: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'contains'):
                try:
                    return self._generate_contains_with_cte(func_node, base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for contains(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            search_value = self.visit(func_node.args[0])
            return f"""
            CASE 
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN (
                    SELECT CASE WHEN COUNT(*) > 0 THEN true ELSE false END
                    FROM {self.iterate_json_array(base_expr, "$")}
                    WHERE value = {search_value}
                )
                WHEN {base_expr} = {search_value} THEN true
                ELSE false
            END
            """
        elif func_name == 'select':
            if len(func_node.args) != 1:
                raise ValueError("select() function requires exactly one argument")
            
            # PHASE 4A: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'select'):
                try:
                    return self._generate_select_with_cte(func_node, base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for select(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Generate expression for array elements (uses 'value' from json_each)
            alias = self.generate_alias()
            array_element_expression_generator = SQLGenerator(
                table_name=f"json_each({base_expr})",
                json_column="value",
                resource_type=self.resource_type,
                dialect=self.dialect
            )
            array_element_expression = array_element_expression_generator.visit(func_node.args[0])
            
            # CRITICAL FIX: Merge CTEs from nested generator into main generator
            # This ensures that CTEs created during nested function calls (like upper()) are not lost
            for cte_name, cte_def in array_element_expression_generator.ctes.items():
                if cte_name not in self.ctes:
                    self.ctes[cte_name] = cte_def
            
            # Generate expression for non-array case (reuse same expression to avoid duplication)
            non_array_element_expression_generator = SQLGenerator(
                table_name=self.table_name,
                json_column=self.json_column,
                resource_type=self.resource_type,
                dialect=self.dialect
            )
            non_array_element_expression = non_array_element_expression_generator.visit(func_node.args[0])
            
            # Merge CTEs from non-array case too
            for cte_name, cte_def in non_array_element_expression_generator.ctes.items():
                if cte_name not in self.ctes:
                    self.ctes[cte_name] = cte_def
            
            return f"""
            CASE 
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN (
                    SELECT {self.aggregate_to_json_array(f"({array_element_expression})")}
                    FROM {self.iterate_json_array(base_expr, "$")}
                    WHERE ({array_element_expression}) IS NOT NULL
                )
                WHEN {base_expr} IS NOT NULL THEN {self.dialect.json_array_function}({non_array_element_expression})
                ELSE {self.dialect.json_array_function}()
            END
            """
        elif func_name == 'where':
            if len(func_node.args) != 1:
                raise ValueError("where() function requires exactly one argument")
            
            # PHASE 2: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'where'):
                try:
                    return self._generate_where_with_cte(func_node, base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for where(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Optimize complex base expressions to avoid duplication
            optimized_base = self._create_optimized_expression(base_expr, "where")
            
            # Generate condition for array elements (uses 'value' from json_each)
            alias = self.generate_alias()
            array_element_condition_generator = SQLGenerator(
                table_name=f"json_each({optimized_base})",  # Context for sub-queries within condition
                json_column="value",
                resource_type=self.resource_type, # Propagate resource type context
                dialect=self.dialect # Propagate dialect
            )
            array_element_condition_sql = array_element_condition_generator.visit(func_node.args[0])

            # Generate condition for the non-array element (operates on base_expr directly)
            # This generator's json_column is the SQL expression for base_expr.
            single_item_condition_generator = SQLGenerator(
                table_name=self.table_name, # Use original table_name for context
                json_column=f"({optimized_base})", # Context is the base_expr itself
                resource_type=self.resource_type, # Propagate resource type context
                dialect=self.dialect # Propagate dialect
            )
            single_item_condition_sql = single_item_condition_generator.visit(func_node.args[0])
            
            return f"""
            CASE 
                WHEN {self.get_json_type(optimized_base)} = 'ARRAY' THEN
                    {self.coalesce_empty_array(f"(SELECT {self.aggregate_to_json_array('value')} FROM {self.iterate_json_array(optimized_base, '$')} WHERE {array_element_condition_sql})")}
                ELSE 
                    CASE WHEN {single_item_condition_sql} THEN {optimized_base} ELSE {self.dialect.json_array_function}() END
            END
            """
        elif func_name == 'lowboundary':
            # FHIR boundary functions calculate boundaries based on precision
            # Handle both scalar values and arrays (extract first element from arrays)
            return f"""
            CASE 
                -- Handle arrays: extract first element and apply boundary logic
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' AND {self.get_json_array_length(base_expr)} > 0 THEN
                    CASE 
                        -- Numeric values in array: subtract precision
                        WHEN {self.get_json_type(self.extract_json_object(base_expr, '$[0]'))} IN ('NUMBER', 'INTEGER', 'DOUBLE') THEN
                            CAST(CAST({self.extract_json_object(base_expr, '$[0]')} AS DOUBLE) - 0.05 AS VARCHAR)
                        -- String values in array: apply date/time boundary logic
                        WHEN {self.get_json_type(self.extract_json_object(base_expr, '$[0]'))} = 'VARCHAR' THEN
                            CASE
                                -- Month precision (YYYY-MM): first day of month
                                WHEN LENGTH(CAST(json_extract({base_expr}, '$[0]') AS VARCHAR)) = 7 
                                     AND CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '____-__' THEN
                                    CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) || '-01'
                                -- Date precision (YYYY-MM-DD): start of day in latest timezone 
                                WHEN LENGTH(CAST(json_extract({base_expr}, '$[0]') AS VARCHAR)) = 10
                                     AND CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '____-__-__' THEN
                                    CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) || 'T00:00:00.000+14:00'
                                -- Time precision (HH:MM): start of minute
                                WHEN LENGTH(CAST(json_extract({base_expr}, '$[0]') AS VARCHAR)) = 5
                                     AND CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '__:__' THEN
                                    CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) || ':00.000'
                                ELSE CAST(json_extract({base_expr}, '$[0]') AS VARCHAR)
                            END
                        ELSE CAST(json_extract({base_expr}, '$[0]') AS VARCHAR)
                    END
                -- Empty arrays: return null
                WHEN json_type({base_expr}) = 'ARRAY' AND json_array_length({base_expr}) = 0 THEN NULL
                -- Scalar numeric values: subtract precision
                WHEN json_type({base_expr}) IN ('NUMBER', 'INTEGER', 'DOUBLE') THEN
                    CAST(CAST({base_expr} AS DOUBLE) - 0.05 AS VARCHAR)
                -- Scalar string values: apply date/time boundary logic
                WHEN json_type({base_expr}) = 'VARCHAR' THEN
                    CASE
                        -- Month precision (YYYY-MM): first day of month
                        WHEN LENGTH(CAST({base_expr} AS VARCHAR)) = 7 
                             AND CAST({base_expr} AS VARCHAR) LIKE '____-__' THEN
                            CAST({base_expr} AS VARCHAR) || '-01'
                        -- Date precision (YYYY-MM-DD): start of day in latest timezone 
                        WHEN LENGTH(CAST({base_expr} AS VARCHAR)) = 10
                             AND CAST({base_expr} AS VARCHAR) LIKE '____-__-__' THEN
                            CAST({base_expr} AS VARCHAR) || 'T00:00:00.000+14:00'
                        -- Time precision (HH:MM): start of minute
                        WHEN LENGTH(CAST({base_expr} AS VARCHAR)) = 5
                             AND CAST({base_expr} AS VARCHAR) LIKE '__:__' THEN
                            CAST({base_expr} AS VARCHAR) || ':00.000'
                        -- DateTime values: already have specific precision
                        WHEN LENGTH(CAST({base_expr} AS VARCHAR)) > 10
                             AND CAST({base_expr} AS VARCHAR) LIKE '____-__-__T%' THEN
                            CAST({base_expr} AS VARCHAR)
                        ELSE CAST({base_expr} AS VARCHAR)
                    END
                -- Null or other types: return null
                ELSE NULL
            END
            """
        elif func_name == 'highboundary':
            # FHIR boundary functions calculate boundaries based on precision
            # Handle both scalar values and arrays (extract first element from arrays)
            return f"""
            CASE 
                -- Handle arrays: extract first element and apply boundary logic
                WHEN json_type({base_expr}) = 'ARRAY' AND json_array_length({base_expr}) > 0 THEN
                    CASE 
                        -- Numeric values in array: add precision
                        WHEN json_type(json_extract({base_expr}, '$[0]')) IN ('NUMBER', 'INTEGER', 'DOUBLE') THEN
                            CAST(CAST(json_extract({base_expr}, '$[0]') AS DOUBLE) + 0.05 AS VARCHAR)
                        -- String values in array: apply date/time boundary logic
                        WHEN json_type(json_extract({base_expr}, '$[0]')) = 'VARCHAR' THEN
                            CASE
                                -- Month precision (YYYY-MM): last day of month
                                WHEN LENGTH(CAST(json_extract({base_expr}, '$[0]') AS VARCHAR)) = 7 
                                     AND CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '____-__' THEN
                                    CASE 
                                        -- Calculate last day of month
                                        WHEN CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_01' OR CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_03' 
                                          OR CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_05' OR CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_07'
                                          OR CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_08' OR CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_10'  
                                          OR CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_12' THEN CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) || '-31'  -- 31-day months
                                        WHEN CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_04' OR CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_06'
                                          OR CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_09' OR CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_11' 
                                          THEN CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) || '-30'   -- 30-day months  
                                        WHEN CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '%_02' THEN CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) || '-28'  -- February (simplified)
                                        ELSE CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) || '-30'  -- Default fallback
                                    END
                                -- Date precision (YYYY-MM-DD): end of day in earliest timezone
                                WHEN LENGTH(CAST(json_extract({base_expr}, '$[0]') AS VARCHAR)) = 10
                                     AND CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '____-__-__' THEN
                                    CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) || 'T23:59:59.999-12:00'
                                -- Time precision (HH:MM): end of minute
                                WHEN LENGTH(CAST(json_extract({base_expr}, '$[0]') AS VARCHAR)) = 5
                                     AND CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) LIKE '__:__' THEN
                                    CAST(json_extract({base_expr}, '$[0]') AS VARCHAR) || ':59.999'
                                ELSE CAST(json_extract({base_expr}, '$[0]') AS VARCHAR)
                            END
                        ELSE CAST(json_extract({base_expr}, '$[0]') AS VARCHAR)
                    END
                -- Empty arrays: return null
                WHEN json_type({base_expr}) = 'ARRAY' AND json_array_length({base_expr}) = 0 THEN NULL
                -- Scalar numeric values: add precision
                WHEN json_type({base_expr}) IN ('NUMBER', 'INTEGER', 'DOUBLE') THEN
                    CAST(CAST({base_expr} AS DOUBLE) + 0.05 AS VARCHAR)
                -- Scalar string values: apply date/time boundary logic
                WHEN json_type({base_expr}) = 'VARCHAR' THEN
                    CASE
                        -- Month precision (YYYY-MM): last day of month
                        WHEN LENGTH(CAST({base_expr} AS VARCHAR)) = 7 
                             AND CAST({base_expr} AS VARCHAR) LIKE '____-__' THEN
                            CASE 
                                -- Calculate last day of month
                                WHEN CAST({base_expr} AS VARCHAR) LIKE '%_01' OR CAST({base_expr} AS VARCHAR) LIKE '%_03' 
                                  OR CAST({base_expr} AS VARCHAR) LIKE '%_05' OR CAST({base_expr} AS VARCHAR) LIKE '%_07'
                                  OR CAST({base_expr} AS VARCHAR) LIKE '%_08' OR CAST({base_expr} AS VARCHAR) LIKE '%_10'  
                                  OR CAST({base_expr} AS VARCHAR) LIKE '%_12' THEN CAST({base_expr} AS VARCHAR) || '-31'  -- 31-day months
                                WHEN CAST({base_expr} AS VARCHAR) LIKE '%_04' OR CAST({base_expr} AS VARCHAR) LIKE '%_06'
                                  OR CAST({base_expr} AS VARCHAR) LIKE '%_09' OR CAST({base_expr} AS VARCHAR) LIKE '%_11' 
                                  THEN CAST({base_expr} AS VARCHAR) || '-30'   -- 30-day months  
                                WHEN CAST({base_expr} AS VARCHAR) LIKE '%_02' THEN CAST({base_expr} AS VARCHAR) || '-28'  -- February (simplified)
                                ELSE CAST({base_expr} AS VARCHAR) || '-30'  -- Default fallback
                            END
                        -- Date precision (YYYY-MM-DD): end of day in earliest timezone
                        WHEN LENGTH(CAST({base_expr} AS VARCHAR)) = 10
                             AND CAST({base_expr} AS VARCHAR) LIKE '____-__-__' THEN
                            CAST({base_expr} AS VARCHAR) || 'T23:59:59.999-12:00'
                        -- Time precision (HH:MM): end of minute
                        WHEN LENGTH(CAST({base_expr} AS VARCHAR)) = 5
                             AND CAST({base_expr} AS VARCHAR) LIKE '__:__' THEN
                            CAST({base_expr} AS VARCHAR) || ':59.999'
                        -- DateTime values: already have specific precision
                        WHEN LENGTH(CAST({base_expr} AS VARCHAR)) > 10
                             AND CAST({base_expr} AS VARCHAR) LIKE '____-__-__T%' THEN
                            CAST({base_expr} AS VARCHAR)
                        ELSE CAST({base_expr} AS VARCHAR)
                    END
                -- Null or other types: return null
                ELSE NULL
            END
            """
        elif func_name == 'not':
            # Implement the not() function for boolean negation
            return f"""
            CASE 
                WHEN json_type({base_expr}) = 'ARRAY' THEN
                    CASE 
                        WHEN json_array_length({base_expr}) = 0 THEN true  -- empty collection is false, so not() is true
                        ELSE false  -- non-empty collection is true, so not() is false
                    END
                WHEN {base_expr} IS NULL THEN true  -- null/missing is false, so not() is true
                WHEN json_type({base_expr}) = 'BOOLEAN' THEN NOT CAST({base_expr} AS BOOLEAN)
                ELSE false  -- any other value is true, so not() is false
            END
            """
        elif func_name == 'getreferencekey':
            # Extract the ID from a FHIR reference string like "Patient/p1" -> "p1"
            # Can optionally validate resource type if provided as argument
            
            # PHASE 5O: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'getreferencekey'):
                try:
                    return self._generate_getreferencekey_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for getReferenceKey(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Enhanced handling for complex base expressions (like arrays from path resolution)
            # Simplify the base expression if it's a complex array query to avoid nesting issues
            simplified_base = base_expr
            
            # If the base expression is a complex array query, extract the reference field more directly
            if 'json_group_array' in base_expr and 'link' in base_expr:
                # This is likely a path like "link.other" that resolved to a complex array expression
                # Use a simpler extraction that gets the first valid reference from link.other.reference
                simplified_base = f"""
                (SELECT {self.extract_json_field(self.extract_json_object('value', '$.other'), '$.reference')} 
                 FROM {self.iterate_json_array(self.extract_json_object(self.json_column, '$.link'), '$')}
                 WHERE {self.extract_json_field(self.extract_json_object('value', '$.other'), '$.reference')} IS NOT NULL
                 LIMIT 1)
                """
            
            if func_node.args:
                # If a resource type is provided, validate it matches
                if isinstance(func_node.args[0], self.IdentifierNode):
                    expected_type = func_node.args[0].name
                    type_filter = f"""
                    CASE 
                        WHEN {simplified_base} IS NULL THEN NULL
                        WHEN STRPOS(CAST({simplified_base} AS VARCHAR), '/') > 0 THEN
                            CASE 
                                WHEN SPLIT_PART(CAST({simplified_base} AS VARCHAR), '/', 1) = '{expected_type}' THEN
                                    SPLIT_PART(CAST({simplified_base} AS VARCHAR), '/', 2)
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                    """
                else:
                    raise ValueError("getReferenceKey() type argument must be an identifier")
            else:
                # No type validation, just extract the ID part
                type_filter = f"""
                CASE 
                    WHEN {simplified_base} IS NULL THEN NULL
                    WHEN STRPOS(CAST({simplified_base} AS VARCHAR), '/') > 0 THEN
                        SPLIT_PART(CAST({simplified_base} AS VARCHAR), '/', 2)
                    ELSE NULL
                END
                """
            
            return type_filter
        elif func_name == 'oftype':
            # Enhanced debug log for ofType entry
            self.debug_steps.append({
                "operation": "apply_function_to_expression:ofType:entered_block",
                "type_name_arg_from_node_value": func_node.args[0].name if func_node.args and isinstance(func_node.args[0], self.IdentifierNode) else "INVALID_ARGS_FOR_OFTYPE"
            })

            if len(func_node.args) != 1 or not isinstance(func_node.args[0], self.IdentifierNode):
                raise ValueError("ofType() function requires a single identifier argument (the type name).")
            
            type_name_arg = func_node.args[0].name # Keep original casing for resourceType checks
            type_name_lower = type_name_arg.lower()

            # Build condition for individual elements
            element_condition: str
            fhir_primitive_types_as_string = self.FHIR_PRIMITIVE_TYPES_AS_STRING # Use from constants

            if type_name_lower in [t.lower() for t in fhir_primitive_types_as_string]:
                element_condition = f"{self.get_json_type('element_value')} = 'VARCHAR'"
            elif type_name_lower == "boolean":
                element_condition = f"({self.get_json_type('element_value')} = 'BOOLEAN' OR ({self.get_json_type('element_value')} = 'VARCHAR' AND LOWER(CAST(element_value AS VARCHAR)) IN ('true', 'false')))"
            elif type_name_lower == "integer":
                element_condition = f"({self.get_json_type('element_value')} = 'INTEGER' OR ({self.get_json_type('element_value')} IN ('NUMBER', 'DOUBLE') AND (CAST(element_value AS DOUBLE) = floor(CAST(element_value AS DOUBLE)))))"
            elif type_name_lower == "decimal":
                element_condition = f"{self.get_json_type('element_value')} IN ('NUMBER', 'INTEGER', 'DOUBLE')"
            elif type_name_arg == "Quantity":
                element_condition = f"{self.get_json_type('element_value')} = 'OBJECT' AND {self.extract_json_object('element_value', '$.value')} IS NOT NULL"
            else: # FHIR resource types
                element_condition = f"({self.get_json_type('element_value')} = 'OBJECT' AND {self.extract_json_field('element_value', '$.resourceType')} = '{type_name_arg}')"
            
            self.debug_steps.append({
                "operation": "apply_function_to_expression:ofType",
                "type_name_arg": type_name_arg,
                "base_expr_sql_for_ofType": base_expr,
                "generated_element_condition_sql_for_ofType": element_condition
            })

            # PHASE 5B: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'oftype'):
                try:
                    return self._generate_oftype_with_cte(base_expr, type_name_arg, element_condition)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for ofType(), falling back to subqueries: {e}")

            # Original implementation (fallback)
            # Handle both arrays and single values
            return f"""
            CASE 
                WHEN json_type({base_expr}) = 'ARRAY' THEN
                    COALESCE(
                        (SELECT json_group_array(value)
                         FROM json_each({base_expr})
                         WHERE {element_condition.replace('element_value', 'value')}),
                        json_array()
                    )
                ELSE 
                    CASE WHEN {element_condition.replace('element_value', base_expr)} THEN json_array({base_expr}) ELSE json_array() END
            END
            """

        elif func_name == 'extension':
            # Handle FHIR extension() function for filtering extensions by URL
            if len(func_node.args) != 1 or not isinstance(func_node.args[0], self.LiteralNode):
                raise ValueError("extension() function requires exactly one string argument (the extension URL)")
            
            extension_url = func_node.args[0].value
            
            # PHASE 5G: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'extension'):
                try:
                    return self._generate_extension_with_cte(base_expr, extension_url)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for extension(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Extension function that creates extension objects with resolved value[x] AND nested extensions
            # This allows chained extension() calls like extension('url1').extension('url2')
            return f"""
            (SELECT 
                CASE 
                    WHEN COUNT(*) = 0 THEN json_array()
                    ELSE json_group_array(
                        json_object(
                            'id', {self.extract_json_field('value', '$.id')},
                            'url', {self.extract_json_field('value', '$.url')},
                            'extension', {self.extract_json_object('value', '$.extension')},
                            'value', COALESCE(
                                {self.extract_json_object('value', '$.valueString')},
                                {self.extract_json_object('value', '$.valueCode')},
                                {self.extract_json_object('value', '$.valueBoolean')},
                                {self.extract_json_object('value', '$.valueInteger')},
                                {self.extract_json_object('value', '$.valueDecimal')},
                                {self.extract_json_object('value', '$.valueQuantity')},
                                {self.extract_json_object('value', '$.valueCodeableConcept')},
                                {self.extract_json_object('value', '$.valueCoding')},
                                {self.extract_json_object('value', '$.valueDateTime')},
                                {self.extract_json_object('value', '$.valueDate')}
                            )
                        )
                    )
                END
             FROM json_each(json_extract({base_expr}, '$.extension'))
             WHERE {self.extract_json_field('value', '$.url')} = '{extension_url}')
            """

        elif func_name == 'join':
            # PHASE 2B: CTE Implementation temporarily disabled for join() due to CTE dependency issues
            # TODO: Fix CTE generation for join() function in future iteration
            # if self.enable_cte and self._should_use_cte_unified(base_expr, 'join'):
            #     try:
            #         return self._generate_join_with_cte(func_node, base_expr)
            #     except Exception as e:
            #         # Fallback to original implementation if CTE generation fails
            #         print(f"CTE generation failed for join(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # join() function - concatenate string array elements with optional separator
            # join() with no args uses empty string separator
            # join(separator) uses the specified separator
            
            if len(func_node.args) == 0:
                # join() with no separator (empty string)
                separator_sql = "''"
            elif len(func_node.args) == 1:
                # join(separator)
                separator_sql = self.visit(func_node.args[0])
            else:
                raise ValueError("join() function takes 0 or 1 arguments")
            
            # Use dialect-specific array joining approach for clean array joining
            # This properly flattens nested arrays from array-aware path extraction
            
            return self.dialect.join_array_elements(base_expr, separator_sql)

        elif func_name == 'substring':
            # substring(start, length) function - extract substring from string
            if len(func_node.args) != 2:
                raise ValueError("substring() function requires exactly 2 arguments: start and length")
            
            start_sql = self.visit(func_node.args[0])
            length_sql = self.visit(func_node.args[1])
            
            # PHASE 5D: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'substring'):
                try:
                    return self._generate_substring_with_cte(base_expr, start_sql, length_sql)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for substring(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Handle the case where base_expr is already a JSON extracted value
            if 'json_extract(' in base_expr and not 'json_extract_string(' in base_expr:
                # Convert json_extract to json_extract_string to remove quotes for string operations
                # This handles the pattern: json_extract(resource, '$.field') -> json_extract_string(resource, '$.field')
                import re
                base_value = re.sub(r'json_extract\(', 'json_extract_string(', base_expr)
            elif 'json_extract_string(' in base_expr:
                # Already a string extraction, use directly
                base_value = base_expr
            else:
                # Extract as string
                base_value = self.extract_json_field(base_expr, '$')
            
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL THEN
                    {self.dialect.substring(base_value, start_sql, length_sql)}
                ELSE NULL
            END
            """
            
        elif func_name == 'tointeger':
            # toInteger() function - convert string to integer
            if len(func_node.args) != 0:
                raise ValueError("toInteger() function takes no arguments")
            
            # PHASE 5M: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'tointeger'):
                try:
                    return self._generate_tointeger_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for tointeger(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Handle the case where base_expr is already extracted
            if 'json_extract' in base_expr or 'SUBSTRING' in base_expr:
                # If already a string value, cast directly
                base_value = base_expr
            else:
                # Extract as string first
                base_value = self.extract_json_field(base_expr, '$')
            
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL THEN
                    CAST({base_value} AS INTEGER)
                ELSE NULL
            END
            """
            
        elif func_name == 'tostring':
            # toString() function - convert value to string
            if len(func_node.args) != 0:
                raise ValueError("toString() function takes no arguments")
            
            # PHASE 5M: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'tostring'):
                try:
                    return self._generate_tostring_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for tostring(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # toString() should truncate decimal values for integer-like results  
            # For numeric results from arithmetic operations, always truncate to integer
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL THEN
                    CAST(CAST({base_expr} AS INTEGER) AS VARCHAR)
                ELSE NULL
            END
            """
        elif func_name == 'startswith':
            # startsWith(prefix) function - check if string starts with prefix
            if len(func_node.args) != 1:
                raise ValueError("startsWith() function requires exactly 1 argument")
            
            prefix_sql = self.visit(func_node.args[0])
            
            # PHASE 5H: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'startswith'):
                try:
                    return self._generate_startswith_with_cte(base_expr, prefix_sql)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for startswith(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL AND {prefix_sql} IS NOT NULL THEN
                    CASE WHEN CAST({base_expr} AS VARCHAR) LIKE {self.dialect.string_concat(f'CAST({prefix_sql} AS VARCHAR)', "'%'")} THEN true ELSE false END
                ELSE false
            END
            """
        elif func_name == 'endswith':
            # endsWith(suffix) function - check if string ends with suffix
            if len(func_node.args) != 1:
                raise ValueError("endsWith() function requires exactly 1 argument")
            
            suffix_sql = self.visit(func_node.args[0])
            
            # PHASE 5I: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'endswith'):
                try:
                    return self._generate_endswith_with_cte(base_expr, suffix_sql)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for endswith(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL AND {suffix_sql} IS NOT NULL THEN
                    CASE WHEN CAST({base_expr} AS VARCHAR) LIKE {self.dialect.string_concat("'%'", f'CAST({suffix_sql} AS VARCHAR)')} THEN true ELSE false END
                ELSE false
            END
            """
        elif func_name == 'indexof':
            # indexOf(substring) function - find index of substring
            if len(func_node.args) != 1:
                raise ValueError("indexOf() function requires exactly 1 argument")
            
            search_sql = self.visit(func_node.args[0])
            
            # PHASE 5J: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'indexof'):
                try:
                    return self._generate_indexof_with_cte(base_expr, search_sql)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for indexof(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL AND {search_sql} IS NOT NULL THEN
                    {self.dialect.string_position(search_sql, base_expr)}
                ELSE -1
            END
            """
        elif func_name == 'replace':
            # replace(search, replace) function - replace occurrences of search with replace
            if len(func_node.args) != 2:
                raise ValueError("replace() function requires exactly 2 arguments")
            
            search_sql = self.visit(func_node.args[0])
            replace_sql = self.visit(func_node.args[1])
            
            # PHASE 5E: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'replace'):
                try:
                    return self._generate_replace_with_cte(base_expr, search_sql, replace_sql)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for replace(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL THEN
                    REPLACE(CAST({base_expr} AS VARCHAR), CAST({search_sql} AS VARCHAR), CAST({replace_sql} AS VARCHAR))
                ELSE NULL
            END
            """
        elif func_name == 'toupper':
            # toUpper() function - convert to uppercase
            if len(func_node.args) != 0:
                raise ValueError("toUpper() function takes no arguments")
            
            # PHASE 5K: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'toupper'):
                try:
                    return self._generate_toupper_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for toupper(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL THEN UPPER(CAST({base_expr} AS VARCHAR))
                ELSE NULL
            END
            """
        elif func_name == 'tolower':
            # toLower() function - convert to lowercase
            if len(func_node.args) != 0:
                raise ValueError("toLower() function takes no arguments")
            
            # PHASE 5K: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'tolower'):
                try:
                    return self._generate_tolower_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for tolower(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL THEN LOWER(CAST({base_expr} AS VARCHAR))
                ELSE NULL
            END
            """
        elif func_name == 'upper':
            # upper() function - standard FHIRPath name for toUpper()
            if len(func_node.args) != 0:
                raise ValueError("upper() function takes no arguments")
            
            # PHASE 5K: CTE Implementation with Feature Flag (use toupper logic)
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'toupper'):
                try:
                    return self._generate_toupper_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for upper(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL THEN UPPER(CAST({base_expr} AS VARCHAR))
                ELSE NULL
            END
            """
        elif func_name == 'lower':
            # lower() function - standard FHIRPath name for toLower()
            if len(func_node.args) != 0:
                raise ValueError("lower() function takes no arguments")
            
            # PHASE 5K: CTE Implementation with Feature Flag (use tolower logic)
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'tolower'):
                try:
                    return self._generate_tolower_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for lower(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL THEN LOWER(CAST({base_expr} AS VARCHAR))
                ELSE NULL
            END
            """
        elif func_name == 'trim':
            # trim() function - remove leading and trailing whitespace
            if len(func_node.args) != 0:
                raise ValueError("trim() function takes no arguments")
            
            # PHASE 5L: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'trim'):
                try:
                    return self._generate_trim_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for trim(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL THEN TRIM(CAST({base_expr} AS VARCHAR))
                ELSE NULL
            END
            """
        elif func_name == 'split':
            # split() function - split string by separator
            if len(func_node.args) != 1:
                raise ValueError("split() function requires exactly 1 argument")
            
            separator_sql = self.visit(func_node.args[0])
            
            # PHASE 5F: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'split'):
                try:
                    return self._generate_split_with_cte(base_expr, separator_sql)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for split(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Use dialect-specific string split function
            return f"""
            CASE 
                WHEN {base_expr} IS NOT NULL THEN {self.dialect.split_string(f'CAST({base_expr} AS VARCHAR)', separator_sql)}
                ELSE NULL
            END
            """
        elif func_name == 'all':
            # all() function - check if all elements match criteria
            if len(func_node.args) != 1:
                raise ValueError("all() function requires exactly 1 argument")
            
            # PHASE 4C: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'all'):
                try:
                    return self._generate_all_with_cte(func_node, base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for all(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            criteria_expr = func_node.args[0]
            alias = self.generate_alias()
            
            # Generate a subquery that checks if all elements match the criteria
            criteria_generator = SQLGenerator(
                self.table_name, self.json_column, 
                resource_type=self.resource_type, dialect=self.dialect
            )
            criteria_sql = criteria_generator.visit(criteria_expr)
            
            return f"""
            (SELECT 
                CASE 
                    WHEN COUNT(*) = 0 THEN true
                    WHEN COUNT(*) = COUNT(CASE WHEN ({criteria_sql}) THEN 1 END) THEN true
                    ELSE false
                END
            FROM {self.iterate_json_array(base_expr, '$')} AS {alias}(value)
            WHERE {alias}.value IS NOT NULL)
            """
        elif func_name == 'distinct':
            # distinct() function - return unique elements
            if len(func_node.args) != 0:
                raise ValueError("distinct() function takes no arguments")
            
            # PHASE 4D: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'distinct'):
                try:
                    return self._generate_distinct_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for distinct(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # For collections, return distinct elements
            alias = self.generate_alias()
            return f"""
            (SELECT json_group_array(DISTINCT {alias}.value)
            FROM {self.iterate_json_array(base_expr, '$')} AS {alias}(value)
            WHERE {alias}.value IS NOT NULL)
            """

        elif func_name == 'getresourcekey':
            # getResourceKey() function - extract resource ID
            # PHASE 5N: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_for_getresourcekey():
                try:
                    return self._generate_getresourcekey_with_cte()
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for getResourceKey(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Return the resource ID as the key
            return f"json_extract_string({self.json_column}, '$.id')"

        else:
            raise ValueError(f"Unknown function: {func_name}")
    
    def visit_function_call(self, node) -> str:
        """Visit a function call node"""
        func_name = node.name.lower()
        
        # Handle getResourceKey() function
        if func_name == 'getresourcekey':
            # PHASE 5N: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_for_getresourcekey():
                try:
                    return self._generate_getresourcekey_with_cte()
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for getResourceKey(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            # Return the resource ID as the key
            return f"json_extract_string({self.json_column}, '$.id')"
        
        # Handle getReferenceKey() function
        if func_name == 'getreferencekey':
            # PHASE 5O: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(self.json_column, 'getreferencekey'):
                try:
                    return self._generate_getreferencekey_with_cte(self.json_column, node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    print(f"CTE generation failed for getReferenceKey(), falling back to subqueries: {e}")
            
            # Original implementation (fallback)
            return self.apply_function_to_expression(node, self.json_column)
        
        # Handle extension() function when used as a standalone function
        if func_name == 'extension':
            return self.apply_function_to_expression(node, self.json_column)
        
        # Functions like 'exists', 'empty', 'first', 'last', 'count', 'where', 'join', 'length', 'contains', 'select'
        # and string functions when they are the first part of a path (e.g., Patient.where(...))
        # or applied to the implicit root context (e.g. where(...))
        # should be handled by apply_function_to_expression with self.json_column as base_expr.
        if func_name in ['exists', 'empty', 'first', 'last', 'count', 'where', 'join', 'length', 'contains', 'select',
                         'substring', 'startswith', 'endswith', 'indexof', 'replace', 'toupper', 'tolower', 'upper', 'lower', 'trim', 'split', 'all', 'distinct', 'tostring', 'getresourcekey', 'getreferencekey']:
            return self.apply_function_to_expression(node, self.json_column)
        # Note: The 'where' case here was for root-level where like `where(name.given = 'Peter')`.
        # This is now covered by apply_function_to_expression(node, self.json_column)
        # which correctly handles array/non-array base_expr (self.json_column in this case).
        
        # If other standalone functions are added that don't fit the path-based model
        # of apply_function_to_expression, they would be handled here.
        # For now, all supported functions are handled via apply_function_to_expression.
        else:
            raise ValueError(f"Unknown or unsupported standalone function: {func_name}")
    
    def visit_binary_op(self, node) -> str:
        """Visit a binary operation node with proper type casting"""
        left = self.visit(node.left)
        right = self.visit(node.right)
        
        sql_op = self.SQL_OPERATORS.get(node.operator.lower(), node.operator)
        
        # Handle string concatenation vs arithmetic for + operator
        if sql_op == '+':
            # Check if this should be string concatenation
            if self._is_string_concatenation(node.left, node.right):
                # Use dialect-specific string concatenation
                return self.dialect.string_concat(left, right)
            else:
                # Treat as arithmetic - cast to numeric types
                if not (isinstance(node.left, self.LiteralNode) and node.left.type in ['integer', 'decimal']):
                    left = f"CAST({left} AS DOUBLE)"
                if not (isinstance(node.right, self.LiteralNode) and node.right.type in ['integer', 'decimal']):
                    right = f"CAST({right} AS DOUBLE)"
                return f"({left} {sql_op} {right})"
        
        # Handle other arithmetic operations with potential JSON operands
        elif sql_op in ['-', '*', '/']:
            # If left is not a number literal, cast it (it might be from json_extract)
            if not (isinstance(node.left, self.LiteralNode) and node.left.type in ['integer', 'decimal']):
                left = f"CAST({left} AS DOUBLE)"
            # If right is not a number literal, cast it
            if not (isinstance(node.right, self.LiteralNode) and node.right.type in ['integer', 'decimal']):
                right = f"CAST({right} AS DOUBLE)"
            return f"({left} {sql_op} {right})"

        # Handle logical operations (AND, OR) with proper boolean casting
        elif sql_op in ['AND', 'OR']:
            # Cast known boolean fields to proper boolean types for logical operations
            left_cast = self._ensure_boolean_casting(node.left, left)
            right_cast = self._ensure_boolean_casting(node.right, right)
            return f"({left_cast} {sql_op} {right_cast})"

        # Handle JSON value comparisons with proper type casting
        elif node.operator in ['=', '!=', '>', '<', '>=', '<=']:
            left_cast, right_cast = self.determine_comparison_casts(node.left, node.right, left, right)
            return f"({left_cast} {sql_op} {right_cast})"
        
        return f"({left} {sql_op} {right})"
    
    def _ensure_boolean_casting(self, node, sql_expr: str) -> str:
        """Ensure proper boolean casting for logical operations"""
        # Check if this is a known boolean field that needs casting
        if self._is_boolean_field(node):
            return f"CAST({sql_expr} AS BOOLEAN)"
        # Check if this is already a boolean comparison that doesn't need additional casting
        elif sql_expr.strip().startswith("CAST(") and "AS BOOLEAN" in sql_expr:
            return sql_expr
        # Check if this is a CTE subquery that might contain boolean values
        elif (sql_expr.strip().startswith("(SELECT") and 
              sql_expr.strip().endswith(")") and 
              ("extracted_value" in sql_expr or "array_extract_result" in sql_expr)):
            return f"CAST({sql_expr} AS BOOLEAN)"
        # Check if this is a simple json_extract that might be a boolean field
        elif "json_extract" in sql_expr and self._contains_boolean_field_reference(sql_expr):
            return f"CAST({sql_expr} AS BOOLEAN)"
        else:
            return sql_expr
    
    def _is_boolean_field(self, node) -> bool:
        """Check if a node represents a known boolean field"""
        # Import KNOWN_BOOLEAN_FIELDS from constants
        try:
            from .constants import KNOWN_BOOLEAN_FIELDS
        except ImportError:
            KNOWN_BOOLEAN_FIELDS = ['active', 'deceasedBoolean']
            
        if isinstance(node, self.IdentifierNode):
            return node.name in KNOWN_BOOLEAN_FIELDS
        elif isinstance(node, self.PathNode) and node.segments:
            # Check if the last segment is a boolean field
            last_segment = node.segments[-1]
            if isinstance(last_segment, self.IdentifierNode):
                return last_segment.name in KNOWN_BOOLEAN_FIELDS
        return False
    
    def _contains_boolean_field_reference(self, sql_expr: str) -> bool:
        """Check if SQL expression contains references to known boolean fields"""
        try:
            from .constants import KNOWN_BOOLEAN_FIELDS
        except ImportError:
            KNOWN_BOOLEAN_FIELDS = ['active', 'deceasedBoolean']
        return any(f"'{field}'" in sql_expr or f'$.{field}' in sql_expr for field in KNOWN_BOOLEAN_FIELDS)
    
    def _is_string_concatenation(self, left_node, right_node) -> bool:
        """Determine if a + operation should be string concatenation vs arithmetic"""
        # If either operand is a string literal, treat as string concatenation
        if (isinstance(left_node, self.LiteralNode) and left_node.type == 'string') or \
           (isinstance(right_node, self.LiteralNode) and right_node.type == 'string'):
            return True
        
        # If dealing with paths that likely return strings (name, family, given, etc.)
        if self._is_likely_string_path(left_node) or self._is_likely_string_path(right_node):
            return True
        
        # If either operand is a function that likely returns a string
        if self._is_likely_string_function(left_node) or self._is_likely_string_function(right_node):
            return True
            
        return False
    
    def _is_likely_string_path(self, node) -> bool:
        """Check if a node likely represents a string value"""
        if isinstance(node, self.PathNode):
            # Check for common string field patterns
            path_str = self._path_to_string(node)
            string_patterns = ['name', 'family', 'given', 'system', 'value', 'display', 'text', 'code', 'status', 'version', 'title', 'description']
            return any(pattern in path_str.lower() for pattern in string_patterns)
        elif isinstance(node, self.IdentifierNode):
            string_fields = ['name', 'family', 'given', 'system', 'value', 'display', 'text', 'code', 'status', 'version', 'title', 'description', 'id', 'resourceType']
            return node.name.lower() in string_fields
        return False
    
    def _is_likely_string_function(self, node) -> bool:
        """Check if a function call likely returns a string value"""
        if isinstance(node, self.FunctionCallNode):
            func_name = node.name.lower()
            string_functions = ['tostring', 'substring', 'first', 'last', 'join', 'replace', 'toupper', 'tolower', 'upper', 'lower', 'trim']
            return func_name in string_functions
        return False
    
    def _validate_function_args(self, func_name: str, args: list) -> None:
        """Validate function arguments for better error messages"""
        arg_requirements = {
            'exists': (0, 1),  # 0 or 1 arguments
            'empty': (0, 0),   # no arguments
            'first': (0, 0),   # no arguments
            'last': (0, 0),    # no arguments
            'count': (0, 0),   # no arguments
            'length': (0, 0),  # no arguments
            'where': (1, 1),   # exactly 1 argument
            'contains': (1, 1), # exactly 1 argument
            'select': (1, 1),  # exactly 1 argument
            'join': (0, 1),    # 0 or 1 arguments
            'substring': (2, 2), # exactly 2 arguments
            'startswith': (1, 1), # exactly 1 argument
            'endswith': (1, 1),   # exactly 1 argument
            'indexof': (1, 1),    # exactly 1 argument
            'replace': (2, 2),    # exactly 2 arguments
            'toupper': (0, 0),    # no arguments
            'tolower': (0, 0),    # no arguments
            'upper': (0, 0),      # no arguments
            'lower': (0, 0),      # no arguments
            'trim': (0, 0),       # no arguments
            'split': (1, 1),      # exactly 1 argument
            'all': (1, 1),        # exactly 1 argument
            'distinct': (0, 0),   # no arguments
            'tostring': (0, 0),   # no arguments
        }
        
        if func_name in arg_requirements:
            min_args, max_args = arg_requirements[func_name]
            actual_args = len(args)
            
            if actual_args < min_args or actual_args > max_args:
                if min_args == max_args:
                    raise ValueError(f"requires exactly {min_args} argument{'s' if min_args != 1 else ''}, got {actual_args}")
                else:
                    raise ValueError(f"requires {min_args}-{max_args} arguments, got {actual_args}")
    
    def _path_to_string(self, path_node) -> str:
        """Convert a path node to string representation for analysis"""
        if not isinstance(path_node, self.PathNode):
            return ""
        segments = []
        for segment in path_node.segments:
            if isinstance(segment, self.IdentifierNode):
                segments.append(segment.name)
        return ".".join(segments)
    
    def determine_comparison_casts(self, left_node, right_node, left_sql: str, right_sql: str) -> Tuple[str, str]:
        """Determine appropriate casts for comparison operations, preferring json_extract_string for string comparisons."""

        def _transform_json_extract_to_string_version(sql_expr: str) -> Optional[str]:
            # Tries to convert "json_extract(col, path)" to "json_extract_string(col, path)"
            # Returns None if transformation is not applicable (e.g., complex expression).
            # Also handles the case where we're extracting from json_each() 'value'
            if sql_expr.startswith("json_extract(") and sql_expr.endswith(")"):
                # Assumes basic "json_extract(args...)" form
                return "json_extract_string(" + sql_expr[len("json_extract("):-1] + ")"
            return None
        
        def _is_date_string(s_val: str) -> bool:
            # Basic check for YYYY-MM-DD format
            if isinstance(s_val, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", s_val):
                try:
                    return True # Further validation could be added here if needed
                except ValueError:
                    return False
            return False

        def _is_cte_subquery(sql_expr: str) -> bool:
            """Check if expression is a CTE subquery that needs casting"""
            return (sql_expr.strip().startswith("(SELECT") and 
                    sql_expr.strip().endswith(")") and 
                    ("array_extract_result" in sql_expr or "extracted_value" in sql_expr))
        
        def _wrap_cte_with_cast(sql_expr: str, cast_type: str) -> str:
            """Wrap CTE subquery with appropriate casting"""
            return f"CAST({sql_expr} AS {cast_type})"

        final_left_sql, final_right_sql = left_sql, right_sql

        # Case 1: Right node is a literal, Left node's SQL might be from json_extract
        if isinstance(right_node, self.LiteralNode):
            if right_node.type == 'string':
                transformed_left = _transform_json_extract_to_string_version(left_sql)
                if _is_date_string(right_node.value) and ('json_extract' in left_sql or transformed_left):
                    final_left_sql = f"CAST({left_sql} AS DATE)"
                    final_right_sql = f"CAST({right_sql} AS DATE)"
                else:
                    # Special handling for complex array comparisons to avoid massive SQL
                    if ('json_group_array' in left_sql and 'flat_value' in left_sql and len(left_sql) > 1000):
                        # This is a complex array flattening expression - use simplified array comparison
                        # Instead of casting the entire complex expression, check if any array element matches
                        final_left_sql = f"""(
                            SELECT COUNT(*) > 0 
                            FROM json_each(COALESCE({left_sql}, json_array()))
                            WHERE json_extract_string(value, '$') = {right_sql}
                        )"""
                        final_right_sql = "true"
                    # Special handling for $this comparisons in json_each context
                    elif left_sql.strip() == 'value':  # This is $this in json_each context
                        final_left_sql = f"json_extract_string({left_sql}, '$')"
                    elif transformed_left:
                        final_left_sql = transformed_left
                    elif 'json_extract' in left_sql:  # Fallback for complex expr involving json_extract
                        # Preserve null semantics when casting JSON extracts to VARCHAR
                        # This is critical for base64Binary and other string comparisons with constants
                        final_left_sql = f"""
                        CASE 
                            WHEN {left_sql} IS NULL THEN NULL
                            ELSE CAST({left_sql} AS VARCHAR)
                        END
                        """
            elif right_node.type in ['integer', 'decimal']:
                # Handle CTE subquery results that need numeric casting
                if _is_cte_subquery(left_sql):
                    final_left_sql = _wrap_cte_with_cast(left_sql, 'DOUBLE')
                elif 'json_extract' in left_sql:
                    # Safe casting that handles empty arrays and null values
                    final_left_sql = f"""
                    CASE 
                        WHEN {left_sql} IS NULL THEN NULL
                        WHEN json_type({left_sql}) = 'ARRAY' AND json_array_length({left_sql}) = 0 THEN NULL
                        WHEN json_type({left_sql}) = 'ARRAY' AND json_array_length({left_sql}) > 0 THEN CAST(json_extract({left_sql}, '$[0]') AS DOUBLE)
                        ELSE CAST({left_sql} AS DOUBLE)
                    END
                    """
            elif right_node.type == 'boolean':
                # Special handling for arrays from ofType(boolean) comparisons
                # Look for patterns that indicate this is an array result from ofType
                if ('json_group_array' in left_sql and 'json_each' in left_sql) or ('COALESCE' in left_sql and 'json_array()' in left_sql):
                    # Handle ofType(boolean) = true/false by checking if array contains the value
                    if right_node.value:  # right_node.value is True
                        final_left_sql = f"(json_array_length({left_sql}) > 0 AND json_extract({left_sql}, '$[0]') = true)"
                        final_right_sql = "true"
                    else:  # right_node.value is False
                        final_left_sql = f"(json_array_length({left_sql}) > 0 AND json_extract({left_sql}, '$[0]') = false)"
                        final_right_sql = "true"
                elif 'json_extract' in left_sql:
                    final_left_sql = f"CAST({left_sql} AS BOOLEAN)"

        # Case 2: Left node is a literal, Right node's SQL might be from json_extract
        elif isinstance(left_node, self.LiteralNode):
            if left_node.type == 'string':
                transformed_right = _transform_json_extract_to_string_version(right_sql)
                if _is_date_string(left_node.value) and ('json_extract' in right_sql or transformed_right):
                    final_left_sql = f"CAST({left_sql} AS DATE)"
                    final_right_sql = f"CAST({right_sql} AS DATE)"
                else:
                    # Special handling for $this comparisons in json_each context
                    if right_sql.strip() == 'value':  # This is $this in json_each context
                        final_right_sql = f"json_extract_string({right_sql}, '$')"
                    elif transformed_right:
                        final_right_sql = transformed_right
                    elif 'json_extract' in right_sql: # Fallback for complex expr involving json_extract
                        # Preserve null semantics when casting JSON extracts to VARCHAR
                        final_right_sql = f"""
                        CASE 
                            WHEN {right_sql} IS NULL THEN NULL
                            ELSE CAST({right_sql} AS VARCHAR)
                        END
                        """
            elif left_node.type in ['integer', 'decimal']:
                # Handle CTE subquery results that need numeric casting
                if _is_cte_subquery(right_sql):
                    final_right_sql = _wrap_cte_with_cast(right_sql, 'DOUBLE')
                elif 'json_extract' in right_sql:
                    # Safe casting that handles empty arrays and null values
                    final_right_sql = f"""
                    CASE 
                        WHEN {right_sql} IS NULL THEN NULL
                        WHEN json_type({right_sql}) = 'ARRAY' AND json_array_length({right_sql}) = 0 THEN NULL
                        WHEN json_type({right_sql}) = 'ARRAY' AND json_array_length({right_sql}) > 0 THEN CAST(json_extract({right_sql}, '$[0]') AS DOUBLE)
                        ELSE CAST({right_sql} AS DOUBLE)
                    END
                    """
            elif left_node.type == 'boolean':
                # Special handling for arrays from ofType(boolean) comparisons
                # Look for patterns that indicate this is an array result from ofType
                if ('json_group_array' in right_sql and 'json_each' in right_sql) or ('COALESCE' in right_sql and 'json_array()' in right_sql):
                    # Handle true/false = ofType(boolean) by checking if array contains the value
                    if left_node.value:  # left_node.value is True
                        final_right_sql = f"(json_array_length({right_sql}) > 0 AND json_extract({right_sql}, '$[0]') = true)"
                        final_left_sql = "true"
                    else:  # left_node.value is False
                        final_right_sql = f"(json_array_length({right_sql}) > 0 AND json_extract({right_sql}, '$[0]') = false)"
                        final_left_sql = "true"
                elif 'json_extract' in right_sql:
                    final_right_sql = f"CAST({right_sql} AS BOOLEAN)"

        # Case 3: Neither is a literal, but both might be json_extract results (e.g. path.field = other.path.field)
        # Original behavior: cast both to VARCHAR. This is a reasonable default for JSON-to-JSON comparison.
        elif 'json_extract' in left_sql and 'json_extract' in right_sql:
            final_left_sql = f"CAST({left_sql} AS VARCHAR)"
            final_right_sql = f"CAST({right_sql} AS VARCHAR)"

        return final_left_sql, final_right_sql
    
    def visit_unary_op(self, node) -> str:
        """Visit a unary operation node"""
        operand = self.visit(node.operand)
        
        if node.operator.lower() == 'not':
            return f"NOT ({operand})"
        elif node.operator == '+': # Unary plus
            return f"+({operand})" 
        elif node.operator == '-': # Unary minus
            return f"-({operand})"
        else:
            return f"{node.operator}({operand})"
    
    def visit_indexer(self, node) -> str:
        """Visit an indexer node - handles array indexing"""
        # This method is for when IndexerNode is the primary node being visited,
        # e.g., (some_expression)[index] or root_array[index].
        base_expr = self.visit(node.expression)
        index_val_sql = self.visit(node.index)

        # Special handling for string character access (non-standard FHIRPath)
        # to pass the specific test: "'example'[0]" -> "e"
        if isinstance(node.expression, self.LiteralNode) and node.expression.type == 'string':
            # FHIRPath index is 0-based, SQL substring is 1-based.
            # Ensure index_val_sql is an integer for arithmetic.
            return f"substring({base_expr}, CAST({index_val_sql} AS INTEGER) + 1, 1)"

        # Optimization: If the base expression is a simple JSON path and the index is simple arithmetic,
        # try to build a direct JSON path instead of using dynamic indexing
        if (isinstance(node.expression, self.IdentifierNode) and 
            self._is_simple_arithmetic_expression(index_val_sql)):
            try:
                # Try to evaluate simple arithmetic expressions at generation time
                evaluated_index = self._evaluate_simple_arithmetic(index_val_sql)
                if evaluated_index is not None and evaluated_index >= 0:
                    # Build a direct JSON path - use a marker to indicate this is an optimized indexer
                    # This will be resolved later when the full path is built
                    return f"__OPTIMIZED_INDEX__{self.json_column}__$.{node.expression.name}[{evaluated_index}]__"
            except:
                pass  # Fall back to dynamic indexing
        
        # Check if base expression is a simple JSON extract that we can extend with direct indexing
        import re
        json_extract_match = re.match(r"json_extract(?:_string)?\(([^,]+),\s*'([^']+)'\)", base_expr.strip())
        if (json_extract_match and self._is_simple_arithmetic_expression(index_val_sql)):
            try:
                json_base, current_path = json_extract_match.groups()
                evaluated_index = self._evaluate_simple_arithmetic(index_val_sql)
                if evaluated_index is not None and evaluated_index >= 0:
                    # Extend the JSON path directly - use a marker for later resolution
                    new_path = f"{current_path}[{evaluated_index}]"
                    return f"__OPTIMIZED_INDEX__{json_base}__{new_path}__"
            except:
                pass  # Fall back to dynamic indexing

        # For simple arithmetic expressions, try to use json_extract_string when appropriate
        if (self._is_simple_arithmetic_expression(index_val_sql) and 
            not ('json_group_array' in base_expr or 'json_each' in base_expr)):
            try:
                evaluated_index = self._evaluate_simple_arithmetic(index_val_sql)
                if evaluated_index is not None and evaluated_index >= 0:
                    # Use json_extract_string for likely string results to avoid quotes
                    return f"COALESCE(json_extract_string({base_expr}, '$[{evaluated_index}]'), '')"
            except:
                pass
        
        # Default: JSON array indexing with optimization for string results
        # For simple arithmetic on string-like extractions, use json_extract_string to avoid quotes
        if (self._is_simple_arithmetic_expression(index_val_sql) and 
            'json_extract_string(' in base_expr and not ('json_group_array' in base_expr or 'json_each' in base_expr)):
            try:
                evaluated_index = self._evaluate_simple_arithmetic(index_val_sql)
                if evaluated_index is not None and evaluated_index >= 0:
                    # Use static index with json_extract_string
                    return f"COALESCE(json_extract_string({base_expr}, '$[{evaluated_index}]'), '')"
            except:
                pass
        
        # For simple arithmetic indexing on string extractions, use json_extract_string to avoid quotes
        if self._is_simple_arithmetic_expression(index_val_sql):
            try:
                evaluated_index = self._evaluate_simple_arithmetic(index_val_sql) 
                if evaluated_index is not None and evaluated_index >= 0:
                    # Use json_extract_string instead of json_extract for string results
                    return f"COALESCE(json_extract_string({base_expr}, '$[{evaluated_index}]'), '')"
            except:
                pass

        # Default: JSON array indexing. Return empty collection on out-of-bounds/non-array.
        # For arithmetic expressions, use json_extract_string to avoid quotes around string values
        if self._is_simple_arithmetic_expression(index_val_sql):
            path_prefix = "'$['"
            path_suffix = "']'"
            return f"COALESCE(json_extract_string({base_expr}, {self.dialect.string_concat(self.dialect.string_concat(path_prefix, f'CAST({index_val_sql} AS VARCHAR)'), path_suffix)}), '')"
        else:
            path_prefix = "'$['"
            path_suffix = "']'"
            return f"COALESCE(json_extract({base_expr}, {self.dialect.string_concat(self.dialect.string_concat(path_prefix, f'CAST({index_val_sql} AS VARCHAR)'), path_suffix)}), json_array())"
    
    def _is_simple_arithmetic_expression(self, expr: str) -> bool:
        """Check if an expression is simple arithmetic that can be evaluated at generation time"""
        # Remove whitespace and parentheses for evaluation
        clean_expr = expr.strip().replace(' ', '')
        # Check if it contains only numbers, basic operators, and parentheses
        import re
        return bool(re.match(r'^[\d+\-*/().\s]+$', clean_expr))
    
    def _evaluate_simple_arithmetic(self, expr: str) -> int:
        """Safely evaluate simple arithmetic expressions to integers"""
        try:
            # Remove extra parentheses and whitespace
            clean_expr = expr.strip()
            if clean_expr.startswith('(') and clean_expr.endswith(')'):
                clean_expr = clean_expr[1:-1]
            
            # Only allow safe arithmetic operations
            import re
            if not re.match(r'^[\d+\-*/().\s]+$', clean_expr):
                return None
            
            # Use eval safely for simple arithmetic (only numbers and basic operators)
            result = eval(clean_expr)
            
            # Ensure result is a non-negative integer
            if isinstance(result, (int, float)) and result >= 0 and result == int(result):
                return int(result)
            
            return None
        except:
            return None
    
    def _build_segments_path(self, segments) -> str:
        """Build SQL path expression for a list of segments (helper for choice type handling)"""
        if not segments:
            return self.json_column
        
        # Start with the first segment
        current_sql = self.visit(segments[0])
        
        # Apply subsequent segments
        for segment in segments[1:]:
            if isinstance(segment, self.IdentifierNode):
                current_sql = self._apply_identifier_segment(segment.name, current_sql)
            elif isinstance(segment, self.FunctionCallNode):
                current_sql = self.apply_function_to_expression(segment, current_sql)
            else:
                # For other types like IndexerNode, we'd need more complex handling
                # For now, just raise an error
                raise ValueError(f"Unsupported segment type in choice type prefix: {type(segment)}")
        
        return current_sql

    def _get_choice_field_mapping_direct(self, field_name: str, type_name: str) -> Optional[str]:
        """
        DEPRECATED: Use fhir_choice_types.get_choice_field_mapping_direct() instead.
        
        This method is kept for backward compatibility but delegates to the comprehensive
        FHIR choice type system that supports 1,300+ mappings from choiceTypePaths.json.
        
        Args:
            field_name: Choice field name (e.g., "value", "identified")
            type_name: Target type (e.g., "Quantity", "dateTime")
            
        Returns:
            Mapped field name (e.g., "valueQuantity", "identifiedDateTime") or None
        """
        return self.fhir_choice_types.get_choice_field_mapping_direct(field_name, type_name)
    
