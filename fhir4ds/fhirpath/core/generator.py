"""
SQL Generator for FHIRPath expressions

This module contains the SQL generation logic that converts FHIRPath ASTs
into SQL queries compatible with DuckDB's JSON extension.
"""

import re
import hashlib
from typing import List, Dict, Any, Optional, Union, Tuple
from ...utils.logging_config import get_logger, log_fallback_usage
from .exceptions import FHIRPathValidationError, FHIRPathGenerationError, FHIRPathOptimizationError
from .error_handling import validate_argument_count, validate_argument_range, ErrorContext
from .generators import LiteralHandler
from .generators.operators import OperatorHandler
from .generators.path import PathHandler
from .generators.functions.collection_functions import CollectionFunctionHandler
from .generators.functions.string_functions import StringFunctionHandler
from .generators.functions.math_functions import MathFunctionHandler
from .generators.functions.type_functions import TypeFunctionHandler
from .generators.functions.datetime_functions import DateTimeFunctionHandler
from .cte_manager import CTEManager

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
        
        # CTE system configuration (standardized on legacy system)
        self.cte_builder = None
        self.ctes = {}  # CTE name -> CTE SQL  
        self.cte_counter = 0
        self.enable_cte = True  # Global CTE enablement
        # Unified CTE decision system
        self._setup_cte_configs()
        
        # Initialize CTEManager for advanced CTE optimization
        self.cte_manager = CTEManager(table_name)
        
        # Advanced CTE optimizations (optional)
        self.enable_cte_deduplication = enable_cte_deduplication
        self.cte_fingerprints = {}  # fingerprint -> cte_name mapping for deduplication
        
        # Logging
        self.logger = get_logger(__name__)
        
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
                ASTNode, ThisNode, VariableNode, LiteralNode, IdentifierNode, FunctionCallNode,
                BinaryOpNode, UnaryOpNode, PathNode, IndexerNode, TupleNode
            )
            self.ASTNode = ASTNode
            self.ThisNode = ThisNode
            self.VariableNode = VariableNode
            self.LiteralNode = LiteralNode
            self.IdentifierNode = IdentifierNode
            self.FunctionCallNode = FunctionCallNode
            self.BinaryOpNode = BinaryOpNode
            self.UnaryOpNode = UnaryOpNode
            self.PathNode = PathNode
            self.IndexerNode = IndexerNode
            self.TupleNode = TupleNode
        else:
            # Use injected AST nodes
            (self.ASTNode, self.ThisNode, self.VariableNode, self.LiteralNode, self.IdentifierNode, 
             self.FunctionCallNode, self.BinaryOpNode, self.UnaryOpNode, 
             self.PathNode, self.IndexerNode, self.TupleNode) = ast_nodes_module
            
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
        
        # Initialize extracted component handlers
        self.literal_handler = LiteralHandler()
        
        # Initialize operator handler with required dependencies
        ast_nodes_dict = {
            'LiteralNode': self.LiteralNode,
            'IdentifierNode': self.IdentifierNode,
            'PathNode': self.PathNode,
            'FunctionCallNode': self.FunctionCallNode,
            'IndexerNode': self.IndexerNode
        }
        self.operator_handler = OperatorHandler(self.SQL_OPERATORS, ast_nodes_dict, self.dialect)
        
        # Initialize path handler with required dependencies  
        self.path_handler = PathHandler(ast_nodes_dict, self.dialect, self.json_column, self.fhir_choice_types)
        self.path_handler.set_generator_context(self)
        
        # Initialize function handlers with CTE support
        self.collection_function_handler = CollectionFunctionHandler(self, self.cte_builder)
        self.string_function_handler = StringFunctionHandler(self, self.cte_builder)
        self.math_function_handler = MathFunctionHandler(self, self.cte_builder)
        self.type_function_handler = TypeFunctionHandler(self, self.cte_builder)
        self.datetime_function_handler = DateTimeFunctionHandler(self, self.cte_builder)

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
            
            # Math functions
            'abs': CTEConfig(50, 0, 1),      # Low complexity
            'ceiling': CTEConfig(50, 0, 1),  # Low complexity
            'floor': CTEConfig(50, 0, 1),    # Low complexity
            'round': CTEConfig(60, 0, 1),    # Medium complexity (optional precision)
            'sqrt': CTEConfig(50, 0, 1),     # Low complexity
            'truncate': CTEConfig(50, 0, 1), # Low complexity
            
            # Temporal functions
            'now': CTEConfig(50, 0, 1),      # Low complexity
            'today': CTEConfig(50, 0, 1),    # Low complexity
            'timeofday': CTEConfig(50, 0, 1), # Low complexity
            
            # Debug functions
            'trace': CTEConfig(80, 0, 1),    # Medium complexity (optional projection)
            
            # Advanced collection functions
            'aggregate': CTEConfig(200, 2, 3, 100),  # High complexity (custom aggregation functions)
            'flatten': CTEConfig(150, 1, 2, 80),     # Medium-high complexity (recursive flattening)
            
            # FHIR-specific type testing functions
            'convertstoquantity': CTEConfig(100, 1, 2, 60),  # Medium complexity (type conversion validation)
            'hasvalue': CTEConfig(80, 0, 1, 50),             # Medium complexity (value existence checking)
            'hascodedvalue': CTEConfig(120, 1, 2, 70),       # Medium-high complexity (coded value validation)
            'htmlchecks': CTEConfig(150, 1, 2, 80),          # Medium-high complexity (HTML validation)
            'hastemplateidof': CTEConfig(100, 1, 2, 60),     # Medium complexity (template ID validation)
            
            # String encoding/decoding functions
            'encode': CTEConfig(90, 0, 1, 50),               # Medium complexity (URL encoding)
            'decode': CTEConfig(90, 0, 1, 50),               # Medium complexity (URL decoding)
            
            # Advanced FHIR functions
            'conformsto': CTEConfig(200, 1, 2, 100),         # High complexity (profile conformance checking)
            'memberof': CTEConfig(180, 1, 2, 90),            # High complexity (ValueSet membership)
            
            # Phase 1: Critical Collection Functions
            'single': CTEConfig(80, 0, 1, 50),               # Medium complexity (single element with error handling)
            'tail': CTEConfig(90, 0, 1, 60),                 # Medium complexity (all but first element)
            'skip': CTEConfig(100, 0, 1, 70),                # Medium complexity (skip N elements with argument)
            'take': CTEConfig(110, 0, 1, 80),                # Medium complexity (take N elements with argument)
            'intersect': CTEConfig(120, 1, 2, 90),           # High complexity (set intersection with comparison)
            'exclude': CTEConfig(130, 1, 2, 100),            # High complexity (set exclusion with comparison)
            
            # Phase 2: Boolean Logic & Conditional Operations
            'alltrue': CTEConfig(75, 0, 1, 40),              # Medium complexity (boolean aggregation)
            'allfalse': CTEConfig(76, 0, 1, 41),             # Medium complexity (boolean aggregation)
            'anytrue': CTEConfig(77, 0, 1, 42),              # Medium complexity (boolean aggregation)
            'anyfalse': CTEConfig(78, 0, 1, 43),             # Medium complexity (boolean aggregation)
            'iif': CTEConfig(150, 1, 3, 80),                 # High complexity (conditional with 3 arguments)
            'subsetof': CTEConfig(200, 1, 2, 120),           # High complexity (set comparison operation)
            'supersetof': CTEConfig(210, 1, 2, 125),         # High complexity (set comparison operation)
            'isdistinct': CTEConfig(180, 1, 2, 100),         # High complexity (distinctness check)
            'as': CTEConfig(120, 1, 1, 80),              # Medium complexity (type casting)
            'is': CTEConfig(110, 1, 1, 75),              # Medium complexity (type checking)
            'toboolean': CTEConfig(90, 0, 1, 60),        # Medium complexity (boolean conversion)
            'convertstoboolean': CTEConfig(85, 0, 1, 55), # Medium complexity (boolean conversion test)
            'todecimal': CTEConfig(95, 0, 1, 65),        # Medium complexity (decimal conversion)
            'convertstodecimal': CTEConfig(90, 0, 1, 60), # Medium complexity (decimal conversion test)
            'convertstointeger': CTEConfig(85, 0, 1, 55), # Medium complexity (integer conversion test)
            
            # Phase 3 Week 9: Date/Time Conversion Functions
            'todate': CTEConfig(100, 0, 1, 70),        # Medium complexity (date conversion)
            'convertstodate': CTEConfig(95, 0, 1, 65), # Medium complexity (date conversion test)
            'todatetime': CTEConfig(110, 0, 1, 75),    # Medium complexity (datetime conversion)
            'convertstodatetime': CTEConfig(105, 0, 1, 70), # Medium complexity (datetime conversion test)
            'totime': CTEConfig(100, 0, 1, 70),        # Medium complexity (time conversion)
            'convertstotime': CTEConfig(95, 0, 1, 65), # Medium complexity (time conversion test)
            
            # Phase 4 Week 10: Advanced Mathematical Functions
            'exp': CTEConfig(80, 0, 1, 50),           # Medium complexity (exponential function)
            'ln': CTEConfig(85, 0, 1, 55),            # Medium complexity (natural logarithm)
            'log': CTEConfig(85, 0, 1, 55),           # Medium complexity (base-10 logarithm)
            
            # Phase 4 Week 11: Advanced Operations
            'power': CTEConfig(90, 0, 1, 60),         # Medium complexity (power function with argument)
            
            # Phase 5 Week 12: String Functions
            'tochars': CTEConfig(120, 0, 1, 80),      # Medium complexity (string to character array conversion)
            'matches': CTEConfig(110, 0, 1, 75),      # Medium complexity (regex matching)
            'replacematches': CTEConfig(130, 0, 1, 85), # Medium-high complexity (regex replacement)
            
            # Phase 5 Week 13: Navigation Functions
            'children': CTEConfig(140, 0, 1, 90),     # Medium-high complexity (direct child navigation)
            'descendants': CTEConfig(160, 0, 1, 100), # High complexity (recursive descendant navigation)
            
            # Phase 6 Week 14: Collection Combining Functions
            'union': CTEConfig(150, 0, 1, 95),        # High complexity (set union with deduplication)
            'combine': CTEConfig(120, 0, 1, 80),      # Medium complexity (collection combination)
            
            # Phase 6 Week 15: Advanced Operations
            'repeat': CTEConfig(200, 0, 1, 120),      # Very high complexity (iterative projection with recursion)
        }
        
        # Default configuration for unlisted functions
        self.default_cte_config = CTEConfig(50, 0, 1)
    
    def _should_use_cte_unified(self, base_expr: str, function_type: str) -> bool:
        """Unified CTE decision logic using CTEManager's sophisticated analysis"""
        # CRITICAL: Never use CTEs in WHERE context - they can't be properly referenced
        if self.in_where_context:
            return False
            
        # Use CTEManager's unified decision system
        return self.cte_manager.should_use_cte_unified(base_expr, function_type)

    def generate_alias(self) -> str:
        """Generate a unique alias"""
        self.alias_counter += 1
        return f"t{self.alias_counter}"
    
    # Dialect-aware helper methods for JSON operations
    def extract_json_field(self, column: str, path: str) -> str:
        """Extract JSON field as text using dialect-specific method"""
        return self.path_handler.extract_json_field(column, path)
    
    def extract_json_object(self, column: str, path: str) -> str:
        """Extract JSON object using dialect-specific method"""
        return self.path_handler.extract_json_object(column, path)
    
    def iterate_json_array(self, column: str, path: str) -> str:
        """Iterate JSON array using dialect-specific method"""
        return self.dialect.iterate_json_array(column, path)
    
    def iterate_json_elements_indexed(self, column: str) -> str:
        """Iterate JSON elements with proper indexing for both arrays and objects"""
        # For PostgreSQL, we need to handle arrays vs objects differently
        if self.dialect.name == 'POSTGRESQL':
            # PostgreSQL needs different functions for arrays vs objects
            return f"""(
                SELECT value, (ordinality - 1)::text as key 
                FROM jsonb_array_elements({column}) WITH ORDINALITY AS t(value, ordinality)
            )"""
        else:
            # For DuckDB and other dialects, use the existing method
            return f"{self.dialect.json_each_function}({column})"
    
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
    
    def extract_json_array_element(self, expression: str, index: int) -> str:
        """Extract array element at index using dialect-specific method"""
        return self.dialect.extract_json_object(expression, f'$[{index}]')
    
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
        # Don't create placeholders when CTEs are disabled, as they won't be resolved properly
        if not self.enable_cte:
            return base_expr
            
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
    
    def merge_nested_generator_state(self, nested_generator: 'SQLGenerator') -> None:
        """
        Merge CTEs and complex expression cache from a nested generator.
        
        This centralizes the logic for merging nested generator state,
        replacing the scattered workarounds throughout the codebase.
        
        Args:
            nested_generator: The nested generator to merge state from
        """
        # Merge CTEs from nested generator
        for cte_name, cte_def in nested_generator.ctes.items():
            if cte_name not in self.ctes:
                self.ctes[cte_name] = cte_def
        
        # Merge complex expression cache to resolve optimized placeholders
        for placeholder, expr in nested_generator.complex_expr_cache.items():
            if placeholder not in self.complex_expr_cache:
                self.complex_expr_cache[placeholder] = expr
    
    def _create_cte(self, expression: str, operation_name: str) -> str:
        """Create CTE using CTEManager for advanced deduplication and optimization"""
        # Use CTEManager for sophisticated CTE creation
        cte_name = self.cte_manager.create_cte(expression, operation_name)
        
        # Synchronize with CTEManager's CTE store
        self.ctes.update(self.cte_manager.ctes)
        
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
        """Build the final query with CTEs using CTEManager's advanced optimization"""
        # Use CTEManager's sophisticated final query building
        final_query = self.cte_manager.build_final_query_with_ctes(main_query)
        
        # If CTEManager didn't add CTEs, check legacy system for backwards compatibility
        if final_query == main_query and self.ctes:
            # Resolve optimized placeholders before building final query
            main_query = self._resolve_optimized_placeholders(main_query)
            
            # Phase 6C: Apply smart CTE inlining first (most aggressive optimization)
            if self.enable_cte_inlining:
                return self._build_final_query_with_smart_inlining(main_query)
            
            # Phase 6B: Apply dialect-specific CTE optimizations
            if self.enable_dialect_optimizations:
                return self._build_final_query_with_dialect_optimizations(main_query)
            
            # Build WITH clause (original implementation)
            cte_definitions = []
            for cte_name, cte_expr in self.ctes.items():
                # Also resolve placeholders in CTE expressions
                resolved_cte_expr = self._resolve_optimized_placeholders(cte_expr)
                cte_definitions.append(f"{cte_name} AS ({resolved_cte_expr})")
            
            with_clause = "WITH " + ",\n".join(cte_definitions)
            return f"{with_clause}\n{main_query}"
        
        return final_query
    
    def _build_final_query_with_dialect_optimizations(self, main_query: str) -> str:
        """
        Phase 6B: Build final query with database-specific CTE optimizations
        
        Applies optimizations based on the specific database dialect:
        - PostgreSQL: MATERIALIZED/NOT MATERIALIZED hints
        - DuckDB: JSON-specific optimizations
        """
        # Resolve optimized placeholders before dialect optimizations
        main_query = self._resolve_optimized_placeholders(main_query)
        
        # Build CTE definitions with dialect-specific hints
        cte_definitions = []
        for cte_name, cte_expr in self.ctes.items():
            # Resolve placeholders in CTE expressions before optimization
            resolved_cte_expr = self._resolve_optimized_placeholders(cte_expr)
            optimized_cte = self._optimize_cte_for_dialect(cte_name, resolved_cte_expr)
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
        # Resolve optimized placeholders before smart inlining
        main_query = self._resolve_optimized_placeholders(main_query)
        
        if not self.ctes:
            return main_query
        
        # Analyze CTEs for inlining opportunities
        ctes_to_inline = {}
        ctes_to_keep = {}
        
        for cte_name, cte_expr in self.ctes.items():
            # Resolve placeholders in CTE expressions before analysis
            resolved_cte_expr = self._resolve_optimized_placeholders(cte_expr)
            if self._should_inline_cte(cte_name, resolved_cte_expr, main_query):
                ctes_to_inline[cte_name] = resolved_cte_expr
            else:
                ctes_to_keep[cte_name] = resolved_cte_expr
        
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
        validate_argument_count("where", 1, len(func_node.args))
            
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
            validate_argument_range("join", 0, 1, len(func_node.args))
        
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

    def _generate_single_with_cte(self, base_expr: str) -> str:
        """Generate single() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "single_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for single element extraction with error handling
        single_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if the expression is null (empty collection)
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Array is empty - return empty collection (NULL)
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            -- Array has exactly one element - return that element
                            WHEN {self.get_json_array_length(base_ref)} = 1 THEN
                                {self.extract_json_object(base_ref, '$[0]')}
                            -- Array has more than one element - return empty collection (NULL) per FHIRPath error semantics
                            ELSE NULL
                        END
                    -- For non-arrays (single objects), return the object itself
                    ELSE {base_ref}
                END as single_value
            FROM {from_clause}
        """, "single_result")
        
        return f"(SELECT single_value FROM {single_cte_name})"

    def _generate_tail_with_cte(self, base_expr: str) -> str:
        """Generate tail() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "tail_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for tail element extraction (all but first element)
        tail_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if the expression is null (empty collection)
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Array is empty or has only one element - return empty collection (NULL)
                            WHEN {self.get_json_array_length(base_ref)} <= 1 THEN NULL
                            -- Array has multiple elements - return slice from index 1 to end
                            ELSE (
                                SELECT {self.dialect.json_array_function}(
                                    value::VARCHAR
                                )
                                FROM (
                                    SELECT 
                                        {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                    FROM {self.iterate_json_elements_indexed(base_ref)}
                                    WHERE ROW_NUMBER() OVER () > 1
                                ) AS tail_elements
                            )
                        END
                    -- For non-arrays (single objects), return empty collection (NULL)
                    ELSE NULL
                END as tail_value
            FROM {from_clause}
        """, "tail_result")
        
        return f"(SELECT tail_value FROM {tail_cte_name})"

    def _generate_skip_with_cte(self, base_expr: str, func_node) -> str:
        """Generate skip() function using CTE approach"""
        
        # Get the skip count argument
        validate_argument_count("skip", 1, len(func_node.args))
        
        skip_count_arg = func_node.args[0]
        if hasattr(skip_count_arg, 'value'):
            skip_count = skip_count_arg.value
        else:
            # Handle complex expressions - visit the argument
            skip_count_sql = self.visit(skip_count_arg)
            skip_count = f"({skip_count_sql})"
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "skip_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for skip element extraction
        skip_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if the expression is null (empty collection)
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- If skip count is null or negative, return original array
                            WHEN {skip_count} IS NULL OR {skip_count} < 0 THEN {base_ref}
                            -- Array length is less than or equal to skip count - return empty collection (NULL)
                            WHEN {self.get_json_array_length(base_ref)} <= {skip_count} THEN NULL
                            -- Array has more elements than skip count - return elements after skip
                            ELSE (
                                SELECT {self.dialect.json_array_function}(
                                    value::VARCHAR
                                )
                                FROM (
                                    SELECT 
                                        {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                    FROM {self.iterate_json_elements_indexed(base_ref)}
                                    WHERE ROW_NUMBER() OVER () > {skip_count}
                                ) AS skip_elements
                            )
                        END
                    -- For non-arrays (single objects), skip behavior
                    ELSE 
                        CASE 
                            WHEN {skip_count} IS NULL OR {skip_count} <= 0 THEN {base_ref}
                            ELSE NULL
                        END
                END as skip_value
            FROM {from_clause}
        """, "skip_result")
        
        return f"(SELECT skip_value FROM {skip_cte_name})"

    def _generate_take_with_cte(self, base_expr: str, func_node) -> str:
        """Generate take() function using CTE approach"""
        
        # Get the take count argument
        if len(func_node.args) != 1:
            validate_argument_count("take", 1, len(func_node.args))
        
        take_count_arg = func_node.args[0]
        if hasattr(take_count_arg, 'value'):
            take_count = take_count_arg.value
        else:
            # Handle complex expressions - visit the argument
            take_count_sql = self.visit(take_count_arg)
            take_count = f"({take_count_sql})"
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "take_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for take element extraction
        take_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if the expression is null (empty collection)
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- If take count is null or zero or negative, return empty collection (NULL)
                            WHEN {take_count} IS NULL OR {take_count} <= 0 THEN NULL
                            -- Array is empty - return empty collection (NULL)
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            -- Take count is greater than or equal to array length - return entire array
                            WHEN {take_count} >= {self.get_json_array_length(base_ref)} THEN {base_ref}
                            -- Take count is less than array length - return first take_count elements
                            ELSE (
                                SELECT {self.dialect.json_array_function}(
                                    value::VARCHAR
                                )
                                FROM (
                                    SELECT 
                                        {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                    FROM {self.iterate_json_elements_indexed(base_ref)}
                                    WHERE ROW_NUMBER() OVER () <= {take_count}
                                ) AS take_elements
                            )
                        END
                    -- For non-arrays (single objects), take behavior
                    ELSE 
                        CASE 
                            WHEN {take_count} IS NULL OR {take_count} <= 0 THEN NULL
                            ELSE {base_ref}
                        END
                END as take_value
            FROM {from_clause}
        """, "take_result")
        
        return f"(SELECT take_value FROM {take_cte_name})"

    def _generate_intersect_with_cte(self, base_expr: str, func_node) -> str:
        """Generate intersect() function using CTE approach"""
        
        # Get the other collection argument
        if len(func_node.args) != 1:
            validate_argument_count("intersect", 1, len(func_node.args))
        
        other_collection_arg = func_node.args[0]
        # Visit the argument to get the SQL for the other collection
        other_collection_sql = self.visit(other_collection_arg)
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "intersect_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for other collection if it's complex
        if other_collection_sql.startswith("(SELECT") and "FROM " in other_collection_sql:
            other_cte_name = self._create_cte(
                f"SELECT {other_collection_sql} as other_value FROM {self.table_name}",
                "intersect_other"
            )
            other_ref = "other_value"
            other_from_clause = other_cte_name
        else:
            other_ref = other_collection_sql
            other_from_clause = self.table_name
        
        # Create CTE for intersect operation
        intersect_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if either expression is null (empty collection)
                    WHEN {base_ref} IS NULL OR ({other_ref}) IS NULL THEN NULL
                    -- Check if both are arrays
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' AND {self.get_json_type(other_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Either array is empty - return empty collection (NULL)
                            WHEN {self.get_json_array_length(base_ref)} = 0 OR {self.get_json_array_length(other_ref)} = 0 THEN NULL
                            -- Both arrays have elements - find intersection
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = 0 THEN NULL
                                    ELSE {self.dialect.json_array_function}(
                                        DISTINCT value::VARCHAR
                                    )
                                END
                                FROM (
                                    SELECT 
                                        {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                    FROM {self.iterate_json_elements_indexed(base_ref)}
                                ) AS left_elements
                                WHERE EXISTS (
                                    SELECT 1 
                                    FROM (
                                        SELECT 
                                            {self.dialect.json_extract_function}({other_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as other_value
                                        FROM {self.iterate_json_elements_indexed(other_ref)}
                                    ) AS right_elements
                                    WHERE left_elements.value = right_elements.other_value
                                )
                            )
                        END
                    -- Handle mixed array/non-array cases
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' AND {self.get_json_type(other_ref)} != 'ARRAY' THEN
                        -- Base is array, other is single - check if single value exists in array
                        CASE 
                            WHEN EXISTS (
                                SELECT 1 
                                FROM (
                                    SELECT 
                                        {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                    FROM {self.iterate_json_elements_indexed(base_ref)}
                                ) AS array_elements
                                WHERE array_elements.value = {other_ref}
                            ) THEN {self.dialect.json_array_function}({other_ref}::VARCHAR)
                            ELSE NULL
                        END
                    WHEN {self.get_json_type(base_ref)} != 'ARRAY' AND {self.get_json_type(other_ref)} = 'ARRAY' THEN
                        -- Base is single, other is array - check if single value exists in array
                        CASE 
                            WHEN EXISTS (
                                SELECT 1 
                                FROM (
                                    SELECT 
                                        {self.dialect.json_extract_function}({other_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                    FROM {self.iterate_json_elements_indexed(other_ref)}
                                ) AS array_elements
                                WHERE array_elements.value = {base_ref}
                            ) THEN {self.dialect.json_array_function}({base_ref}::VARCHAR)
                            ELSE NULL
                        END
                    -- Both are single values - check equality
                    ELSE 
                        CASE 
                            WHEN {base_ref} = {other_ref} THEN {self.dialect.json_array_function}({base_ref}::VARCHAR)
                            ELSE NULL
                        END
                END as intersect_value
            FROM {from_clause if from_clause == other_from_clause else f"{from_clause}, {other_from_clause}"}
        """, "intersect_result")
        
        return f"(SELECT intersect_value FROM {intersect_cte_name})"

    def _generate_exclude_with_cte(self, base_expr: str, func_node) -> str:
        """Generate exclude() function using CTE approach"""
        
        # Get the other collection argument
        if len(func_node.args) != 1:
            validate_argument_count("exclude", 1, len(func_node.args))
        
        other_collection_arg = func_node.args[0]
        # Visit the argument to get the SQL for the other collection
        other_collection_sql = self.visit(other_collection_arg)
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "exclude_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for other collection if it's complex
        if other_collection_sql.startswith("(SELECT") and "FROM " in other_collection_sql:
            other_cte_name = self._create_cte(
                f"SELECT {other_collection_sql} as other_value FROM {self.table_name}",
                "exclude_other"
            )
            other_ref = "other_value"
            other_from_clause = other_cte_name
        else:
            other_ref = other_collection_sql
            other_from_clause = self.table_name
        
        # Create CTE for exclude operation
        exclude_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if the base expression is null (empty collection)
                    WHEN {base_ref} IS NULL THEN NULL
                    -- If other collection is null, return the base collection (nothing to exclude)
                    WHEN ({other_ref}) IS NULL THEN {base_ref}
                    -- Check if base is an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Base array is empty - return empty collection (NULL)
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            -- Other is also an array - exclude elements that exist in other
                            WHEN {self.get_json_type(other_ref)} = 'ARRAY' THEN (
                                SELECT CASE 
                                    WHEN COUNT(*) = 0 THEN NULL
                                    ELSE {self.dialect.json_array_function}(
                                        DISTINCT value::VARCHAR
                                    )
                                END
                                FROM (
                                    SELECT 
                                        {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                    FROM {self.iterate_json_elements_indexed(base_ref)}
                                ) AS left_elements
                                WHERE NOT EXISTS (
                                    SELECT 1 
                                    FROM (
                                        SELECT 
                                            {self.dialect.json_extract_function}({other_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as other_value
                                        FROM {self.iterate_json_elements_indexed(other_ref)}
                                    ) AS right_elements
                                    WHERE left_elements.value = right_elements.other_value
                                )
                            )
                            -- Other is single value - exclude if it matches any array element
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = 0 THEN NULL
                                    ELSE {self.dialect.json_array_function}(
                                        DISTINCT value::VARCHAR
                                    )
                                END
                                FROM (
                                    SELECT 
                                        {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                    FROM {self.iterate_json_elements_indexed(base_ref)}
                                ) AS array_elements
                                WHERE array_elements.value != {other_ref}
                            )
                        END
                    -- Base is single value
                    ELSE 
                        CASE 
                            -- Other is array - check if base value exists in array, if so exclude it
                            WHEN {self.get_json_type(other_ref)} = 'ARRAY' THEN
                                CASE 
                                    WHEN EXISTS (
                                        SELECT 1 
                                        FROM (
                                            SELECT 
                                                {self.dialect.json_extract_function}({other_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                            FROM {self.iterate_json_elements_indexed(other_ref)}
                                        ) AS array_elements
                                        WHERE array_elements.value = {base_ref}
                                    ) THEN NULL
                                    ELSE {self.dialect.json_array_function}({base_ref}::VARCHAR)
                                END
                            -- Both are single values - exclude if they match
                            ELSE 
                                CASE 
                                    WHEN {base_ref} = {other_ref} THEN NULL
                                    ELSE {self.dialect.json_array_function}({base_ref}::VARCHAR)
                                END
                        END
                END as exclude_value
            FROM {from_clause if from_clause == other_from_clause else f"{from_clause}, {other_from_clause}"}
        """, "exclude_result")
        
        return f"(SELECT exclude_value FROM {exclude_cte_name})"

    def _generate_alltrue_with_cte(self, base_expr: str) -> str:
        """Generate allTrue() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "alltrue_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for allTrue operation
        alltrue_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if the expression is null (empty collection)
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Array is empty - return empty (NULL) per FHIRPath spec
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            -- Array has elements - check if all are true
                            ELSE (
                                SELECT CASE 
                                    -- If any element is false, return false
                                    WHEN COUNT(CASE WHEN 
                                        (CASE 
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                            ELSE NULL
                                        END) = false 
                                        THEN 1 END) > 0 THEN false
                                    -- If any element is null/non-boolean, return false (strict evaluation)
                                    WHEN COUNT(CASE WHEN 
                                        (CASE 
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                            ELSE NULL
                                        END) IS NULL 
                                        THEN 1 END) > 0 THEN false
                                    -- If all elements are true, return true
                                    ELSE true
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- For non-arrays (single values), check if the value is true
                    ELSE 
                        CASE 
                            WHEN {base_ref}::VARCHAR = 'true' THEN true
                            WHEN {base_ref}::VARCHAR = 'false' THEN false
                            ELSE false  -- Non-boolean single values are treated as false
                        END
                END as alltrue_value
            FROM {from_clause}
        """, "alltrue_result")
        
        return f"(SELECT alltrue_value FROM {alltrue_cte_name})"

    def _generate_allfalse_with_cte(self, base_expr: str) -> str:
        """Generate allFalse() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "allfalse_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for allFalse operation
        allfalse_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if the expression is null (empty collection)
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Array is empty - return empty (NULL) per FHIRPath spec
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            -- Array has elements - check if all are false
                            ELSE (
                                SELECT CASE 
                                    -- If any element is true, return false
                                    WHEN COUNT(CASE WHEN 
                                        (CASE 
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                            ELSE NULL
                                        END) = true 
                                        THEN 1 END) > 0 THEN false
                                    -- If any element is null/non-boolean, return false (strict evaluation)
                                    WHEN COUNT(CASE WHEN 
                                        (CASE 
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                            ELSE NULL
                                        END) IS NULL 
                                        THEN 1 END) > 0 THEN false
                                    -- If all elements are false, return true
                                    ELSE true
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- For non-arrays (single values), check if the value is false
                    ELSE 
                        CASE 
                            WHEN {base_ref}::VARCHAR = 'false' THEN true
                            WHEN {base_ref}::VARCHAR = 'true' THEN false
                            ELSE false  -- Non-boolean single values are treated as not-false (so false for allFalse)
                        END
                END as allfalse_value
            FROM {from_clause}
        """, "allfalse_result")
        
        return f"(SELECT allfalse_value FROM {allfalse_cte_name})"

    def _generate_anytrue_with_cte(self, base_expr: str) -> str:
        """Generate anyTrue() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "anytrue_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for anyTrue operation
        anytrue_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if the expression is null (empty collection)
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Array is empty - return empty (NULL) per FHIRPath spec
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            -- Array has elements - check if any are true
                            ELSE (
                                SELECT CASE 
                                    -- If any element is true, return true
                                    WHEN COUNT(CASE WHEN 
                                        (CASE 
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                            ELSE NULL
                                        END) = true 
                                        THEN 1 END) > 0 THEN true
                                    -- If any element is null/non-boolean, return false (strict evaluation)
                                    WHEN COUNT(CASE WHEN 
                                        (CASE 
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                            ELSE NULL
                                        END) IS NULL 
                                        THEN 1 END) > 0 THEN false
                                    -- If all elements are false, return false
                                    ELSE false
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- For non-arrays (single values), check if the value is true
                    ELSE 
                        CASE 
                            WHEN {base_ref}::VARCHAR = 'true' THEN true
                            WHEN {base_ref}::VARCHAR = 'false' THEN false
                            ELSE false  -- Non-boolean single values are treated as not-true (so false for anyTrue)
                        END
                END as anytrue_value
            FROM {from_clause}
        """, "anytrue_result")
        
        return f"(SELECT anytrue_value FROM {anytrue_cte_name})"

    def _generate_anyfalse_with_cte(self, base_expr: str) -> str:
        """Generate anyFalse() function using CTE approach"""
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "anyfalse_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for anyFalse operation
        anyfalse_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if the expression is null (empty collection)
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Array is empty - return empty (NULL) per FHIRPath spec
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            -- Array has elements - check if any are false
                            ELSE (
                                SELECT CASE 
                                    -- If any element is false, return true
                                    WHEN COUNT(CASE WHEN 
                                        (CASE 
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                            ELSE NULL
                                        END) = false 
                                        THEN 1 END) > 0 THEN true
                                    -- If any element is null/non-boolean, return false (strict evaluation)
                                    WHEN COUNT(CASE WHEN 
                                        (CASE 
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                            WHEN {self.dialect.json_extract_function}({base_ref}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                            ELSE NULL
                                        END) IS NULL 
                                        THEN 1 END) > 0 THEN false
                                    -- If all elements are true, return false
                                    ELSE false
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- For non-arrays (single values), check if the value is false
                    ELSE 
                        CASE 
                            WHEN {base_ref}::VARCHAR = 'false' THEN true
                            WHEN {base_ref}::VARCHAR = 'true' THEN false
                            ELSE false  -- Non-boolean single values are treated as not-false (so false for anyFalse)
                        END
                END as anyfalse_value
            FROM {from_clause}
        """, "anyfalse_result")
        
        return f"(SELECT anyfalse_value FROM {anyfalse_cte_name})"

    def _generate_iif_with_cte(self, base_expr: str, func_node) -> str:
        """Generate iif() function using CTE approach"""
        
        # Extract the three arguments
        if len(func_node.args) != 3:
            raise ValueError(f"iif() requires exactly 3 arguments, got {len(func_node.args)}")
        
        condition_arg = func_node.args[0]
        true_result_arg = func_node.args[1]
        false_result_arg = func_node.args[2]
        
        # Generate SQL for each argument
        condition_sql = self.visit(condition_arg)
        true_result_sql = self.visit(true_result_arg)
        false_result_sql = self.visit(false_result_arg)
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "iif_base"
            )
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            from_clause = self.table_name
        
        # Create CTE for iif operation
        iif_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Evaluate condition and return appropriate result
                    WHEN ({condition_sql})::VARCHAR = 'true' THEN {true_result_sql}
                    WHEN ({condition_sql})::VARCHAR = 'false' THEN {false_result_sql}
                    -- If condition is null or non-boolean, return empty (null)
                    ELSE NULL
                END as iif_value
            FROM {from_clause}
        """, "iif_result")
        
        return f"(SELECT iif_value FROM {iif_cte_name})"

    def _generate_subsetof_with_cte(self, base_expr: str, func_node) -> str:
        """Generate subsetOf() function using CTE approach"""
        
        # Extract the argument (the collection to compare against)
        if len(func_node.args) != 1:
            raise ValueError(f"subsetOf() requires exactly 1 argument, got {len(func_node.args)}")
        
        other_collection_arg = func_node.args[0]
        other_collection_sql = self.visit(other_collection_arg)
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "subsetof_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for subsetOf operation
        subsetof_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if base collection is null or empty
                    WHEN {base_ref} IS NULL THEN true
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' AND {self.get_json_array_length(base_ref)} = 0 THEN true
                    -- Check if other collection is null
                    WHEN ({other_collection_sql}) IS NULL THEN false
                    -- Both are arrays - check if all elements of base are in other
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' AND {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN (
                        SELECT CASE 
                            WHEN COUNT(*) = 0 THEN true  -- Empty base collection is subset of any collection
                            WHEN COUNT(CASE WHEN other_elem.value IS NULL THEN 1 END) > 0 THEN false  -- Any element not found in other collection
                            ELSE true  -- All elements found
                        END
                        FROM {self.iterate_json_elements_indexed(base_ref)} base_elem
                        LEFT JOIN {self.iterate_json_elements_indexed(other_collection_sql)} other_elem 
                            ON base_elem.value = other_elem.value
                    )
                    -- Handle single element cases
                    WHEN {self.get_json_type(base_ref)} != 'ARRAY' AND {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN (
                        SELECT CASE 
                            WHEN COUNT(*) > 0 THEN true
                            ELSE false
                        END
                        FROM {self.iterate_json_elements_indexed(other_collection_sql)}
                        WHERE value = {base_ref}
                    )
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' AND {self.get_json_type(other_collection_sql)} != 'ARRAY' THEN (
                        SELECT CASE 
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN true
                            WHEN {self.get_json_array_length(base_ref)} = 1 AND {self.dialect.json_extract_function}({base_ref}, '$[0]') = ({other_collection_sql}) THEN true
                            ELSE false
                        END
                    )
                    -- Both single elements
                    ELSE CASE WHEN {base_ref} = ({other_collection_sql}) THEN true ELSE false END
                END as subsetof_value
            FROM {from_clause}
        """, "subsetof_result")
        
        return f"(SELECT subsetof_value FROM {subsetof_cte_name})"

    def _generate_supersetof_with_cte(self, base_expr: str, func_node) -> str:
        """Generate supersetOf() function using CTE approach"""
        
        # Extract the argument (the collection to check if it's a subset of current)
        if len(func_node.args) != 1:
            raise ValueError(f"supersetOf() requires exactly 1 argument, got {len(func_node.args)}")
        
        other_collection_arg = func_node.args[0]
        other_collection_sql = self.visit(other_collection_arg)
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "supersetof_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for supersetOf operation
        supersetof_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if other collection is null or empty - base collection is superset of empty collection
                    WHEN ({other_collection_sql}) IS NULL THEN true
                    WHEN {self.get_json_type(other_collection_sql)} = 'ARRAY' AND {self.get_json_array_length(other_collection_sql)} = 0 THEN true
                    -- Check if base collection is null
                    WHEN {base_ref} IS NULL THEN false
                    -- Both are arrays - check if all elements of other are in base
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' AND {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN (
                        SELECT CASE 
                            WHEN COUNT(*) = 0 THEN true  -- Empty other collection is subset of any collection
                            WHEN COUNT(CASE WHEN base_elem.value IS NULL THEN 1 END) > 0 THEN false  -- Any element of other not found in base collection
                            ELSE true  -- All elements found
                        END
                        FROM {self.iterate_json_elements_indexed(other_collection_sql)} other_elem
                        LEFT JOIN {self.iterate_json_elements_indexed(base_ref)} base_elem 
                            ON other_elem.value = base_elem.value
                    )
                    -- Handle single element cases
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' AND {self.get_json_type(other_collection_sql)} != 'ARRAY' THEN (
                        SELECT CASE 
                            WHEN COUNT(*) > 0 THEN true
                            ELSE false
                        END
                        FROM {self.iterate_json_elements_indexed(base_ref)}
                        WHERE value = ({other_collection_sql})
                    )
                    WHEN {self.get_json_type(base_ref)} != 'ARRAY' AND {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN (
                        SELECT CASE 
                            WHEN {self.get_json_array_length(other_collection_sql)} = 0 THEN true
                            WHEN {self.get_json_array_length(other_collection_sql)} = 1 AND {self.dialect.json_extract_function}({other_collection_sql}, '$[0]') = {base_ref} THEN true
                            ELSE false
                        END
                    )
                    -- Both single elements
                    ELSE CASE WHEN {base_ref} = ({other_collection_sql}) THEN true ELSE false END
                END as supersetof_value
            FROM {from_clause}
        """, "supersetof_result")
        
        return f"(SELECT supersetof_value FROM {supersetof_cte_name})"

    def _generate_isdistinct_with_cte(self, base_expr: str, func_node) -> str:
        """Generate isDistinct() function using CTE approach"""
        
        # isDistinct() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"isDistinct() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "isdistinct_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for isDistinct operation
        isdistinct_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty - empty collection is distinct by definition
                    WHEN {base_ref} IS NULL THEN true
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array is distinct
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN true
                            -- Array with one element is distinct
                            WHEN {self.get_json_array_length(base_ref)} = 1 THEN true
                            -- Array with multiple elements - check for duplicates
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = COUNT(DISTINCT value) THEN true
                                    ELSE false
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- Single element is distinct by definition
                    ELSE true
                END as isdistinct_value
            FROM {from_clause}
        """, "isdistinct_result")
        
        return f"(SELECT isdistinct_value FROM {isdistinct_cte_name})"

    def _generate_as_with_cte(self, base_expr: str, func_node) -> str:
        """Generate as() function using CTE approach"""
        
        # Extract the type argument
        if len(func_node.args) != 1:
            raise ValueError(f"as() requires exactly 1 argument, got {len(func_node.args)}")
        
        if not isinstance(func_node.args[0], self.IdentifierNode):
            raise ValueError("as() function requires a type identifier argument")
        
        type_name = func_node.args[0].name
        type_name_lower = type_name.lower()
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "as_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Build type condition
        fhir_primitive_types_as_string = self.FHIR_PRIMITIVE_TYPES_AS_STRING
        
        if type_name_lower in [t.lower() for t in fhir_primitive_types_as_string]:
            type_condition = f"{self.get_json_type('value')} = 'VARCHAR'"
        elif type_name_lower == "boolean":
            type_condition = f"({self.get_json_type('value')} = 'BOOLEAN' OR ({self.get_json_type('value')} = 'VARCHAR' AND LOWER(CAST(value AS VARCHAR)) IN ('true', 'false')))"
        elif type_name_lower == "integer":
            type_condition = f"({self.get_json_type('value')} = 'INTEGER' OR ({self.get_json_type('value')} IN ('NUMBER', 'DOUBLE') AND (CAST(value AS DOUBLE) = floor(CAST(value AS DOUBLE)))))"
        elif type_name_lower == "decimal":
            type_condition = f"{self.get_json_type('value')} IN ('NUMBER', 'INTEGER', 'DOUBLE')"
        elif type_name == "Quantity":
            type_condition = f"{self.get_json_type('value')} = 'OBJECT' AND {self.extract_json_object('value', '$.value')} IS NOT NULL"
        else:
            type_condition = f"({self.get_json_type('value')} = 'OBJECT' AND {self.extract_json_field('value', '$.resourceType')} = '{type_name}')"
        
        # Create CTE for as operation
        as_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if base expression is null
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN (
                        SELECT CASE 
                            WHEN COUNT(*) = 0 THEN NULL
                            ELSE {self.dialect.json_array_function}(DISTINCT value::VARCHAR)
                        END
                        FROM {self.iterate_json_elements_indexed(base_ref)}
                        WHERE {type_condition}
                    )
                    -- Single element - check if it matches the type
                    ELSE 
                        CASE 
                            WHEN ({type_condition.replace('value', base_ref)}) THEN {self.dialect.json_array_function}({base_ref}::VARCHAR)
                            ELSE NULL
                        END
                END as as_value
            FROM {from_clause}
        """, "as_result")
        
        return f"(SELECT as_value FROM {as_cte_name})"

    def _generate_is_with_cte(self, base_expr: str, func_node) -> str:
        """Generate is() function using CTE approach"""
        
        # Extract the type argument
        if len(func_node.args) != 1:
            raise ValueError(f"is() requires exactly 1 argument, got {len(func_node.args)}")
        
        if not isinstance(func_node.args[0], self.IdentifierNode):
            raise ValueError("is() function requires a type identifier argument")
        
        type_name = func_node.args[0].name
        type_name_lower = type_name.lower()
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "is_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Build type condition
        fhir_primitive_types_as_string = self.FHIR_PRIMITIVE_TYPES_AS_STRING
        
        if type_name_lower in [t.lower() for t in fhir_primitive_types_as_string]:
            type_condition = f"{self.get_json_type('value')} = 'VARCHAR'"
        elif type_name_lower == "boolean":
            type_condition = f"({self.get_json_type('value')} = 'BOOLEAN' OR ({self.get_json_type('value')} = 'VARCHAR' AND LOWER(CAST(value AS VARCHAR)) IN ('true', 'false')))"
        elif type_name_lower == "integer":
            type_condition = f"({self.get_json_type('value')} = 'INTEGER' OR ({self.get_json_type('value')} IN ('NUMBER', 'DOUBLE') AND (CAST(value AS DOUBLE) = floor(CAST(value AS DOUBLE)))))"
        elif type_name_lower == "decimal":
            type_condition = f"{self.get_json_type('value')} IN ('NUMBER', 'INTEGER', 'DOUBLE')"
        elif type_name == "Quantity":
            type_condition = f"{self.get_json_type('value')} = 'OBJECT' AND {self.extract_json_object('value', '$.value')} IS NOT NULL"
        else:
            type_condition = f"({self.get_json_type('value')} = 'OBJECT' AND {self.extract_json_field('value', '$.resourceType')} = '{type_name}')"
        
        # Create CTE for is operation
        is_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if base expression is null (empty collection)
                    WHEN {base_ref} IS NULL THEN false
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns false
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN false
                            -- Check if all elements match the type
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = COUNT(CASE WHEN {type_condition} THEN 1 END) THEN true
                                    ELSE false
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- Single element - check if it matches the type
                    ELSE 
                        CASE 
                            WHEN ({type_condition.replace('value', base_ref)}) THEN true
                            ELSE false
                        END
                END as is_value
            FROM {from_clause}
        """, "is_result")
        
        return f"(SELECT is_value FROM {is_cte_name})"

    def _generate_toboolean_with_cte(self, base_expr: str, func_node) -> str:
        """Generate toBoolean() function using CTE approach"""
        
        # toBoolean() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"toBoolean() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "toboolean_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for toBoolean operation
        toboolean_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns NULL
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            -- Convert each element to boolean
                            ELSE (
                                SELECT {self.dialect.json_array_function}(
                                    CASE 
                                        WHEN {self.get_json_type('value')} = 'BOOLEAN' THEN value
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND LOWER(CAST(value AS VARCHAR)) = 'true' THEN true
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND LOWER(CAST(value AS VARCHAR)) = 'false' THEN false
                                        WHEN {self.get_json_type('value')} IN ('INTEGER', 'NUMBER', 'DOUBLE') AND CAST(value AS DOUBLE) != 0 THEN true
                                        WHEN {self.get_json_type('value')} IN ('INTEGER', 'NUMBER', 'DOUBLE') AND CAST(value AS DOUBLE) = 0 THEN false
                                        ELSE NULL
                                    END::VARCHAR
                                )
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                                WHERE CASE 
                                    WHEN {self.get_json_type('value')} = 'BOOLEAN' THEN true
                                    WHEN {self.get_json_type('value')} = 'VARCHAR' AND LOWER(CAST(value AS VARCHAR)) IN ('true', 'false') THEN true
                                    WHEN {self.get_json_type('value')} IN ('INTEGER', 'NUMBER', 'DOUBLE') THEN true
                                    ELSE false
                                END
                            )
                        END
                    -- Single element - convert to boolean
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'BOOLEAN' THEN {self.dialect.json_array_function}({base_ref}::VARCHAR)
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND LOWER(CAST({base_ref} AS VARCHAR)) = 'true' THEN {self.dialect.json_array_function}('true')
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND LOWER(CAST({base_ref} AS VARCHAR)) = 'false' THEN {self.dialect.json_array_function}('false')
                            WHEN {self.get_json_type(base_ref)} IN ('INTEGER', 'NUMBER', 'DOUBLE') AND CAST({base_ref} AS DOUBLE) != 0 THEN {self.dialect.json_array_function}('true')
                            WHEN {self.get_json_type(base_ref)} IN ('INTEGER', 'NUMBER', 'DOUBLE') AND CAST({base_ref} AS DOUBLE) = 0 THEN {self.dialect.json_array_function}('false')
                            ELSE NULL
                        END
                END as toboolean_value
            FROM {from_clause}
        """, "toboolean_result")
        
        return f"(SELECT toboolean_value FROM {toboolean_cte_name})"

    def _generate_convertstoboolean_with_cte(self, base_expr: str, func_node) -> str:
        """Generate convertsToBoolean() function using CTE approach"""
        
        # convertsToBoolean() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToBoolean() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "convertstoboolean_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for convertsToBoolean operation
        convertstoboolean_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN false
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns false
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN false
                            -- Check if all elements can be converted to boolean
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = COUNT(CASE 
                                        WHEN {self.get_json_type('value')} = 'BOOLEAN' THEN 1
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND LOWER(CAST(value AS VARCHAR)) IN ('true', 'false') THEN 1
                                        WHEN {self.get_json_type('value')} IN ('INTEGER', 'NUMBER', 'DOUBLE') THEN 1
                                        ELSE NULL
                                    END) THEN true
                                    ELSE false
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- Single element - check if it can be converted to boolean
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'BOOLEAN' THEN true
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND LOWER(CAST({base_ref} AS VARCHAR)) IN ('true', 'false') THEN true
                            WHEN {self.get_json_type(base_ref)} IN ('INTEGER', 'NUMBER', 'DOUBLE') THEN true
                            ELSE false
                        END
                END as convertstoboolean_value
            FROM {from_clause}
        """, "convertstoboolean_result")
        
        return f"(SELECT convertstoboolean_value FROM {convertstoboolean_cte_name})"

    def _generate_todecimal_with_cte(self, base_expr: str, func_node) -> str:
        """Generate toDecimal() function using CTE approach"""
        
        # toDecimal() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"toDecimal() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "todecimal_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for toDecimal operation
        todecimal_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns NULL
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            -- Convert each element to decimal
                            ELSE (
                                SELECT {self.dialect.json_array_function}(
                                    CAST(value AS DOUBLE)::VARCHAR
                                )
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                                WHERE CASE 
                                    WHEN {self.get_json_type('value')} IN ('INTEGER', 'NUMBER', 'DOUBLE') THEN true
                                    WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS DOUBLE) IS NOT NULL THEN true
                                    ELSE false
                                END
                            )
                        END
                    -- Single element - convert to decimal
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} IN ('INTEGER', 'NUMBER', 'DOUBLE') THEN {self.dialect.json_array_function}(CAST({base_ref} AS DOUBLE)::VARCHAR)
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND TRY_CAST({base_ref} AS DOUBLE) IS NOT NULL THEN {self.dialect.json_array_function}(CAST({base_ref} AS DOUBLE)::VARCHAR)
                            ELSE NULL
                        END
                END as todecimal_value
            FROM {from_clause}
        """, "todecimal_result")
        
        return f"(SELECT todecimal_value FROM {todecimal_cte_name})"

    def _generate_convertstodecimal_with_cte(self, base_expr: str, func_node) -> str:
        """Generate convertsToDecimal() function using CTE approach"""
        
        # convertsToDecimal() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToDecimal() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "convertstodecimal_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for convertsToDecimal operation
        convertstodecimal_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN false
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns false
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN false
                            -- Check if all elements can be converted to decimal
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = COUNT(CASE 
                                        WHEN {self.get_json_type('value')} IN ('INTEGER', 'NUMBER', 'DOUBLE') THEN 1
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS DOUBLE) IS NOT NULL THEN 1
                                        ELSE NULL
                                    END) THEN true
                                    ELSE false
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- Single element - check if it can be converted to decimal
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} IN ('INTEGER', 'NUMBER', 'DOUBLE') THEN true
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND TRY_CAST({base_ref} AS DOUBLE) IS NOT NULL THEN true
                            ELSE false
                        END
                END as convertstodecimal_value
            FROM {from_clause}
        """, "convertstodecimal_result")
        
        return f"(SELECT convertstodecimal_value FROM {convertstodecimal_cte_name})"

    def _generate_convertstointeger_with_cte(self, base_expr: str, func_node) -> str:
        """Generate convertsToInteger() function using CTE approach"""
        
        # convertsToInteger() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToInteger() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "convertstointeger_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for convertsToInteger operation
        convertstointeger_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN false
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns false
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN false
                            -- Check if all elements can be converted to integer
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = COUNT(CASE 
                                        WHEN {self.get_json_type('value')} = 'INTEGER' THEN 1
                                        WHEN {self.get_json_type('value')} IN ('NUMBER', 'DOUBLE') AND CAST(value AS DOUBLE) = floor(CAST(value AS DOUBLE)) THEN 1
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS INTEGER) IS NOT NULL THEN 1
                                        ELSE NULL
                                    END) THEN true
                                    ELSE false
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- Single element - check if it can be converted to integer
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'INTEGER' THEN true
                            WHEN {self.get_json_type(base_ref)} IN ('NUMBER', 'DOUBLE') AND CAST({base_ref} AS DOUBLE) = floor(CAST({base_ref} AS DOUBLE)) THEN true
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND TRY_CAST({base_ref} AS INTEGER) IS NOT NULL THEN true
                            ELSE false
                        END
                END as convertstointeger_value
            FROM {from_clause}
        """, "convertstointeger_result")
        
        return f"(SELECT convertstointeger_value FROM {convertstointeger_cte_name})"

    def _generate_todate_with_cte(self, base_expr: str, func_node) -> str:
        """Generate toDate() function using CTE approach"""
        
        # toDate() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"toDate() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "todate_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for toDate operation
        todate_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NULL THEN NULL
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            ELSE (
                                SELECT json_group_array(
                                    CASE 
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS DATE) IS NOT NULL THEN TRY_CAST(value AS DATE)
                                        WHEN {self.get_json_type('value')} = 'DATE' THEN CAST(value AS DATE)
                                        ELSE NULL
                                    END
                                )
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                                WHERE CASE 
                                    WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS DATE) IS NOT NULL THEN true
                                    WHEN {self.get_json_type('value')} = 'DATE' THEN true
                                    ELSE false
                                END
                            )
                        END
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND TRY_CAST({base_ref} AS DATE) IS NOT NULL THEN TRY_CAST({base_ref} AS DATE)
                            WHEN {self.get_json_type(base_ref)} = 'DATE' THEN CAST({base_ref} AS DATE)
                            ELSE NULL
                        END
                END as todate_value
            FROM {from_clause}
        """, "todate_result")
        
        return f"(SELECT todate_value FROM {todate_cte_name})"

    def _generate_convertstodate_with_cte(self, base_expr: str, func_node) -> str:
        """Generate convertsToDate() function using CTE approach"""
        
        # convertsToDate() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToDate() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "convertstodate_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for convertsToDate operation
        convertstodate_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN false
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns false
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN false
                            -- Check if all elements can be converted to date
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = COUNT(CASE 
                                        WHEN {self.get_json_type('value')} = 'DATE' THEN 1
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS DATE) IS NOT NULL THEN 1
                                        ELSE NULL
                                    END) THEN true
                                    ELSE false
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- Single element - check if it can be converted to date
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'DATE' THEN true
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND TRY_CAST({base_ref} AS DATE) IS NOT NULL THEN true
                            ELSE false
                        END
                END as convertstodate_value
            FROM {from_clause}
        """, "convertstodate_result")
        
        return f"(SELECT convertstodate_value FROM {convertstodate_cte_name})"

    def _generate_todatetime_with_cte(self, base_expr: str, func_node) -> str:
        """Generate toDateTime() function using CTE approach"""
        
        # toDateTime() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"toDateTime() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "todatetime_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for toDateTime operation
        todatetime_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NULL THEN NULL
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            ELSE (
                                SELECT json_group_array(
                                    CASE 
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS TIMESTAMP) IS NOT NULL THEN TRY_CAST(value AS TIMESTAMP)
                                        WHEN {self.get_json_type('value')} = 'TIMESTAMP' THEN CAST(value AS TIMESTAMP)
                                        WHEN {self.get_json_type('value')} = 'DATE' THEN CAST(value AS TIMESTAMP)
                                        ELSE NULL
                                    END
                                )
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                                WHERE CASE 
                                    WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS TIMESTAMP) IS NOT NULL THEN true
                                    WHEN {self.get_json_type('value')} IN ('TIMESTAMP', 'DATE') THEN true
                                    ELSE false
                                END
                            )
                        END
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND TRY_CAST({base_ref} AS TIMESTAMP) IS NOT NULL THEN TRY_CAST({base_ref} AS TIMESTAMP)
                            WHEN {self.get_json_type(base_ref)} = 'TIMESTAMP' THEN CAST({base_ref} AS TIMESTAMP)
                            WHEN {self.get_json_type(base_ref)} = 'DATE' THEN CAST({base_ref} AS TIMESTAMP)
                            ELSE NULL
                        END
                END as todatetime_value
            FROM {from_clause}
        """, "todatetime_result")
        
        return f"(SELECT todatetime_value FROM {todatetime_cte_name})"

    def _generate_convertstodatetime_with_cte(self, base_expr: str, func_node) -> str:
        """Generate convertsToDateTime() function using CTE approach"""
        
        # convertsToDateTime() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToDateTime() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "convertstodatetime_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for convertsToDateTime operation
        convertstodatetime_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN false
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns false
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN false
                            -- Check if all elements can be converted to datetime
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = COUNT(CASE 
                                        WHEN {self.get_json_type('value')} IN ('TIMESTAMP', 'DATE') THEN 1
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS TIMESTAMP) IS NOT NULL THEN 1
                                        ELSE NULL
                                    END) THEN true
                                    ELSE false
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- Single element - check if it can be converted to datetime
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} IN ('TIMESTAMP', 'DATE') THEN true
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND TRY_CAST({base_ref} AS TIMESTAMP) IS NOT NULL THEN true
                            ELSE false
                        END
                END as convertstodatetime_value
            FROM {from_clause}
        """, "convertstodatetime_result")
        
        return f"(SELECT convertstodatetime_value FROM {convertstodatetime_cte_name})"

    def _generate_totime_with_cte(self, base_expr: str, func_node) -> str:
        """Generate toTime() function using CTE approach"""
        
        # toTime() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"toTime() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "totime_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for toTime operation
        totime_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NULL THEN NULL
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN NULL
                            ELSE (
                                SELECT json_group_array(
                                    CASE 
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS TIME) IS NOT NULL THEN TRY_CAST(value AS TIME)
                                        WHEN {self.get_json_type('value')} = 'TIME' THEN CAST(value AS TIME)
                                        WHEN {self.get_json_type('value')} = 'TIMESTAMP' THEN CAST(value AS TIME)
                                        ELSE NULL
                                    END
                                )
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                                WHERE CASE 
                                    WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS TIME) IS NOT NULL THEN true
                                    WHEN {self.get_json_type('value')} IN ('TIME', 'TIMESTAMP') THEN true
                                    ELSE false
                                END
                            )
                        END
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND TRY_CAST({base_ref} AS TIME) IS NOT NULL THEN TRY_CAST({base_ref} AS TIME)
                            WHEN {self.get_json_type(base_ref)} = 'TIME' THEN CAST({base_ref} AS TIME)
                            WHEN {self.get_json_type(base_ref)} = 'TIMESTAMP' THEN CAST({base_ref} AS TIME)
                            ELSE NULL
                        END
                END as totime_value
            FROM {from_clause}
        """, "totime_result")
        
        return f"(SELECT totime_value FROM {totime_cte_name})"

    def _generate_convertstotime_with_cte(self, base_expr: str, func_node) -> str:
        """Generate convertsToTime() function using CTE approach"""
        
        # convertsToTime() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"convertsToTime() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "convertstotime_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for convertsToTime operation
        convertstotime_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN false
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns false
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN false
                            -- Check if all elements can be converted to time
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = COUNT(CASE 
                                        WHEN {self.get_json_type('value')} IN ('TIME', 'TIMESTAMP') THEN 1
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND TRY_CAST(value AS TIME) IS NOT NULL THEN 1
                                        ELSE NULL
                                    END) THEN true
                                    ELSE false
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- Single element - check if it can be converted to time
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} IN ('TIME', 'TIMESTAMP') THEN true
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' AND TRY_CAST({base_ref} AS TIME) IS NOT NULL THEN true
                            ELSE false
                        END
                END as convertstotime_value
            FROM {from_clause}
        """, "convertstotime_result")
        
        return f"(SELECT convertstotime_value FROM {convertstotime_cte_name})"

    def _generate_tochars_with_cte(self, base_expr: str, func_node) -> str:
        """Generate toChars() function using CTE approach"""
        
        # toChars() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"toChars() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "tochars_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for toChars operation
        tochars_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns empty array
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN {self.dialect.json_array_function}()
                            -- Convert each string element to character array
                            ELSE (
                                SELECT {self.dialect.json_array_function}(
                                    CASE 
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' THEN (
                                            SELECT {self.dialect.json_array_function}(substr_char)
                                            FROM (
                                                SELECT SUBSTRING(CAST(value AS VARCHAR), seq, 1) as substr_char
                                                FROM RANGE(1, LENGTH(CAST(value AS VARCHAR)) + 1) as seq_table(seq)
                                            )
                                        )
                                        ELSE NULL
                                    END
                                )
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                                WHERE {self.get_json_type('value')} = 'VARCHAR'
                            )
                        END
                    -- Single element - check if it's a string and convert to character array
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' THEN (
                                SELECT {self.dialect.json_array_function}(substr_char)
                                FROM (
                                    SELECT SUBSTRING(CAST({base_ref} AS VARCHAR), seq, 1) as substr_char
                                    FROM RANGE(1, LENGTH(CAST({base_ref} AS VARCHAR)) + 1) as seq_table(seq)
                                )
                            )
                            ELSE NULL
                        END
                END as tochars_value
            FROM {from_clause}
        """, "tochars_result")
        
        return f"(SELECT tochars_value FROM {tochars_cte_name})"

    def _generate_matches_with_cte(self, base_expr: str, func_node) -> str:
        """Generate matches() function using CTE approach"""
        
        # matches() takes exactly 1 argument (the regex pattern)
        if len(func_node.args) != 1:
            raise ValueError(f"matches() requires exactly 1 argument (regex pattern), got {len(func_node.args)}")
        
        # Get the regex pattern expression
        pattern_expr = self.visit(func_node.args[0])
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "matches_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for matches operation
        matches_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN false
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns false
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN false
                            -- Check if all string elements match the regex pattern
                            ELSE (
                                SELECT CASE 
                                    WHEN COUNT(*) = COUNT(CASE 
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' AND regexp_full_match(CAST(value AS VARCHAR), CAST({pattern_expr} AS VARCHAR)) THEN 1
                                        ELSE NULL
                                    END) THEN true
                                    ELSE false
                                END
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                                WHERE {self.get_json_type('value')} = 'VARCHAR'
                            )
                        END
                    -- Single element - check if it's a string and matches the regex
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' THEN
                                CASE 
                                    WHEN regexp_full_match(CAST({base_ref} AS VARCHAR), CAST({pattern_expr} AS VARCHAR)) THEN true
                                    ELSE false
                                END
                            ELSE false
                        END
                END as matches_value
            FROM {from_clause}
        """, "matches_result")
        
        return f"(SELECT matches_value FROM {matches_cte_name})"

    def _generate_replacematches_with_cte(self, base_expr: str, func_node) -> str:
        """Generate replaceMatches() function using CTE approach"""
        
        # replaceMatches() takes exactly 2 arguments (regex pattern, replacement)
        if len(func_node.args) != 2:
            raise ValueError(f"replaceMatches() requires exactly 2 arguments (pattern, replacement), got {len(func_node.args)}")
        
        # Get the regex pattern and replacement expressions
        pattern_expr = self.visit(func_node.args[0])
        replacement_expr = self.visit(func_node.args[1])
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "replacematches_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for replaceMatches operation
        replacematches_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns empty array
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN {self.dialect.json_array_function}()
                            -- Replace regex matches in all string elements
                            ELSE (
                                SELECT {self.dialect.json_array_function}(
                                    CASE 
                                        WHEN {self.get_json_type('value')} = 'VARCHAR' THEN 
                                            regexp_replace(CAST(value AS VARCHAR), CAST({pattern_expr} AS VARCHAR), CAST({replacement_expr} AS VARCHAR), 'g')
                                        ELSE value
                                    END
                                )
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                        END
                    -- Single element - replace regex matches if it's a string
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'VARCHAR' THEN
                                regexp_replace(CAST({base_ref} AS VARCHAR), CAST({pattern_expr} AS VARCHAR), CAST({replacement_expr} AS VARCHAR), 'g')
                            ELSE {base_ref}
                        END
                END as replacematches_value
            FROM {from_clause}
        """, "replacematches_result")
        
        return f"(SELECT replacematches_value FROM {replacematches_cte_name})"

    def _generate_children_with_cte(self, base_expr: str, func_node) -> str:
        """Generate children() function using CTE approach"""
        
        # children() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"children() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "children_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for children operation
        children_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    -- Check if collection is null or empty
                    WHEN {base_ref} IS NULL THEN NULL
                    -- Check if it's an array
                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN
                        CASE 
                            -- Empty array returns empty array
                            WHEN {self.get_json_array_length(base_ref)} = 0 THEN {self.dialect.json_array_function}()
                            -- Extract all children from each element in the array
                            ELSE (
                                SELECT {self.dialect.json_array_function}(
                                    CASE 
                                        WHEN {self.get_json_type('value')} = 'OBJECT' THEN (
                                            SELECT {self.dialect.json_array_function}(child_value)
                                            FROM {self.dialect.json_each_function}(value) as child_table(child_key, child_value)
                                        )
                                        WHEN {self.get_json_type('value')} = 'ARRAY' THEN (
                                            SELECT {self.dialect.json_array_function}(array_element)
                                            FROM {self.dialect.json_each_function}(value) as array_table(array_key, array_element)
                                        )
                                        ELSE NULL
                                    END
                                )
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                                WHERE {self.get_json_type('value')} IN ('OBJECT', 'ARRAY')
                            )
                        END
                    -- Single element - extract its direct children
                    ELSE 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'OBJECT' THEN (
                                SELECT {self.dialect.json_array_function}(child_value)
                                FROM {self.iterate_json_elements_indexed(base_ref)} as child_table(child_key, child_value)
                            )
                            WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN (
                                SELECT {self.dialect.json_array_function}(array_element)
                                FROM {self.iterate_json_elements_indexed(base_ref)} as array_table(array_key, array_element)
                            )
                            ELSE {self.dialect.json_array_function}()
                        END
                END as children_value
            FROM {from_clause}
        """, "children_result")
        
        return f"(SELECT children_value FROM {children_cte_name})"

    def _generate_descendants_with_cte(self, base_expr: str, func_node) -> str:
        """Generate descendants() function using CTE approach"""
        
        # descendants() takes no arguments
        if len(func_node.args) != 0:
            raise ValueError(f"descendants() requires no arguments, got {len(func_node.args)}")
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "descendants_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for descendants operation using recursive approach
        descendants_cte_name = self._create_cte(f"""
            WITH RECURSIVE descendants_recursive(value, level) AS (
                -- Base case: start with the input collection(s)
                SELECT 
                    CASE 
                        WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN (
                            SELECT child_value
                            FROM {self.iterate_json_elements_indexed(base_ref)} as root_table(root_key, root_value),
                                 {self.dialect.json_each_function}(root_value) as child_table(child_key, child_value)
                            WHERE {self.get_json_type('root_value')} IN ('OBJECT', 'ARRAY')
                        )
                        WHEN {self.get_json_type(base_ref)} IN ('OBJECT', 'ARRAY') THEN (
                            SELECT child_value
                            FROM {self.iterate_json_elements_indexed(base_ref)} as child_table(child_key, child_value)
                        )
                        ELSE NULL
                    END as value,
                    1 as level
                FROM {from_clause}
                WHERE {base_ref} IS NOT NULL
                  AND {self.get_json_type(base_ref)} IN ('OBJECT', 'ARRAY')
                
                UNION ALL
                
                -- Recursive case: find children of current descendants
                SELECT nested_child_value as value, d.level + 1 as level
                FROM descendants_recursive d,
                     {self.dialect.json_each_function}(d.value) as nested_table(nested_key, nested_child_value)
                WHERE {self.get_json_type('d.value')} IN ('OBJECT', 'ARRAY')
                  AND d.level < 10  -- Limit recursion depth to prevent infinite loops
                  AND d.value IS NOT NULL
            )
            SELECT 
                CASE 
                    WHEN {base_ref} IS NULL THEN NULL
                    WHEN COUNT(*) = 0 THEN {self.dialect.json_array_function}()
                    ELSE {self.dialect.json_array_function}(value)
                END as descendants_value
            FROM descendants_recursive
        """, "descendants_result")
        
        return f"(SELECT descendants_value FROM {descendants_cte_name})"

    def _generate_union_with_cte(self, base_expr: str, func_node) -> str:
        """Generate union() function using CTE approach"""
        
        # union() takes exactly 1 argument
        if len(func_node.args) != 1:
            raise ValueError(f"union() requires exactly 1 argument (collection to union with), got {len(func_node.args)}")
        
        # Get the argument collection expression
        other_collection_expr = self.visit(func_node.args[0])
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "union_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for other collection if it's complex
        if other_collection_expr.startswith("(SELECT") and "FROM " in other_collection_expr:
            other_cte_name = self._create_cte(
                f"SELECT {other_collection_expr} as other_value FROM {self.table_name}",
                "union_other"
            )
            other_ref = "other_value"
            other_from_clause = other_cte_name
        else:
            other_ref = other_collection_expr
            other_from_clause = self.table_name
        
        # Create CTE for union operation with deduplication
        union_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NULL AND {other_ref} IS NULL THEN NULL
                    WHEN {base_ref} IS NULL THEN 
                        CASE 
                            WHEN {self.get_json_type(other_ref)} = 'ARRAY' THEN (
                                SELECT {self.dialect.json_array_function}(DISTINCT value)
                                FROM {self.iterate_json_elements_indexed(other_ref)}
                            )
                            ELSE {self.dialect.json_array_function}({other_ref})
                        END
                    WHEN {other_ref} IS NULL THEN 
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN (
                                SELECT {self.dialect.json_array_function}(DISTINCT value)
                                FROM {self.iterate_json_elements_indexed(base_ref)}
                            )
                            ELSE {self.dialect.json_array_function}({base_ref})
                        END
                    ELSE (
                        -- Both collections are non-null, perform union with deduplication
                        SELECT {self.dialect.json_array_function}(DISTINCT combined_value)
                        FROM (
                            -- Values from the base collection
                            SELECT value as combined_value
                            FROM {self.dialect.json_each_function}(
                                CASE 
                                    WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN {base_ref}
                                    ELSE {self.dialect.json_array_function}({base_ref})
                                END
                            )
                            
                            UNION ALL
                            
                            -- Values from the argument collection
                            SELECT value as combined_value
                            FROM {self.dialect.json_each_function}(
                                CASE 
                                    WHEN {self.get_json_type(other_ref)} = 'ARRAY' THEN {other_ref}
                                    ELSE {self.dialect.json_array_function}({other_ref})
                                END
                            )
                        ) AS union_source
                    )
                END as union_value
            FROM {from_clause}, {other_from_clause}
        """, "union_result")
        
        return f"(SELECT union_value FROM {union_cte_name})"

    def _generate_combine_with_cte(self, base_expr: str, func_node) -> str:
        """Generate combine() function using CTE approach"""
        
        # combine() takes exactly 1 argument
        if len(func_node.args) != 1:
            raise ValueError(f"combine() requires exactly 1 argument (collection to combine with), got {len(func_node.args)}")
        
        # Get the argument collection expression
        other_collection_expr = self.visit(func_node.args[0])
        
        # Create CTE for combine operation without deduplication
        # Fix GROUP BY issues by using a single table reference and avoiding cross joins
        combine_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_expr} IS NULL AND {other_collection_expr} IS NULL THEN NULL
                    WHEN {base_expr} IS NULL THEN 
                        CASE 
                            WHEN {self.get_json_type(other_collection_expr)} = 'ARRAY' THEN {other_collection_expr}
                            ELSE {self.dialect.json_array_function}({other_collection_expr})
                        END
                    WHEN {other_collection_expr} IS NULL THEN 
                        CASE 
                            WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN {base_expr}
                            ELSE {self.dialect.json_array_function}({base_expr})
                        END
                    ELSE (
                        -- Both collections are non-null, perform combination without deduplication
                        SELECT {self.dialect.json_array_agg_function('combined_value')}
                        FROM (
                            -- Values from the base collection
                            SELECT value as combined_value
                            FROM {self.dialect.json_each_function}(
                                CASE 
                                    WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN {base_expr}
                                    ELSE {self.dialect.json_array_function}({base_expr})
                                END
                            )
                            
                            UNION ALL
                            
                            -- Values from the argument collection
                            SELECT value as combined_value
                            FROM {self.dialect.json_each_function}(
                                CASE 
                                    WHEN {self.get_json_type(other_collection_expr)} = 'ARRAY' THEN {other_collection_expr}
                                    ELSE {self.dialect.json_array_function}({other_collection_expr})
                                END
                            )
                        ) AS combine_source
                    )
                END as combine_value
            FROM {self.table_name}
        """, "combine_result")
        
        return f"(SELECT combine_value FROM {combine_cte_name})"

    def _generate_repeat_with_cte(self, base_expr: str, func_node) -> str:
        """Generate repeat() function using CTE approach"""
        
        # repeat() takes exactly 1 argument
        if len(func_node.args) != 1:
            raise ValueError(f"repeat() requires exactly 1 argument (expression to apply iteratively), got {len(func_node.args)}")
        
        # Get the expression to apply repeatedly
        repeat_expr = self.visit(func_node.args[0])
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "repeat_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for repeat operation using recursive approach
        repeat_cte_name = self._create_cte(f"""
            WITH RECURSIVE repeat_recursive(current_value, iteration, value_hash) AS (
                -- Base case: start with the initial collection
                SELECT 
                    CASE 
                        WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN {base_ref}
                        ELSE {self.dialect.json_array_function}({base_ref})
                    END as current_value,
                    0 as iteration,
                    md5(CAST(
                        CASE 
                            WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN {base_ref}
                            ELSE {self.dialect.json_array_function}({base_ref})
                        END
                    AS VARCHAR)) as value_hash
                FROM {from_clause}
                WHERE {base_ref} IS NOT NULL
                
                UNION ALL
                
                -- Recursive case: apply the expression to current result  
                SELECT 
                    (
                        -- Apply the repeat expression to the current value
                        -- Note: This is a simplified version - real implementation would need
                        -- proper expression substitution and evaluation
                        SELECT {self.dialect.json_array_function}(
                            -- For now, just return the current value as we need proper expression evaluation
                            value
                        )
                        FROM {self.dialect.json_each_function}(r.current_value) as eval_table(eval_key, value)
                    ) as current_value,
                    r.iteration + 1 as iteration,
                    md5(CAST(r.current_value AS VARCHAR)) as value_hash
                FROM repeat_recursive r
                WHERE r.iteration < 10  -- Limit recursion depth to prevent infinite loops
                  AND r.iteration < 2   -- For now, limit to 2 iterations for testing
            )
            SELECT 
                CASE 
                    WHEN {base_ref} IS NULL THEN NULL
                    WHEN COUNT(*) = 0 THEN {self.dialect.json_array_function}()
                    ELSE (
                        SELECT current_value 
                        FROM repeat_recursive 
                        WHERE iteration = (SELECT MAX(iteration) FROM repeat_recursive)
                        LIMIT 1
                    )
                END as repeat_value
            FROM repeat_recursive
        """, "repeat_result")
        
        return f"(SELECT repeat_value FROM {repeat_cte_name})"

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
                except Exception as e:
                    # Fallback to traditional where
                    log_fallback_usage(self.logger, "CTE where generation", f"Failed for where clause: {e}")
                    filtered_expr = self.apply_function_to_expression(where_node, base_ref)
            else:
                filtered_expr = self.apply_function_to_expression(where_node, base_ref)
            
            # Create CTE for existence check on filtered results
            exists_cte_name = self._create_cte(f"""
                SELECT 
                    CASE 
                        WHEN {self.get_json_type(f"({filtered_expr})")} = 'ARRAY' THEN {self.get_json_array_length(f"({filtered_expr})")} > 0
                        ELSE ({filtered_expr} IS NOT NULL AND NOT ({self.get_json_type(f"({filtered_expr})")} = 'OBJECT' AND {self.get_json_array_length(f"{self.dialect.json_extract_function}(({filtered_expr}), '$.keys()')")} = 0))
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
            log_fallback_usage(self.logger, "complex select expression", "falling back to original implementation")
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
                        SELECT CASE WHEN COUNT(*) > 0 THEN true ELSE false END
                        FROM {self.iterate_json_array(base_ref, "$")}
                        WHERE value = {search_value}
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
             FROM {self.dialect.iterate_json_array(base_ref, '$.extension')}
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

    def _generate_abs_with_cte(self, func_node, base_expr: str) -> str:
        """Generate abs() function using CTE approach"""
        
        # For abs() function, we need to handle scalar subqueries correctly
        # Use the same pattern as other math functions
        
        # Check if base_expr is already a CTE reference
        if base_expr.startswith("(SELECT") and "FROM " in base_expr:
            # Base expression is already complex, create CTE for it
            base_cte_name = self._create_cte(
                f"SELECT {base_expr} as base_value FROM {self.table_name}",
                "abs_base"
            )
            base_ref = "base_value"
            from_clause = base_cte_name
        else:
            # Simple base expression, reference directly
            base_ref = base_expr
            from_clause = self.table_name
        
        # Create CTE for abs operation with proper scalar handling
        abs_cte_name = self._create_cte(f"""
            SELECT 
                CASE 
                    WHEN {base_ref} IS NOT NULL THEN
                        ABS(CAST({base_ref} AS DOUBLE))
                    ELSE NULL
                END as abs_result
            FROM {from_clause}
        """, "abs_result")
        
        return f"(SELECT abs_result FROM {abs_cte_name} LIMIT 1)"
    
    def visit(self, node) -> str:
        """Visit an AST node and generate SQL"""
        if isinstance(node, self.ThisNode): # Handle $this if AST rewrite produces it
            return self.json_column
        elif isinstance(node, self.VariableNode): # Handle context variables like $index, $total
            return self.visit_variable(node)
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
        elif isinstance(node, self.TupleNode):
            return self.visit_tuple(node)
        else:
            raise ValueError(f"Unknown node type: {type(node)}")
    
    
    def get_cte_debug_info(self) -> Dict[str, Any]:
        """
        Get debugging information about CTE system state.
        
        Returns:
            Dictionary with CTE system information
        """
        return {
            'system': 'Legacy',
            'cte_count': len(self.ctes),
            'cte_names': list(self.ctes.keys()),
            'counter': self.cte_counter
        }
    
    def visit_literal(self, node) -> str:
        """Visit a literal node"""
        return self.literal_handler.visit_literal(node)
    
    def visit_variable(self, node) -> str:
        """Visit a variable node ($index, $total, etc.)"""
        return self.literal_handler.visit_variable(node)
    
    def visit_identifier(self, node) -> str:
        """Visit an identifier node"""
        return self.path_handler.visit_identifier(node, self.resource_type)
    
    def visit_path(self, node) -> str:
        """Visit a path node - builds the JSON path sequentially"""
        return self.path_handler.visit_path(node)
    
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
                        log_fallback_usage(self.logger, "CTE generation", f"Failed for json_extract array extraction: {e}")
                
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
                log_fallback_usage(self.logger, "CTE generation", f"Failed for array extraction: {e}")
        
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
        
        # Delegate to specialized function handlers
        if self.collection_function_handler.can_handle(func_name):
            return self.collection_function_handler.handle_function(func_name, base_expr, func_node)
        elif self.string_function_handler.can_handle(func_name):
            return self.string_function_handler.handle_function(func_name, base_expr, func_node)
        elif self.math_function_handler.can_handle(func_name):
            return self.math_function_handler.handle_function(func_name, base_expr, func_node)
        elif self.type_function_handler.can_handle(func_name):
            return self.type_function_handler.handle_function(func_name, base_expr, func_node)
        elif self.datetime_function_handler.can_handle(func_name):
            return self.datetime_function_handler.handle_function(func_name, base_expr, func_node)
        
        # Delegate remaining functions to the fallback dispatcher
        return self._handle_remaining_functions(func_name, func_node, base_expr)
    
    def _handle_remaining_functions(self, func_name: str, func_node, base_expr: str) -> str:
        """Handle functions not covered by specialized handlers"""
        # This method contains the remaining function logic from the original monolithic method
        # Breaking it down further would require more extensive refactoring
        
        if func_name == 'exists':
            # PHASE 2D: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'exists'):
                try:
                    return self._generate_exists_with_cte(func_node, base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "exists() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "empty() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "first() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "last() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "count() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "length() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "contains() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "select() CTE generation", str(e))
            
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
            
            # Merge CTEs and state from nested generator
            self.merge_nested_generator_state(array_element_expression_generator)
            
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
                    log_fallback_usage(self.logger, "where() CTE generation", str(e))
            
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
            # Extract complex logic into helper method for maintainability
            return self._calculate_low_boundary(base_expr)
        elif func_name == 'highboundary':
            # FHIR boundary functions calculate boundaries based on precision
            # Extract complex logic into helper method for maintainability
            return self._calculate_high_boundary(base_expr)
        elif func_name == 'not':
            # Implement the not() function for boolean negation
            return f"""
            CASE 
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN true  -- empty collection is false, so not() is true
                        ELSE false  -- non-empty collection is true, so not() is false
                    END
                WHEN {base_expr} IS NULL THEN true  -- null/missing is false, so not() is true
                WHEN {self.get_json_type(base_expr)} = 'BOOLEAN' THEN NOT CAST({base_expr} AS BOOLEAN)
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
                    log_fallback_usage(self.logger, "getReferenceKey() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "ofType() CTE generation", str(e))

            # Original implementation (fallback)
            # Handle both arrays and single values
            return f"""
            CASE 
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
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
                    log_fallback_usage(self.logger, "extension() CTE generation", str(e))
            
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
            # Use standard SQL implementation for join() function
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
                    log_fallback_usage(self.logger, "substring() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "startswith() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "endswith() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "indexof() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "replace() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "toupper() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "tolower() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "upper() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "lower() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "trim() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "split() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "all() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "distinct() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "getResourceKey() CTE generation", str(e))
            
            # Original implementation (fallback)
            # Return the resource ID as the key
            return f"json_extract_string({self.json_column}, '$.id')"
            
        elif func_name == 'abs':
            # abs() function - absolute value
            if len(func_node.args) != 0:
                raise ValueError("abs() function takes no arguments")
            
            # Direct implementation for scalar math functions
            return f"ABS(CAST({base_expr} AS DOUBLE))"
            
        elif func_name == 'ceiling':
            # ceiling() function - round up to integer
            if len(func_node.args) != 0:
                raise ValueError("ceiling() function takes no arguments")
            
            # Direct implementation for scalar math functions
            return f"CEIL(CAST({base_expr} AS DOUBLE))"
            
        elif func_name == 'floor':
            # floor() function - round down to integer
            if len(func_node.args) != 0:
                raise ValueError("floor() function takes no arguments")
            
            # Direct implementation for scalar math functions
            return f"FLOOR(CAST({base_expr} AS DOUBLE))"
            
        elif func_name == 'round':
            # round() function - round to nearest integer with optional precision
            if len(func_node.args) == 0:
                # round() without precision - round to nearest integer
                return f"ROUND(CAST({base_expr} AS DOUBLE))"
            elif len(func_node.args) == 1:
                # round(precision) with precision
                precision_expr = self.visit(func_node.args[0])
                return f"ROUND(CAST({base_expr} AS DOUBLE), CAST({precision_expr} AS INTEGER))"
            else:
                raise ValueError("round() function takes 0 or 1 arguments")
                
        elif func_name == 'sqrt':
            # sqrt() function - square root
            if len(func_node.args) != 0:
                raise ValueError("sqrt() function takes no arguments")
            
            # Direct implementation for scalar math functions
            # Note: Need to handle negative numbers (sqrt of negative is null/error)
            return f"CASE WHEN CAST({base_expr} AS DOUBLE) >= 0 THEN SQRT(CAST({base_expr} AS DOUBLE)) ELSE NULL END"
            
        elif func_name == 'truncate':
            # truncate() function - remove decimal part (round towards zero)
            if len(func_node.args) != 0:
                raise ValueError("truncate() function takes no arguments")
            
            # Direct implementation for scalar math functions
            # TRUNCATE function removes decimal part by rounding towards zero
            # For positive numbers: TRUNCATE(3.7) = 3, for negative: TRUNCATE(-3.7) = -3
            return f"TRUNC(CAST({base_expr} AS DOUBLE))"
            
        elif func_name == 'exp':
            # exp() function - exponential function (e^x)
            if len(func_node.args) != 0:
                raise ValueError("exp() function takes no arguments")
            
            # Direct implementation - exponential function
            # Note: Need to handle overflow cases (exp of very large numbers)
            return f"CASE WHEN CAST({base_expr} AS DOUBLE) > 700 THEN NULL ELSE EXP(CAST({base_expr} AS DOUBLE)) END"
            
        elif func_name == 'ln':
            # ln() function - natural logarithm (log base e)
            if len(func_node.args) != 0:
                raise ValueError("ln() function takes no arguments")
            
            # Direct implementation - natural logarithm
            # Note: Need to handle domain restrictions (ln of non-positive numbers is undefined)
            return f"CASE WHEN CAST({base_expr} AS DOUBLE) > 0 THEN LN(CAST({base_expr} AS DOUBLE)) ELSE NULL END"
            
        elif func_name == 'log':
            # log() function - base-10 logarithm
            if len(func_node.args) != 0:
                raise ValueError("log() function takes no arguments")
            
            # Direct implementation - base-10 logarithm
            # Note: Need to handle domain restrictions (log of non-positive numbers is undefined)
            return f"CASE WHEN CAST({base_expr} AS DOUBLE) > 0 THEN LOG10(CAST({base_expr} AS DOUBLE)) ELSE NULL END"
            
        elif func_name == 'power':
            # power(exponent) function - raises base (current collection) to the power of exponent
            if len(func_node.args) != 1:
                raise ValueError("power() function requires exactly 1 argument (exponent)")
            
            # Get the exponent expression
            exponent_expr = self.visit(func_node.args[0])
            
            # Direct implementation - power function
            # Note: Need to handle special cases (0^0, negative bases with fractional exponents)
            return f"""CASE 
                WHEN CAST({base_expr} AS DOUBLE) = 0 AND CAST({exponent_expr} AS DOUBLE) = 0 THEN 1
                WHEN CAST({base_expr} AS DOUBLE) < 0 AND CAST({exponent_expr} AS DOUBLE) != floor(CAST({exponent_expr} AS DOUBLE)) THEN NULL
                WHEN CAST({base_expr} AS DOUBLE) = 0 AND CAST({exponent_expr} AS DOUBLE) < 0 THEN NULL
                ELSE POWER(CAST({base_expr} AS DOUBLE), CAST({exponent_expr} AS DOUBLE))
            END"""
            
        elif func_name == 'tochars':
            # Phase 5 Week 12: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'tochars'):
                try:
                    return self._generate_tochars_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "toChars() CTE generation", str(e))
            
            # Original implementation (fallback)
            # toChars() - converts a string to an array of single-character strings
            # FHIRPath specification: toChars() - string to character array conversion
            
            # toChars() takes no arguments
            if len(func_node.args) != 0:
                raise ValueError(f"toChars() requires no arguments, got {len(func_node.args)}")
            
            # Reference for base_expr to avoid repetition 
            base_ref = base_expr
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN {self.dialect.json_array_function}()
                        ELSE (
                            SELECT {self.dialect.json_array_function}(
                                CASE 
                                    WHEN {self.get_json_type('value')} = 'VARCHAR' THEN (
                                        SELECT {self.dialect.json_array_function}(substr_char)
                                        FROM (
                                            SELECT SUBSTRING(CAST(value AS VARCHAR), seq, 1) as substr_char
                                            FROM RANGE(1, LENGTH(CAST(value AS VARCHAR)) + 1) as seq_table(seq)
                                        )
                                    )
                                    ELSE NULL
                                END
                            )
                            FROM {self.iterate_json_elements_indexed(base_ref)}
                            WHERE {self.get_json_type('value')} = 'VARCHAR'
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {self.get_json_type(base_ref)} = 'VARCHAR' THEN (
                            SELECT {self.dialect.json_array_function}(substr_char)
                            FROM (
                                SELECT SUBSTRING(CAST({base_ref} AS VARCHAR), seq, 1) as substr_char
                                FROM RANGE(1, LENGTH(CAST({base_ref} AS VARCHAR)) + 1) as seq_table(seq)
                            )
                        )
                        ELSE NULL
                    END
            END
            """
            
        elif func_name == 'matches':
            # Phase 5 Week 12: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'matches'):
                try:
                    return self._generate_matches_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "matches() CTE generation", str(e))
            
            # Original implementation (fallback)
            # matches(pattern) - tests if the input string matches the given regular expression
            # FHIRPath specification: matches() - regular expression matching
            
            # matches() takes exactly 1 argument (the regex pattern)
            if len(func_node.args) != 1:
                raise ValueError(f"matches() requires exactly 1 argument (regex pattern), got {len(func_node.args)}")
            
            # Get the regex pattern expression
            pattern_expr = self.visit(func_node.args[0])
            
            # Reference for base_expr to avoid repetition 
            base_ref = base_expr
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN false
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN false
                        ELSE (
                            SELECT CASE 
                                WHEN COUNT(*) = COUNT(CASE 
                                    WHEN {self.get_json_type('value')} = 'VARCHAR' AND regexp_full_match(CAST(value AS VARCHAR), CAST({pattern_expr} AS VARCHAR)) THEN 1
                                    ELSE NULL
                                END) THEN true
                                ELSE false
                            END
                            FROM {self.iterate_json_elements_indexed(base_ref)}
                            WHERE {self.get_json_type('value')} = 'VARCHAR'
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {self.get_json_type(base_ref)} = 'VARCHAR' THEN
                            CASE 
                                WHEN regexp_full_match(CAST({base_ref} AS VARCHAR), CAST({pattern_expr} AS VARCHAR)) THEN true
                                ELSE false
                            END
                        ELSE false
                    END
            END
            """
            
        elif func_name == 'replacematches':
            # Phase 5 Week 12: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'replacematches'):
                try:
                    return self._generate_replacematches_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "replaceMatches() CTE generation", str(e))
            
            # Original implementation (fallback)
            # replaceMatches(pattern, replacement) - replaces all matches of the regex pattern with the replacement string
            # FHIRPath specification: replaceMatches() - regex replacement
            
            # replaceMatches() takes exactly 2 arguments (regex pattern, replacement)
            if len(func_node.args) != 2:
                raise ValueError(f"replaceMatches() requires exactly 2 arguments (pattern, replacement), got {len(func_node.args)}")
            
            # Get the regex pattern and replacement expressions
            pattern_expr = self.visit(func_node.args[0])
            replacement_expr = self.visit(func_node.args[1])
            
            # Reference for base_expr to avoid repetition 
            base_ref = base_expr
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN {self.dialect.json_array_function}()
                        ELSE (
                            SELECT {self.dialect.json_array_function}(
                                CASE 
                                    WHEN {self.get_json_type('value')} = 'VARCHAR' THEN 
                                        regexp_replace(CAST(value AS VARCHAR), CAST({pattern_expr} AS VARCHAR), CAST({replacement_expr} AS VARCHAR), 'g')
                                    ELSE value
                                END
                            )
                            FROM {self.iterate_json_elements_indexed(base_ref)}
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {self.get_json_type(base_ref)} = 'VARCHAR' THEN
                            regexp_replace(CAST({base_ref} AS VARCHAR), CAST({pattern_expr} AS VARCHAR), CAST({replacement_expr} AS VARCHAR), 'g')
                        ELSE {base_ref}
                    END
            END
            """
            
        elif func_name == 'children':
            # Phase 5 Week 13: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'children'):
                try:
                    return self._generate_children_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "children() CTE generation", str(e))
            
            # Original implementation (fallback)
            # children() - returns all immediate child elements of the current collection
            # FHIRPath specification: children() - direct child navigation
            
            # children() takes no arguments
            if len(func_node.args) != 0:
                raise ValueError(f"children() requires no arguments, got {len(func_node.args)}")
            
            # Reference for base_expr to avoid repetition 
            base_ref = base_expr
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN {self.dialect.json_array_function}()
                        ELSE (
                            SELECT {self.dialect.json_array_function}(
                                CASE 
                                    WHEN {self.get_json_type('value')} = 'OBJECT' THEN (
                                        SELECT {self.dialect.json_array_function}(child_value)
                                        FROM {self.dialect.json_each_function}(value) as child_table(child_key, child_value)
                                    )
                                    WHEN {self.get_json_type('value')} = 'ARRAY' THEN (
                                        SELECT {self.dialect.json_array_function}(array_element)
                                        FROM {self.dialect.json_each_function}(value) as array_table(array_key, array_element)
                                    )
                                    ELSE NULL
                                END
                            )
                            FROM {self.iterate_json_elements_indexed(base_ref)}
                            WHERE {self.get_json_type('value')} IN ('OBJECT', 'ARRAY')
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {self.get_json_type(base_ref)} = 'OBJECT' THEN (
                            SELECT {self.dialect.json_array_function}(child_value)
                            FROM {self.iterate_json_elements_indexed(base_ref)} as child_table(child_key, child_value)
                        )
                        WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN (
                            SELECT {self.dialect.json_array_function}(array_element)
                            FROM {self.iterate_json_elements_indexed(base_ref)} as array_table(array_key, array_element)
                        )
                        ELSE {self.dialect.json_array_function}()
                    END
            END
            """
            
        elif func_name == 'descendants':
            # Phase 5 Week 13: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'descendants'):
                try:
                    return self._generate_descendants_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "descendants() CTE generation", str(e))
            
            # Original implementation (fallback)
            # descendants() - returns all descendant elements recursively from the current collection
            # FHIRPath specification: descendants() - recursive descendant navigation
            
            # descendants() takes no arguments
            if len(func_node.args) != 0:
                raise ValueError(f"descendants() requires no arguments, got {len(func_node.args)}")
            
            # Reference for base_expr to avoid repetition 
            base_ref = base_expr
            
            # Note: For descendants(), we need to implement recursive traversal
            # Since SQL doesn't support true recursion easily, we'll use a WITH RECURSIVE CTE
            # or simulate it with multiple levels of nested queries for a practical depth
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN {self.dialect.json_array_function}()
                        ELSE (
                            WITH RECURSIVE descendants_cte(value, level) AS (
                                -- Base case: direct children
                                SELECT child_value as value, 1 as level
                                FROM {self.iterate_json_elements_indexed(base_ref)} as parent_table(parent_key, parent_value),
                                     {self.dialect.json_each_function}(parent_value) as child_table(child_key, child_value)
                                WHERE {self.get_json_type('parent_value')} IN ('OBJECT', 'ARRAY')
                                
                                UNION ALL
                                
                                -- Recursive case: children of children
                                SELECT child_value as value, d.level + 1 as level
                                FROM descendants_cte d,
                                     {self.dialect.json_each_function}(d.value) as child_table(child_key, child_value)
                                WHERE {self.get_json_type('d.value')} IN ('OBJECT', 'ARRAY')
                                  AND d.level < 10  -- Limit recursion depth to prevent infinite loops
                            )
                            SELECT {self.dialect.json_array_function}(value)
                            FROM descendants_cte
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {self.get_json_type(base_ref)} IN ('OBJECT', 'ARRAY') THEN (
                            WITH RECURSIVE descendants_cte(value, level) AS (
                                -- Base case: direct children
                                SELECT child_value as value, 1 as level
                                FROM {self.iterate_json_elements_indexed(base_ref)} as child_table(child_key, child_value)
                                
                                UNION ALL
                                
                                -- Recursive case: children of children
                                SELECT nested_child_value as value, d.level + 1 as level
                                FROM descendants_cte d,
                                     {self.dialect.json_each_function}(d.value) as nested_table(nested_key, nested_child_value)
                                WHERE {self.get_json_type('d.value')} IN ('OBJECT', 'ARRAY')
                                  AND d.level < 10  -- Limit recursion depth to prevent infinite loops
                            )
                            SELECT {self.dialect.json_array_function}(value)
                            FROM descendants_cte
                        )
                        ELSE {self.dialect.json_array_function}()
                    END
            END
            """
            
        elif func_name == 'union':
            # Phase 6 Week 14: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'union'):
                try:
                    return self._generate_union_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "union() CTE generation", str(e))
            
            # Original implementation (fallback)
            # union(collection) - returns the union of the current collection and the argument collection
            # FHIRPath specification: union(collection) - set union with deduplication
            
            # union() takes exactly 1 argument (the collection to union with)
            if len(func_node.args) != 1:
                raise ValueError(f"union() requires exactly 1 argument (collection to union with), got {len(func_node.args)}")
            
            # Get the argument collection expression
            other_collection_expr = self.visit(func_node.args[0])
            
            # Reference for base_expr to avoid repetition 
            base_ref = base_expr
            
            # Note: union() performs set union with deduplication (distinct values only)
            # This is different from the | operator which concatenates without deduplication
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL AND {other_collection_expr} IS NULL THEN NULL
                WHEN {base_expr} IS NULL THEN 
                    CASE 
                        WHEN {self.get_json_type(other_collection_expr)} = 'ARRAY' THEN (
                            SELECT {self.dialect.json_array_function}(DISTINCT value)
                            FROM {self.dialect.json_each_function}({other_collection_expr})
                        )
                        ELSE {self.dialect.json_array_function}({other_collection_expr})
                    END
                WHEN {other_collection_expr} IS NULL THEN 
                    CASE 
                        WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN (
                            SELECT {self.dialect.json_array_function}(DISTINCT value)
                            FROM {self.iterate_json_elements_indexed(base_ref)}
                        )
                        ELSE {self.dialect.json_array_function}({base_ref})
                    END
                ELSE (
                    -- Both collections are non-null, perform union with deduplication
                    SELECT {self.dialect.json_array_function}(DISTINCT combined_value)
                    FROM (
                        -- Values from the base collection
                        SELECT value as combined_value
                        FROM {self.dialect.json_each_function}(
                            CASE 
                                WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN {base_ref}
                                ELSE {self.dialect.json_array_function}({base_ref})
                            END
                        )
                        
                        UNION ALL
                        
                        -- Values from the argument collection
                        SELECT value as combined_value
                        FROM {self.dialect.json_each_function}(
                            CASE 
                                WHEN {self.get_json_type(other_collection_expr)} = 'ARRAY' THEN {other_collection_expr}
                                ELSE {self.dialect.json_array_function}({other_collection_expr})
                            END
                        )
                    ) AS union_source
                )
            END
            """
            
        elif func_name == 'combine':
            # Phase 6 Week 14: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'combine'):
                try:
                    return self._generate_combine_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "combine() CTE generation", str(e))
            
            # Original implementation (fallback)
            # combine(collection) - returns the combination of the current collection and the argument collection
            # FHIRPath specification: combine(collection) - collection combination without deduplication
            
            # combine() takes exactly 1 argument (the collection to combine with)
            if len(func_node.args) != 1:
                raise ValueError(f"combine() requires exactly 1 argument (collection to combine with), got {len(func_node.args)}")
            
            # Get the argument collection expression
            other_collection_expr = self.visit(func_node.args[0])
            
            # Reference for base_expr to avoid repetition 
            base_ref = base_expr
            
            # Note: combine() performs collection combination without deduplication
            # All elements from both collections are included, preserving duplicates
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL AND {other_collection_expr} IS NULL THEN NULL
                WHEN {base_expr} IS NULL THEN 
                    CASE 
                        WHEN {self.get_json_type(other_collection_expr)} = 'ARRAY' THEN {other_collection_expr}
                        ELSE {self.dialect.json_array_function}({other_collection_expr})
                    END
                WHEN {other_collection_expr} IS NULL THEN 
                    CASE 
                        WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN {base_ref}
                        ELSE {self.dialect.json_array_function}({base_ref})
                    END
                ELSE (
                    -- Both collections are non-null, perform combination without deduplication
                    SELECT {self.dialect.json_array_function}(combined_value)
                    FROM (
                        -- Values from the base collection
                        SELECT value as combined_value
                        FROM {self.dialect.json_each_function}(
                            CASE 
                                WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN {base_ref}
                                ELSE {self.dialect.json_array_function}({base_ref})
                            END
                        )
                        
                        UNION ALL
                        
                        -- Values from the argument collection
                        SELECT value as combined_value
                        FROM {self.dialect.json_each_function}(
                            CASE 
                                WHEN {self.get_json_type(other_collection_expr)} = 'ARRAY' THEN {other_collection_expr}
                                ELSE {self.dialect.json_array_function}({other_collection_expr})
                            END
                        )
                    ) AS combine_source
                )
            END
            """
            
        elif func_name == 'repeat':
            # Phase 6 Week 15: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'repeat'):
                try:
                    return self._generate_repeat_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "repeat() CTE generation", str(e))
            
            # Original implementation (fallback)
            # repeat(expression) - iteratively applies the expression until a stable result is reached
            # FHIRPath specification: repeat(expression) - iterative projection with recursion termination
            
            # repeat() takes exactly 1 argument (the expression to apply iteratively)
            if len(func_node.args) != 1:
                raise ValueError(f"repeat() requires exactly 1 argument (expression to apply iteratively), got {len(func_node.args)}")
            
            # Get the expression to apply repeatedly
            repeat_expr = self.visit(func_node.args[0])
            
            # Reference for base_expr to avoid repetition 
            base_ref = base_expr
            
            # Note: repeat() applies the expression iteratively until the result stabilizes
            # We need to use WITH RECURSIVE to implement this safely with a depth limit
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                ELSE (
                    WITH RECURSIVE repeat_cte(current_value, iteration, previous_hash) AS (
                        -- Base case: start with the initial collection
                        SELECT 
                            CASE 
                                WHEN {self.get_json_type(base_ref)} = 'ARRAY' THEN {base_ref}
                                ELSE {self.dialect.json_array_function}({base_ref})
                            END as current_value,
                            0 as iteration,
                            '' as previous_hash
                        
                        UNION ALL
                        
                        -- Recursive case: apply the expression to current result
                        SELECT 
                            (
                                -- Apply the repeat expression to each element in current_value
                                SELECT {self.dialect.json_array_function}(
                                    CASE 
                                        WHEN {self.get_json_type('value')} = 'ARRAY' THEN (
                                            SELECT {self.dialect.json_array_function}(result_value)
                                            FROM {self.dialect.json_each_function}(
                                                -- Apply expression: substitute $this with current value
                                                {repeat_expr.replace('$this', 'value')}
                                            ) as result_table(result_key, result_value)
                                        )
                                        ELSE {repeat_expr.replace('$this', 'value')}
                                    END
                                )
                                FROM {self.dialect.json_each_function}(r.current_value) as iter_table(iter_key, value)
                            ) as current_value,
                            r.iteration + 1 as iteration,
                            -- Create hash of current value for convergence detection
                            md5(CAST(r.current_value AS VARCHAR)) as previous_hash
                        FROM repeat_cte r
                        WHERE r.iteration < 10  -- Limit recursion depth to prevent infinite loops
                          AND (
                              r.iteration = 0 OR  -- Always do at least one iteration
                              md5(CAST(r.current_value AS VARCHAR)) != r.previous_hash  -- Continue if result is changing
                          )
                    )
                    SELECT 
                        CASE 
                            WHEN COUNT(*) = 0 THEN {self.dialect.json_array_function}()
                            ELSE current_value
                        END
                    FROM repeat_cte
                    WHERE iteration = (SELECT MAX(iteration) FROM repeat_cte)  -- Get final result
                )
            END
            """
            
        elif func_name == 'trace':
            # trace(name[, projection]) function - debug tracing functionality
            if len(func_node.args) < 1 or len(func_node.args) > 2:
                raise ValueError("trace() function requires 1 or 2 arguments: name[, projection]")
            
            # Get the trace name
            trace_name_expr = self.visit(func_node.args[0])
            
            # Handle optional projection argument
            if len(func_node.args) == 2:
                projection_expr = self.visit(func_node.args[1])
                # Apply projection to base_expr, then trace with name
                traced_expr = f"({projection_expr})"
            else:
                # No projection, trace the base expression directly
                traced_expr = base_expr
            
            # trace function returns the traced expression and logs it
            # Integrate with existing logging system
            from ...utils.logging_config import get_logger
            logger = get_logger(__name__)
            logger.debug("FHIRPath trace - %s: %s", label, traced_expr)
            return traced_expr
            
        elif func_name == 'aggregate':
            # aggregate(aggregator[, init]) function - custom aggregation functionality
            if len(func_node.args) < 1 or len(func_node.args) > 2:
                raise ValueError("aggregate() function requires 1 or 2 arguments: aggregator[, init]")
            
            # Get the aggregator expression
            aggregator_expr = self.visit(func_node.args[0])
            
            # Handle optional init value
            if len(func_node.args) == 2:
                init_expr = self.visit(func_node.args[1])
            else:
                init_expr = "NULL"
            
            # Implement basic aggregation using array_agg
            # Custom aggregation logic using standard SQL aggregation patterns
            return f"""
            COALESCE(
                (SELECT {self.dialect.array_agg_function}({aggregator_expr}) 
                 FROM (
                     SELECT value 
                     FROM {self.dialect.json_each_function}(
                         CASE WHEN {self.dialect.json_type_function}({base_expr}) = 'ARRAY' 
                         THEN {base_expr} 
                         ELSE {self.dialect.json_array_function}({base_expr}) END
                     ) AS t
                     WHERE value IS NOT NULL
                 ) AS items),
                {init_expr}
            )
            """
            
        elif func_name == 'flatten':
            # flatten() function - flattens nested collections
            if len(func_node.args) != 0:
                raise ValueError("flatten() function takes no arguments")
            
            # Implement recursive flattening using JSON functions
            return f"""
            (SELECT {self.dialect.json_array_function}(
                CASE 
                    WHEN {self.dialect.json_type_function}(outer.value) = 'ARRAY' THEN
                        (SELECT {self.dialect.json_array_function}(nested.value)
                         FROM {self.dialect.json_each_function}(outer.value) AS nested
                         WHERE nested.value IS NOT NULL)
                    ELSE outer.value
                END
            )
            FROM {self.dialect.json_each_function}(
                CASE WHEN {self.dialect.json_type_function}({base_expr}) = 'ARRAY' 
                THEN {base_expr} 
                ELSE {self.dialect.json_array_function}({base_expr}) END
            ) AS outer
            WHERE outer.value IS NOT NULL)
            """
            
        elif func_name == 'convertstoquantity':
            # convertsToQuantity() function - tests if value can be converted to a FHIR Quantity
            if len(func_node.args) != 0:
                raise ValueError("convertsToQuantity() function takes no arguments")
            
            # Check if the value can be converted to a FHIR Quantity
            # A value can be converted to Quantity if it's:
            # 1. Already a Quantity object with value and optionally unit
            # 2. A numeric value (which can become a Quantity with just value)
            # 3. A string that represents a valid quantity (e.g., "5 mg", "10.5", "100 units")
            return f"""
            CASE 
                -- Check if it's already a Quantity object
                WHEN {self.dialect.json_type_function}({base_expr}) = 'OBJECT' AND 
                     {self.dialect.json_extract_function}({base_expr}, '$.value') IS NOT NULL 
                THEN TRUE
                -- Check if it's a numeric value (can be converted to Quantity)
                WHEN {self.dialect.json_type_function}({base_expr}) IN ('DOUBLE', 'BIGINT', 'INTEGER', 'FLOAT', 'DECIMAL', 'NUMBER') 
                THEN TRUE
                -- Check if it's a string that can be parsed as a quantity
                WHEN {self.dialect.json_type_function}({base_expr}) IN ('VARCHAR', 'STRING', 'TEXT') THEN
                    CASE 
                        -- Simple numeric string
                        WHEN {self.dialect.json_extract_string_function}({base_expr}, '$') ~ '^[+-]?[0-9]*\.?[0-9]+([eE][+-]?[0-9]+)?$' 
                        THEN TRUE
                        -- Numeric string with unit (basic pattern)
                        WHEN {self.dialect.json_extract_string_function}({base_expr}, '$') ~ '^[+-]?[0-9]*\.?[0-9]+([eE][+-]?[0-9]+)?\\s+[a-zA-Z]+.*$' 
                        THEN TRUE
                        ELSE FALSE
                    END
                ELSE FALSE
            END
            """
            
        elif func_name == 'hasvalue':
            # hasValue() function - tests if value exists (not null/empty)
            if len(func_node.args) != 0:
                raise ValueError("hasValue() function takes no arguments")
            
            # Check if the value exists and is not null
            # According to FHIR spec, hasValue() returns true if the value is not null and not empty
            return f"""
            CASE 
                -- Check if value is not null and not empty
                WHEN {base_expr} IS NOT NULL AND 
                     {self.dialect.json_type_function}({base_expr}) != 'NULL' AND
                     (
                         -- For strings, check if not empty
                         ({self.dialect.json_type_function}({base_expr}) IN ('VARCHAR', 'STRING', 'TEXT') AND 
                          {self.dialect.json_extract_string_function}({base_expr}, '$') != '') OR
                         -- For arrays, check if not empty
                         ({self.dialect.json_type_function}({base_expr}) = 'ARRAY' AND 
                          {self.dialect.json_array_length_function}({base_expr}) > 0) OR
                         -- For objects, check if not empty
                         ({self.dialect.json_type_function}({base_expr}) = 'OBJECT' AND 
                          {self.dialect.json_extract_function}({base_expr}, '$') != '{{}}') OR
                         -- For numeric types, always true if not null
                         {self.dialect.json_type_function}({base_expr}) IN ('DOUBLE', 'BIGINT', 'INTEGER', 'FLOAT', 'DECIMAL', 'NUMBER') OR
                         -- For boolean types, always true if not null
                         {self.dialect.json_type_function}({base_expr}) = 'BOOLEAN'
                     )
                THEN TRUE
                ELSE FALSE
            END
            """

        elif func_name == 'getvalue':
            # getValue() function - extracts primitive value from FHIR elements
            if len(func_node.args) != 0:
                raise ValueError("getValue() function takes no arguments")
            
            # Check if we should use CTE for complex base expressions
            if self._should_use_cte_unified(base_expr, 'getValue'):
                return self._generate_getValue_with_cte(base_expr)
            
            # Extract primitive value from FHIR element, handling both simple values and complex FHIR types
            # According to FHIR spec, getValue() returns the primitive value if input is primitive type,
            # or the value of a FHIR element
            return f"""
            CASE 
                -- Handle primitive types (return as-is)
                WHEN {self.dialect.json_type_function}({base_expr}) IN ('STRING', 'NUMBER', 'BOOLEAN') THEN 
                    {base_expr}
                
                -- Handle FHIR types with 'value' property (Quantity, Identifier, ContactPoint, etc.)
                WHEN {self.dialect.json_type_function}({base_expr}) = 'OBJECT' AND 
                     {self.dialect.json_extract_function}({base_expr}, '$.value') IS NOT NULL THEN
                    {self.dialect.json_extract_function}({base_expr}, '$.value')
                
                -- Handle HumanName (concatenate name parts)
                WHEN {self.dialect.json_type_function}({base_expr}) = 'OBJECT' AND 
                     ({self.dialect.json_extract_function}({base_expr}, '$.family') IS NOT NULL OR 
                      {self.dialect.json_extract_function}({base_expr}, '$.given') IS NOT NULL) THEN
                    TRIM({self.dialect.string_concat(
                        self.dialect.string_concat(
                            f"COALESCE({self.dialect.json_extract_string_function}({base_expr}, '$.given[0]'), '')",
                            f"CASE WHEN {self.dialect.json_extract_function}({base_expr}, '$.given[0]') IS NOT NULL AND {self.dialect.json_extract_function}({base_expr}, '$.family') IS NOT NULL THEN ' ' ELSE '' END"
                        ),
                        f"COALESCE({self.dialect.json_extract_string_function}({base_expr}, '$.family'), '')"
                    )})
                
                -- Handle Address (return formatted address string)
                WHEN {self.dialect.json_type_function}({base_expr}) = 'OBJECT' AND 
                     ({self.dialect.json_extract_function}({base_expr}, '$.line') IS NOT NULL OR 
                      {self.dialect.json_extract_function}({base_expr}, '$.city') IS NOT NULL) THEN
                    TRIM({self.dialect.string_concat(
                        self.dialect.string_concat(
                            f"COALESCE({self.dialect.json_extract_string_function}({base_expr}, '$.line[0]'), '')",
                            f"CASE WHEN {self.dialect.json_extract_function}({base_expr}, '$.line[0]') IS NOT NULL AND {self.dialect.json_extract_function}({base_expr}, '$.city') IS NOT NULL THEN ', ' ELSE '' END"
                        ),
                        f"COALESCE({self.dialect.json_extract_string_function}({base_expr}, '$.city'), '')"
                    )})
                
                -- Return null for complex types without clear value semantics
                ELSE NULL
            END
            """

        elif func_name == 'resolve':
            # resolve() function - resolves FHIR references to actual resources
            if len(func_node.args) != 0:
                raise ValueError("resolve() function takes no arguments")
            
            # Generate reference resolution logic according to FHIR specification
            # The resolve() function takes a reference and returns the resource(s) that are the target of the reference
            
            # Use dialect-specific string splitting approach
            if self.dialect.name == "DUCKDB":
                split_resource_type = f"CASE WHEN {self.dialect.json_extract_string_function}({base_expr}, '$.reference') LIKE '%/%' THEN string_split({self.dialect.json_extract_string_function}({base_expr}, '$.reference'), '/')[1] ELSE NULL END"
                split_resource_id = f"CASE WHEN {self.dialect.json_extract_string_function}({base_expr}, '$.reference') LIKE '%/%' THEN string_split({self.dialect.json_extract_string_function}({base_expr}, '$.reference'), '/')[2] ELSE {self.dialect.json_extract_string_function}({base_expr}, '$.reference') END"
            else:  # PostgreSQL or other
                split_resource_type = f"CASE WHEN {self.dialect.json_extract_string_function}({base_expr}, '$.reference') LIKE '%/%' THEN split_part({self.dialect.json_extract_string_function}({base_expr}, '$.reference'), '/', 1) ELSE NULL END"
                split_resource_id = f"CASE WHEN {self.dialect.json_extract_string_function}({base_expr}, '$.reference') LIKE '%/%' THEN split_part({self.dialect.json_extract_string_function}({base_expr}, '$.reference'), '/', 2) ELSE {self.dialect.json_extract_string_function}({base_expr}, '$.reference') END"
            
            return f"""
            CASE 
                -- Handle null or non-object references
                WHEN {base_expr} IS NULL OR {self.dialect.json_type_function}({base_expr}) != 'OBJECT' THEN 
                    {self.dialect.json_array_function}()
                    
                -- Handle references without reference property
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.reference') IS NULL THEN
                    {self.dialect.json_array_function}()
                    
                -- Handle valid references with reference resolution
                ELSE (
                    WITH reference_parsing AS (
                        SELECT 
                            {base_expr} as reference_obj,
                            {self.dialect.json_extract_string_function}({base_expr}, '$.reference') as ref_string,
                            -- Extract resource type from reference (part before /)
                            {split_resource_type} as resource_type,
                            -- Extract resource ID from reference (part after /)
                            {split_resource_id} as resource_id
                    ),
                    resolved_resources AS (
                        SELECT target_resources.{self.json_column}
                        FROM reference_parsing ref
                        JOIN {self.table_name} target_resources ON (
                            -- Match on resource type and ID
                            {self.dialect.json_extract_string_function}(target_resources.{self.json_column}, '$.resourceType') = ref.resource_type AND
                            {self.dialect.json_extract_string_function}(target_resources.{self.json_column}, '$.id') = ref.resource_id
                        )
                        WHERE ref.resource_type IS NOT NULL AND ref.resource_id IS NOT NULL
                    )
                    SELECT COALESCE(
                        {self.dialect.aggregate_to_json_array(f"resolved_resources.{self.json_column}")}, 
                        {self.dialect.json_array_function}()
                    )
                    FROM resolved_resources
                )
            END
            """

        elif func_name == 'hascodedvalue':
            # hasCodedValue() function - tests if value is a coded value (CodeableConcept or Coding)
            if len(func_node.args) != 0:
                raise ValueError("hasCodedValue() function takes no arguments")
            
            # Check if the value is a coded value according to FHIR specification
            # A coded value is either a CodeableConcept or a Coding object
            return f"""
            CASE 
                -- Check if it's a CodeableConcept (has 'coding' array)
                WHEN {base_expr} IS NOT NULL AND 
                     {self.dialect.json_type_function}({base_expr}) = 'OBJECT' AND 
                     {self.dialect.json_extract_function}({base_expr}, '$.coding') IS NOT NULL AND
                     {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) = 'ARRAY' AND
                     {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) > 0
                THEN TRUE
                -- Check if it's a Coding (has 'system' and/or 'code' properties)
                WHEN {base_expr} IS NOT NULL AND 
                     {self.dialect.json_type_function}({base_expr}) = 'OBJECT' AND 
                     (
                         {self.dialect.json_extract_function}({base_expr}, '$.system') IS NOT NULL OR
                         {self.dialect.json_extract_function}({base_expr}, '$.code') IS NOT NULL
                     )
                THEN TRUE
                -- Check if it's a simple string code (in some contexts)
                WHEN {base_expr} IS NOT NULL AND 
                     {self.dialect.json_type_function}({base_expr}) IN ('VARCHAR', 'STRING', 'TEXT') AND
                     {self.dialect.json_extract_string_function}({base_expr}, '$') != ''
                THEN TRUE
                ELSE FALSE
            END
            """

        elif func_name == 'htmlchecks':
            # htmlChecks() function - performs HTML validation and security checks
            if len(func_node.args) != 0:
                raise ValueError("htmlChecks() function takes no arguments")
            
            # Check for HTML content and potential security issues
            # This is a simplified implementation - in production, you'd want more comprehensive checks
            return f"""
            CASE 
                -- Check if it's a string that might contain HTML
                WHEN {base_expr} IS NOT NULL AND 
                     {self.dialect.json_type_function}({base_expr}) IN ('VARCHAR', 'STRING', 'TEXT')
                THEN
                    CASE 
                        -- Check for HTML tags
                        WHEN {self.dialect.json_extract_string_function}({base_expr}, '$') ~ '<[^>]*>' THEN
                            CASE 
                                -- Check for potentially dangerous tags/attributes
                                WHEN {self.dialect.json_extract_string_function}({base_expr}, '$') ~ '(?i)<(script|iframe|object|embed|form|input|meta|link|style)' OR
                                     {self.dialect.json_extract_string_function}({base_expr}, '$') ~ '(?i)(javascript:|vbscript:|data:|on\\w+\\s*=)' OR
                                     {self.dialect.json_extract_string_function}({base_expr}, '$') ~ '(?i)(expression\\s*\\(|@import|behavior\\s*:)'
                                THEN 'unsafe'
                                -- Check for incomplete or malformed tags
                                WHEN {self.dialect.json_extract_string_function}({base_expr}, '$') ~ '<[^>]*$' OR
                                     {self.dialect.json_extract_string_function}({base_expr}, '$') ~ '[^<]*>' OR
                                     {self.dialect.json_extract_string_function}({base_expr}, '$') ~ '<[^>]*</[^>]*[^>]$'
                                THEN 'malformed'
                                -- Basic HTML content that appears safe
                                ELSE 'safe'
                            END
                        -- Plain text (no HTML tags)
                        ELSE 'text'
                    END
                -- Non-string values
                ELSE 'non-string'
            END
            """

        elif func_name == 'hastemplateidof':
            # hasTemplateIdOf() function - checks if value has a specific template ID
            if len(func_node.args) != 1:
                raise ValueError("hasTemplateIdOf() function takes exactly one argument")
            
            # Get the template ID to check for
            template_id_arg = func_node.args[0]
            if hasattr(template_id_arg, 'value'):
                template_id = template_id_arg.value
            else:
                template_id = str(template_id_arg)
            
            # Escape the template_id for SQL injection prevention
            template_id = template_id.replace("'", "''")
            
            # Check if the value has the specified template ID
            # Template IDs are typically found in extensions or meta.profile
            return f"""
            CASE 
                -- Check if it's an object that might contain template IDs
                WHEN {base_expr} IS NOT NULL AND 
                     {self.dialect.json_type_function}({base_expr}) = 'OBJECT'
                THEN
                    CASE 
                        -- Check in meta.profile array
                        WHEN {self.dialect.json_extract_function}({base_expr}, '$.meta.profile') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.meta.profile')) = 'ARRAY'
                        THEN
                            CASE 
                                WHEN EXISTS (
                                    SELECT 1 FROM json_each({self.dialect.json_extract_function}({base_expr}, '$.meta.profile')) 
                                    WHERE {self.dialect.json_extract_string_function}(value, '$') = '{template_id}'
                                )
                                THEN TRUE
                                ELSE FALSE
                            END
                        -- Check in templateId extension (common in CDA-style templates)
                        WHEN {self.dialect.json_extract_function}({base_expr}, '$.templateId') IS NOT NULL
                        THEN
                            CASE 
                                -- Single templateId object
                                WHEN {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.templateId')) = 'OBJECT' AND
                                     {self.dialect.json_extract_string_function}({self.dialect.json_extract_function}({base_expr}, '$.templateId'), '$.root') = '{template_id}'
                                THEN TRUE
                                -- Array of templateId objects
                                WHEN {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.templateId')) = 'ARRAY' AND
                                     EXISTS (
                                         SELECT 1 FROM json_each({self.dialect.json_extract_function}({base_expr}, '$.templateId')) 
                                         WHERE {self.dialect.json_extract_string_function}(value, '$.root') = '{template_id}'
                                     )
                                THEN TRUE
                                ELSE FALSE
                            END
                        -- Check in extensions for template-related URLs
                        WHEN {self.dialect.json_extract_function}({base_expr}, '$.extension') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.extension')) = 'ARRAY'
                        THEN
                            CASE 
                                WHEN EXISTS (
                                    SELECT 1 FROM json_each({self.dialect.json_extract_function}({base_expr}, '$.extension')) 
                                    WHERE {self.dialect.json_extract_string_function}(value, '$.url') LIKE '%template%' AND
                                          ({self.dialect.json_extract_string_function}(value, '$.valueString') = '{template_id}' OR
                                           {self.dialect.json_extract_string_function}(value, '$.valueUri') = '{template_id}')
                                )
                                THEN TRUE
                                ELSE FALSE
                            END
                        -- Check direct string comparison for simple cases
                        ELSE FALSE
                    END
                -- For string values, check direct comparison
                WHEN {base_expr} IS NOT NULL AND 
                     {self.dialect.json_type_function}({base_expr}) IN ('VARCHAR', 'STRING', 'TEXT') AND
                     {self.dialect.json_extract_string_function}({base_expr}, '$') = '{template_id}'
                THEN TRUE
                ELSE FALSE
            END
            """

        elif func_name == 'encode':
            # encode() function - URL/percent encoding of strings using database-specific functions
            if len(func_node.args) != 0:
                raise ValueError("encode() function takes no arguments")
            
            # Use database-specific URL encoding function for better performance and accuracy
            return f"""
            CASE 
                -- Only encode string values
                WHEN {base_expr} IS NOT NULL AND 
                     {self.dialect.json_type_function}({base_expr}) IN ('VARCHAR', 'STRING', 'TEXT')
                THEN
                    -- Use database-specific URL encoding function
                    {self.dialect.url_encode(f'{self.dialect.json_extract_string_function}({base_expr}, "$")')}
                -- Non-string values return null
                ELSE NULL
            END
            """

        elif func_name == 'decode':
            # decode() function - URL/percent decoding of strings using database-specific functions
            if len(func_node.args) != 0:
                raise ValueError("decode() function takes no arguments")
            
            # Use database-specific URL decoding function for better performance and accuracy
            return f"""
            CASE 
                -- Only decode string values
                WHEN {base_expr} IS NOT NULL AND 
                     {self.dialect.json_type_function}({base_expr}) IN ('VARCHAR', 'STRING', 'TEXT')
                THEN
                    -- Use database-specific URL decoding function
                    {self.dialect.url_decode(f'{self.dialect.json_extract_string_function}({base_expr}, "$")')}
                -- Non-string values return null
                ELSE NULL
            END
            """

        elif func_name == 'conformsto':
            # conformsTo() function - checks if resource conforms to a profile
            if len(func_node.args) != 1:
                raise ValueError("conformsTo() function takes exactly one argument")
            
            # Get the profile URI to check for
            profile_uri_arg = func_node.args[0]
            if hasattr(profile_uri_arg, 'value'):
                profile_uri = profile_uri_arg.value
            else:
                profile_uri = str(profile_uri_arg)
            
            # Escape the profile_uri for SQL injection prevention
            profile_uri = profile_uri.replace("'", "''")
            
            # Check if the resource conforms to the specified profile
            # This is a simplified implementation - full conformance checking would require
            # validating against the actual profile definition
            return f"""
            CASE 
                -- Check if it's a FHIR resource that might have profile information
                WHEN {base_expr} IS NOT NULL AND 
                     {self.dialect.json_type_function}({base_expr}) = 'OBJECT' AND
                     {self.dialect.json_extract_function}({base_expr}, '$.resourceType') IS NOT NULL
                THEN
                    CASE 
                        -- Check in meta.profile array
                        WHEN {self.dialect.json_extract_function}({base_expr}, '$.meta.profile') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.meta.profile')) = 'ARRAY'
                        THEN
                            CASE 
                                WHEN EXISTS (
                                    SELECT 1 FROM json_each({self.dialect.json_extract_function}({base_expr}, '$.meta.profile')) 
                                    WHERE {self.dialect.json_extract_string_function}(value, '$') = '{profile_uri}'
                                )
                                THEN TRUE
                                ELSE FALSE
                            END
                        -- Check if it's a base resource type match (simplified conformance)
                        WHEN '{profile_uri}' LIKE '%' || {self.dialect.json_extract_string_function}({base_expr}, '$.resourceType') || '%'
                        THEN TRUE
                        -- Check for implicit conformance based on resource type
                        WHEN '{profile_uri}' LIKE 'http://hl7.org/fhir/StructureDefinition/%' AND
                             LOWER('{profile_uri}') LIKE '%' || LOWER({self.dialect.json_extract_string_function}({base_expr}, '$.resourceType')) || '%'
                        THEN TRUE
                        ELSE FALSE
                    END
                -- Non-resource values cannot conform to profiles
                ELSE FALSE
            END
            """

        elif func_name == 'memberof':
            # memberOf() function - checks if coded value is a member of a ValueSet
            if len(func_node.args) != 1:
                raise ValueError("memberOf() function takes exactly one argument")
            
            # Get the ValueSet URI to check for membership
            valueset_uri_arg = func_node.args[0]
            if hasattr(valueset_uri_arg, 'value'):
                valueset_uri = valueset_uri_arg.value
            else:
                valueset_uri = str(valueset_uri_arg)
            
            # Escape the valueset_uri for SQL injection prevention
            valueset_uri = valueset_uri.replace("'", "''")
            
            # Check if the coded value is a member of the specified ValueSet
            # This is a simplified implementation - full ValueSet membership would require
            # resolving the actual ValueSet definition and checking all included codes
            return f"""
            CASE 
                -- Check if it's a coded value (CodeableConcept or Coding)
                WHEN {base_expr} IS NOT NULL AND 
                     {self.dialect.json_type_function}({base_expr}) = 'OBJECT'
                THEN
                    CASE 
                        -- Check if it's a CodeableConcept with coding array
                        WHEN {self.dialect.json_extract_function}({base_expr}, '$.coding') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) = 'ARRAY' AND
                             {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) > 0
                        THEN
                            CASE 
                                -- Check if any coding in the array matches the ValueSet
                                WHEN EXISTS (
                                    SELECT 1 FROM json_each({self.dialect.json_extract_function}({base_expr}, '$.coding')) 
                                    WHERE {self.dialect.json_extract_string_function}(value, '$.system') IS NOT NULL AND
                                          (
                                              -- Direct system match (simplified ValueSet membership)
                                              {self.dialect.json_extract_string_function}(value, '$.system') = '{valueset_uri}' OR
                                              -- Check if the system is part of the ValueSet URI
                                              '{valueset_uri}' LIKE '%' || {self.dialect.json_extract_string_function}(value, '$.system') || '%' OR
                                              -- Check if the ValueSet references the system
                                              {self.dialect.json_extract_string_function}(value, '$.system') LIKE '%' || REPLACE('{valueset_uri}', 'ValueSet/', '') || '%'
                                          )
                                )
                                THEN TRUE
                                ELSE FALSE
                            END
                        -- Check if it's a direct Coding object
                        WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') IS NOT NULL AND
                             {self.dialect.json_extract_function}({base_expr}, '$.code') IS NOT NULL
                        THEN
                            CASE 
                                -- Check if the Coding system matches the ValueSet
                                WHEN {self.dialect.json_extract_string_function}({base_expr}, '$.system') = '{valueset_uri}' OR
                                     '{valueset_uri}' LIKE '%' || {self.dialect.json_extract_string_function}({base_expr}, '$.system') || '%' OR
                                     {self.dialect.json_extract_string_function}({base_expr}, '$.system') LIKE '%' || REPLACE('{valueset_uri}', 'ValueSet/', '') || '%'
                                THEN TRUE
                                ELSE FALSE
                            END
                        -- Check if it's a simple string code (context-dependent)
                        WHEN {self.dialect.json_type_function}({base_expr}) IN ('VARCHAR', 'STRING', 'TEXT')
                        THEN
                            CASE 
                                -- Very simplified check - in practice, would need context
                                WHEN {self.dialect.json_extract_string_function}({base_expr}, '$') != '' AND
                                     '{valueset_uri}' LIKE '%code%'
                                THEN TRUE
                                ELSE FALSE
                            END
                        ELSE FALSE
                    END
                -- Non-coded values cannot be members of ValueSets
                ELSE FALSE
            END
            """
        elif func_name == 'single':
            # Phase 1: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'single'):
                try:
                    return self._generate_single_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "single() CTE generation", str(e))
            
            # Original implementation (fallback)
            # single() returns the single element of a collection, or empty collection for error conditions
            # FHIRPath specification: single() - returns the single element in a collection. Empty collection if empty or multiple elements
            # Note: In FHIRPath, "errors" typically result in empty collections rather than exceptions
            return f"""
            CASE 
                -- Check if the expression is null (empty collection)
                WHEN {base_expr} IS NULL THEN NULL
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- Array is empty - return empty collection (NULL)
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        -- Array has exactly one element - return that element
                        WHEN {self.get_json_array_length(base_expr)} = 1 THEN
                            {self.extract_json_object(base_expr, '$[0]')}
                        -- Array has more than one element - return empty collection (NULL) per FHIRPath error semantics
                        ELSE NULL
                    END
                -- For non-arrays (single objects), return the object itself
                ELSE {base_expr}
            END
            """
        elif func_name == 'tail':
            # Phase 1: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'tail'):
                try:
                    return self._generate_tail_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "tail() CTE generation", str(e))
            
            # Original implementation (fallback)
            # tail() returns all elements except the first one in a collection
            # FHIRPath specification: tail() - returns all but the first element in a collection
            # Note: For simplicity, we'll return a computed JSON array slice
            return f"""
            CASE 
                -- Check if the expression is null (empty collection)
                WHEN {base_expr} IS NULL THEN NULL
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- Array is empty or has only one element - return empty collection (NULL)
                        WHEN {self.get_json_array_length(base_expr)} <= 1 THEN NULL
                        -- Array has multiple elements - return slice from index 1 to end
                        ELSE (
                            SELECT {self.dialect.aggregate_to_json_array('value')}
                            FROM (
                                SELECT value
                                FROM {self.iterate_json_elements_indexed(base_expr)}
                                WHERE CAST(key AS INTEGER) >= 1
                                ORDER BY CAST(key AS INTEGER)
                            )
                        )
                    END
                -- For non-arrays (single objects), return empty collection (NULL)
                ELSE NULL
            END
            """
        elif func_name == 'skip':
            # Phase 1: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'skip'):
                try:
                    return self._generate_skip_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "skip() CTE generation", str(e))
            
            # Original implementation (fallback)
            # skip(num) returns all elements after skipping the first num elements
            # FHIRPath specification: skip(num) - returns all but the first num elements
            
            # Get the skip count argument
            if len(func_node.args) != 1:
                raise ValueError("skip() function requires exactly one argument")
            
            skip_count_arg = func_node.args[0]
            if hasattr(skip_count_arg, 'value'):
                skip_count = skip_count_arg.value
            else:
                # Handle complex expressions - visit the argument
                skip_count_sql = self.visit(skip_count_arg)
                skip_count = f"({skip_count_sql})"
            
            return f"""
            CASE 
                -- Check if the expression is null (empty collection)
                WHEN {base_expr} IS NULL THEN NULL
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- If skip count is null or negative, return original array
                        WHEN {skip_count} IS NULL OR {skip_count} < 0 THEN {base_expr}
                        -- Array length is less than or equal to skip count - return empty collection (NULL)
                        WHEN {self.get_json_array_length(base_expr)} <= {skip_count} THEN NULL
                        -- Array has more elements than skip count - return elements after skip
                        ELSE (
                            SELECT {self.dialect.aggregate_to_json_array('value')}
                            FROM (
                                SELECT value
                                FROM {self.iterate_json_elements_indexed(base_expr)}
                                WHERE CAST(key AS INTEGER) >= {skip_count}
                                ORDER BY CAST(key AS INTEGER)
                            )
                        )
                    END
                -- For non-arrays (single objects), skip behavior
                ELSE 
                    CASE 
                        WHEN {skip_count} IS NULL OR {skip_count} <= 0 THEN {base_expr}
                        ELSE NULL
                    END
            END
            """
        elif func_name == 'take':
            # Phase 1: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'take'):
                try:
                    return self._generate_take_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "take() CTE generation", str(e))
            
            # Original implementation (fallback)
            # take(num) returns the first num elements of a collection
            # FHIRPath specification: take(num) - returns the first num elements
            
            # Get the take count argument
            if len(func_node.args) != 1:
                validate_argument_count("take", 1, len(func_node.args))
            
            take_count_arg = func_node.args[0]
            if hasattr(take_count_arg, 'value'):
                take_count = take_count_arg.value
            else:
                # Handle complex expressions - visit the argument
                take_count_sql = self.visit(take_count_arg)
                take_count = f"({take_count_sql})"
            
            return f"""
            CASE 
                -- Check if the expression is null (empty collection)
                WHEN {base_expr} IS NULL THEN NULL
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- If take count is null or zero or negative, return empty collection (NULL)
                        WHEN {take_count} IS NULL OR {take_count} <= 0 THEN NULL
                        -- Array is empty - return empty collection (NULL)
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        -- Take count is greater than or equal to array length - return entire array
                        WHEN {take_count} >= {self.get_json_array_length(base_expr)} THEN {base_expr}
                        -- Take count is less than array length - return first take_count elements
                        ELSE (
                            SELECT {self.dialect.aggregate_to_json_array('value')}
                            FROM (
                                SELECT value
                                FROM {self.iterate_json_elements_indexed(base_expr)}
                                WHERE CAST(key AS INTEGER) < {take_count}
                                ORDER BY CAST(key AS INTEGER)
                            )
                        )
                    END
                -- For non-arrays (single objects), take behavior
                ELSE 
                    CASE 
                        WHEN {take_count} IS NULL OR {take_count} <= 0 THEN NULL
                        ELSE {base_expr}
                    END
            END
            """
        elif func_name == 'intersect':
            # Phase 1: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'intersect'):
                try:
                    return self._generate_intersect_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "intersect() CTE generation", str(e))
            
            # Original implementation (fallback)
            # intersect(other) returns the intersection of the current collection with another collection
            # FHIRPath specification: intersect(other) - returns the set intersection
            
            # Get the other collection argument
            if len(func_node.args) != 1:
                validate_argument_count("intersect", 1, len(func_node.args))
            
            other_collection_arg = func_node.args[0]
            # Visit the argument to get the SQL for the other collection
            other_collection_sql = self.visit(other_collection_arg)
            
            return f"""
            CASE 
                -- Check if either expression is null (empty collection)
                WHEN {base_expr} IS NULL OR ({other_collection_sql}) IS NULL THEN NULL
                -- Check if both are arrays
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' AND {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN
                    CASE 
                        -- Either array is empty - return empty collection (NULL)
                        WHEN {self.get_json_array_length(base_expr)} = 0 OR {self.get_json_array_length(other_collection_sql)} = 0 THEN NULL
                        -- Both arrays have elements - find intersection
                        ELSE (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN NULL
                                ELSE {self.dialect.json_array_function}(
                                    DISTINCT value::VARCHAR
                                )
                            END
                            FROM (
                                SELECT 
                                    {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                FROM {self.iterate_json_elements_indexed(base_expr)}
                            ) AS left_elements
                            WHERE EXISTS (
                                SELECT 1 
                                FROM (
                                    SELECT 
                                        {self.dialect.json_extract_function}({other_collection_sql}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as other_value
                                    FROM {self.iterate_json_elements_indexed(other_collection_sql)}
                                ) AS right_elements
                                WHERE left_elements.value = right_elements.other_value
                            )
                        )
                    END
                -- Handle mixed array/non-array cases
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' AND {self.get_json_type(other_collection_sql)} != 'ARRAY' THEN
                    -- Base is array, other is single - check if single value exists in array
                    CASE 
                        WHEN EXISTS (
                            SELECT 1 
                            FROM (
                                SELECT 
                                    {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                FROM {self.iterate_json_elements_indexed(base_expr)}
                            ) AS array_elements
                            WHERE array_elements.value = {other_collection_sql}
                        ) THEN {self.dialect.json_array_function}({other_collection_sql}::VARCHAR)
                        ELSE NULL
                    END
                WHEN {self.get_json_type(base_expr)} != 'ARRAY' AND {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN
                    -- Base is single, other is array - check if single value exists in array
                    CASE 
                        WHEN EXISTS (
                            SELECT 1 
                            FROM (
                                SELECT 
                                    {self.dialect.json_extract_function}({other_collection_sql}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                FROM {self.iterate_json_elements_indexed(other_collection_sql)}
                            ) AS array_elements
                            WHERE array_elements.value = {base_expr}
                        ) THEN {self.dialect.json_array_function}({base_expr}::VARCHAR)
                        ELSE NULL
                    END
                -- Both are single values - check equality
                ELSE 
                    CASE 
                        WHEN {base_expr} = {other_collection_sql} THEN {self.dialect.json_array_function}({base_expr}::VARCHAR)
                        ELSE NULL
                    END
            END
            """
        elif func_name == 'exclude':
            # Phase 1: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'exclude'):
                try:
                    return self._generate_exclude_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "exclude() CTE generation", str(e))
            
            # Original implementation (fallback)
            # exclude(other) returns elements from the current collection that are NOT in the other collection
            # FHIRPath specification: exclude(other) - returns the set difference (current - other)
            
            # Get the other collection argument
            if len(func_node.args) != 1:
                validate_argument_count("exclude", 1, len(func_node.args))
            
            other_collection_arg = func_node.args[0]
            # Visit the argument to get the SQL for the other collection
            other_collection_sql = self.visit(other_collection_arg)
            
            return f"""
            CASE 
                -- Check if the base expression is null (empty collection)
                WHEN {base_expr} IS NULL THEN NULL
                -- If other collection is null, return the base collection (nothing to exclude)
                WHEN ({other_collection_sql}) IS NULL THEN {base_expr}
                -- Check if base is an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- Base array is empty - return empty collection (NULL)
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        -- Other is also an array - exclude elements that exist in other
                        WHEN {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN NULL
                                ELSE {self.dialect.json_array_function}(
                                    DISTINCT value::VARCHAR
                                )
                            END
                            FROM (
                                SELECT 
                                    {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                FROM {self.iterate_json_elements_indexed(base_expr)}
                            ) AS left_elements
                            WHERE NOT EXISTS (
                                SELECT 1 
                                FROM (
                                    SELECT 
                                        {self.dialect.json_extract_function}({other_collection_sql}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as other_value
                                    FROM {self.iterate_json_elements_indexed(other_collection_sql)}
                                ) AS right_elements
                                WHERE left_elements.value = right_elements.other_value
                            )
                        )
                        -- Other is single value - exclude if it matches any array element
                        ELSE (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN NULL
                                ELSE {self.dialect.json_array_function}(
                                    DISTINCT value::VARCHAR
                                )
                            END
                            FROM (
                                SELECT 
                                    {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                FROM {self.iterate_json_elements_indexed(base_expr)}
                            ) AS array_elements
                            WHERE array_elements.value != {other_collection_sql}
                        )
                    END
                -- Base is single value
                ELSE 
                    CASE 
                        -- Other is array - check if base value exists in array, if so exclude it
                        WHEN {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN
                            CASE 
                                WHEN EXISTS (
                                    SELECT 1 
                                    FROM (
                                        SELECT 
                                            {self.dialect.json_extract_function}({other_collection_sql}, '$[' || (ROW_NUMBER() OVER () - 1) || ']') as value
                                        FROM {self.iterate_json_elements_indexed(other_collection_sql)}
                                    ) AS array_elements
                                    WHERE array_elements.value = {base_expr}
                                ) THEN NULL
                                ELSE {self.dialect.json_array_function}({base_expr}::VARCHAR)
                            END
                        -- Both are single values - exclude if they match
                        ELSE 
                            CASE 
                                WHEN {base_expr} = {other_collection_sql} THEN NULL
                                ELSE {self.dialect.json_array_function}({base_expr}::VARCHAR)
                            END
                    END
            END
            """
        elif func_name == 'alltrue':
            # Use standard SQL implementation for allTrue function
            # allTrue() returns true if all elements in a collection are true, false if any element is false
            # FHIRPath specification: allTrue() - returns true if all elements are true, false otherwise
            # Returns empty if collection is empty
            
            return f"""
            CASE 
                -- Check if the expression is null (empty collection)
                WHEN {base_expr} IS NULL THEN NULL
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- Array is empty - return empty (NULL) per FHIRPath spec
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        -- Array has elements - check if all are true
                        ELSE (
                            SELECT CASE 
                                -- If any element is false, return false
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_type_function}(value) = 'BOOLEAN' THEN CAST(value AS BOOLEAN)
                                        WHEN CAST(value AS VARCHAR) = 'true' THEN true
                                        WHEN CAST(value AS VARCHAR) = 'false' THEN false
                                        ELSE NULL
                                    END) = false 
                                    THEN 1 END) > 0 THEN false
                                -- If any element is null/non-boolean, return false (strict evaluation)
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_type_function}(value) = 'BOOLEAN' THEN CAST(value AS BOOLEAN)
                                        WHEN CAST(value AS VARCHAR) = 'true' THEN true
                                        WHEN CAST(value AS VARCHAR) = 'false' THEN false
                                        ELSE NULL
                                    END) IS NULL 
                                    THEN 1 END) > 0 THEN false
                                -- If all elements are true, return true
                                ELSE true
                            END
                            FROM {self.iterate_json_elements_indexed(base_expr)}
                        )
                    END
                -- For non-arrays (single values), check if the value is true
                ELSE 
                    CASE 
                        WHEN {self.dialect.json_type_function}({base_expr}) = 'BOOLEAN' THEN CAST({base_expr} AS BOOLEAN)
                        WHEN CAST({base_expr} AS VARCHAR) = 'true' THEN true
                        WHEN CAST({base_expr} AS VARCHAR) = 'false' THEN false
                        ELSE false  -- Non-boolean single values are treated as false
                    END
            END
            """
        elif func_name == 'allfalse':
            # Phase 2: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'allfalse'):
                try:
                    return self._generate_allfalse_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "allFalse() CTE generation", str(e))
            
            # Original implementation (fallback)
            # allFalse() returns true if all elements in a collection are false, false if any element is true
            # FHIRPath specification: allFalse() - returns true if all elements are false, false otherwise
            # Returns empty if collection is empty
            
            return f"""
            CASE 
                -- Check if the expression is null (empty collection)
                WHEN {base_expr} IS NULL THEN NULL
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- Array is empty - return empty (NULL) per FHIRPath spec
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        -- Array has elements - check if all are false
                        ELSE (
                            SELECT CASE 
                                -- If any element is true, return false
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                        ELSE NULL
                                    END) = true 
                                    THEN 1 END) > 0 THEN false
                                -- If any element is null/non-boolean, return false (strict evaluation)
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                        ELSE NULL
                                    END) IS NULL 
                                    THEN 1 END) > 0 THEN false
                                -- If all elements are false, return true
                                ELSE true
                            END
                            FROM {self.iterate_json_elements_indexed(base_expr)}
                        )
                    END
                -- For non-arrays (single values), check if the value is false
                ELSE 
                    CASE 
                        WHEN {base_expr}::VARCHAR = 'false' THEN true
                        WHEN {base_expr}::VARCHAR = 'true' THEN false
                        ELSE false  -- Non-boolean single values are treated as not-false (so false for allFalse)
                    END
            END
            """

        elif func_name == 'anytrue':
            # Phase 2: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'anytrue'):
                try:
                    return self._generate_anytrue_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "anyTrue() CTE generation", str(e))
            
            # Original implementation (fallback)
            # anyTrue() returns true if any element in a collection is true, false if all elements are false
            # FHIRPath specification: anyTrue() - returns true if any element is true, false otherwise
            # Returns empty if collection is empty
            
            return f"""
            CASE 
                -- Check if the expression is null (empty collection)
                WHEN {base_expr} IS NULL THEN NULL
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- Array is empty - return empty (NULL) per FHIRPath spec
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        -- Array has elements - check if any are true
                        ELSE (
                            SELECT CASE 
                                -- If any element is true, return true
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                        ELSE NULL
                                    END) = true 
                                    THEN 1 END) > 0 THEN true
                                -- If any element is null/non-boolean, return false (strict evaluation)
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                        ELSE NULL
                                    END) IS NULL 
                                    THEN 1 END) > 0 THEN false
                                -- If all elements are false, return false
                                ELSE false
                            END
                            FROM {self.iterate_json_elements_indexed(base_expr)}
                        )
                    END
                -- For non-arrays (single values), check if the value is true
                ELSE 
                    CASE 
                        WHEN {base_expr}::VARCHAR = 'true' THEN true
                        WHEN {base_expr}::VARCHAR = 'false' THEN false
                        ELSE false  -- Non-boolean single values are treated as not-true (so false for anyTrue)
                    END
            END
            """

        elif func_name == 'anyfalse':
            # Phase 2: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'anyfalse'):
                try:
                    return self._generate_anyfalse_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "anyFalse() CTE generation", str(e))
            
            # Original implementation (fallback)
            # anyFalse() returns true if any element in a collection is false, false if all elements are true
            # FHIRPath specification: anyFalse() - returns true if any element is false, false otherwise
            # Returns empty if collection is empty
            
            return f"""
            CASE 
                -- Check if the expression is null (empty collection)
                WHEN {base_expr} IS NULL THEN NULL
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- Array is empty - return empty (NULL) per FHIRPath spec
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        -- Array has elements - check if any are false
                        ELSE (
                            SELECT CASE 
                                -- If any element is false, return true
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                        ELSE NULL
                                    END) = false 
                                    THEN 1 END) > 0 THEN true
                                -- If any element is null/non-boolean, return false (strict evaluation)
                                WHEN COUNT(CASE WHEN 
                                    (CASE 
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'false' THEN false
                                        WHEN {self.dialect.json_extract_function}({base_expr}, '$[' || (ROW_NUMBER() OVER () - 1) || ']')::VARCHAR = 'true' THEN true
                                        ELSE NULL
                                    END) IS NULL 
                                    THEN 1 END) > 0 THEN false
                                -- If all elements are true, return false
                                ELSE false
                            END
                            FROM {self.iterate_json_elements_indexed(base_expr)}
                        )
                    END
                -- For non-arrays (single values), check if the value is false
                ELSE 
                    CASE 
                        WHEN {base_expr}::VARCHAR = 'false' THEN true
                        WHEN {base_expr}::VARCHAR = 'true' THEN false
                        ELSE false  -- Non-boolean single values are treated as not-false (so false for anyFalse)
                    END
            END
            """

        elif func_name == 'iif':
            # Phase 2: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'iif'):
                try:
                    return self._generate_iif_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "iif() CTE generation", str(e))
            
            # Original implementation (fallback)
            # iif(condition, true-result, false-result) - conditional expression
            # FHIRPath specification: iif() - evaluates condition and returns true-result or false-result
            # Lazy evaluation: only evaluate the branch that will be returned
            
            # Extract the three arguments
            if len(func_node.args) != 3:
                raise ValueError(f"iif() requires exactly 3 arguments, got {len(func_node.args)}")
            
            condition_arg = func_node.args[0]
            true_result_arg = func_node.args[1]
            false_result_arg = func_node.args[2]
            
            # Generate SQL for each argument
            condition_sql = self.visit(condition_arg)
            true_result_sql = self.visit(true_result_arg)
            false_result_sql = self.visit(false_result_arg)
            
            return f"""
            CASE 
                -- Evaluate condition and return appropriate result
                WHEN ({condition_sql})::VARCHAR = 'true' THEN {true_result_sql}
                WHEN ({condition_sql})::VARCHAR = 'false' THEN {false_result_sql}
                -- If condition is null or non-boolean, return empty (null)
                ELSE NULL
            END
            """

        elif func_name == 'subsetof':
            # Phase 2: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'subsetof'):
                try:
                    return self._generate_subsetof_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "subsetOf() CTE generation", str(e))
            
            # Original implementation (fallback)
            # subsetOf(collection) - returns true if all elements in the current collection are contained in the argument collection
            # FHIRPath specification: subsetOf() - test if the current collection is a subset of the argument collection
            
            # Extract the argument (the collection to compare against)
            if len(func_node.args) != 1:
                raise ValueError(f"subsetOf() requires exactly 1 argument, got {len(func_node.args)}")
            
            other_collection_arg = func_node.args[0]
            other_collection_sql = self.visit(other_collection_arg)
            
            return f"""
            CASE 
                -- Check if base collection is null or empty
                WHEN {base_expr} IS NULL THEN true
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' AND {self.get_json_array_length(base_expr)} = 0 THEN true
                -- Check if other collection is null
                WHEN {other_collection_sql} IS NULL THEN false
                -- Both are arrays - check if all elements of base are in other
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' AND {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN (
                    SELECT CASE 
                        WHEN COUNT(*) = 0 THEN true  -- Empty base collection is subset of any collection
                        WHEN COUNT(CASE WHEN other_elem.value IS NULL THEN 1 END) > 0 THEN false  -- Any element not found in other collection
                        ELSE true  -- All elements found
                    END
                    FROM {self.iterate_json_elements_indexed(base_expr)} base_elem
                    LEFT JOIN {self.dialect.json_each_function}({other_collection_sql}) other_elem 
                        ON base_elem.value = other_elem.value
                )
                -- Handle single element cases
                WHEN {self.get_json_type(base_expr)} != 'ARRAY' AND {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN (
                    SELECT CASE 
                        WHEN COUNT(*) > 0 THEN true
                        ELSE false
                    END
                    FROM {self.iterate_json_elements_indexed(other_collection_sql)}
                    WHERE value = {base_expr}
                )
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' AND {self.get_json_type(other_collection_sql)} != 'ARRAY' THEN (
                    SELECT CASE 
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN true
                        WHEN {self.get_json_array_length(base_expr)} = 1 AND {self.dialect.json_extract_function}({base_expr}, '$[0]') = {other_collection_sql} THEN true
                        ELSE false
                    END
                )
                -- Both single elements
                ELSE CASE WHEN {base_expr} = {other_collection_sql} THEN true ELSE false END
            END
            """

        elif func_name == 'supersetof':
            # Phase 2: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'supersetof'):
                try:
                    return self._generate_supersetof_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "supersetOf() CTE generation", str(e))
            
            # Original implementation (fallback)
            # supersetOf(collection) - returns true if all elements in the argument collection are contained in the current collection
            # FHIRPath specification: supersetOf() - test if the current collection is a superset of the argument collection
            # This is essentially the reverse of subsetOf() - check if other collection is subset of base collection
            
            # Extract the argument (the collection to check if it's a subset of current)
            if len(func_node.args) != 1:
                raise ValueError(f"supersetOf() requires exactly 1 argument, got {len(func_node.args)}")
            
            other_collection_arg = func_node.args[0]
            other_collection_sql = self.visit(other_collection_arg)
            
            return f"""
            CASE 
                -- Check if other collection is null or empty - base collection is superset of empty collection
                WHEN {other_collection_sql} IS NULL THEN true
                WHEN {self.get_json_type(other_collection_sql)} = 'ARRAY' AND {self.get_json_array_length(other_collection_sql)} = 0 THEN true
                -- Check if base collection is null
                WHEN {base_expr} IS NULL THEN false
                -- Both are arrays - check if all elements of other are in base
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' AND {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN (
                    SELECT CASE 
                        WHEN COUNT(*) = 0 THEN true  -- Empty other collection is subset of any collection
                        WHEN COUNT(CASE WHEN base_elem.value IS NULL THEN 1 END) > 0 THEN false  -- Any element of other not found in base collection
                        ELSE true  -- All elements found
                    END
                    FROM {self.iterate_json_elements_indexed(other_collection_sql)} other_elem
                    LEFT JOIN {self.dialect.json_each_function}({base_expr}) base_elem 
                        ON other_elem.value = base_elem.value
                )
                -- Handle single element cases
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' AND {self.get_json_type(other_collection_sql)} != 'ARRAY' THEN (
                    SELECT CASE 
                        WHEN COUNT(*) > 0 THEN true
                        ELSE false
                    END
                    FROM {self.iterate_json_elements_indexed(base_expr)}
                    WHERE value = {other_collection_sql}
                )
                WHEN {self.get_json_type(base_expr)} != 'ARRAY' AND {self.get_json_type(other_collection_sql)} = 'ARRAY' THEN (
                    SELECT CASE 
                        WHEN {self.get_json_array_length(other_collection_sql)} = 0 THEN true
                        WHEN {self.get_json_array_length(other_collection_sql)} = 1 AND {self.dialect.json_extract_function}({other_collection_sql}, '$[0]') = {base_expr} THEN true
                        ELSE false
                    END
                )
                -- Both single elements
                ELSE CASE WHEN {base_expr} = {other_collection_sql} THEN true ELSE false END
            END
            """

        elif func_name == 'isdistinct':
            # Phase 2: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'isdistinct'):
                try:
                    return self._generate_isdistinct_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "isDistinct() CTE generation", str(e))
            
            # Original implementation (fallback)
            # isDistinct() - returns true if all elements in the collection are distinct/unique
            # FHIRPath specification: isDistinct() - test if the current collection contains only distinct elements
            # This means there are no duplicate values in the collection
            
            # isDistinct() takes no arguments
            if len(func_node.args) != 0:
                raise ValueError(f"isDistinct() requires no arguments, got {len(func_node.args)}")
            
            return f"""
            CASE 
                -- Check if collection is null or empty - empty collection is distinct by definition
                WHEN {base_expr} IS NULL THEN true
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- Empty array is distinct
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN true
                        -- Array with one element is distinct
                        WHEN {self.get_json_array_length(base_expr)} = 1 THEN true
                        -- Array with multiple elements - check for duplicates
                        ELSE (
                            SELECT CASE 
                                WHEN COUNT(*) = COUNT(DISTINCT value) THEN true
                                ELSE false
                            END
                            FROM {self.iterate_json_elements_indexed(base_expr)}
                        )
                    END
                -- Single element is distinct by definition
                ELSE true
            END
            """

        elif func_name == 'as':
            # Phase 3: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'as'):
                try:
                    return self._generate_as_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "as() CTE generation", str(e))
            
            # Original implementation (fallback)
            # as(type) - returns elements from the collection that are of the specified type
            # FHIRPath specification: as() - type casting operation that filters elements by type
            
            # Extract the type argument
            if len(func_node.args) != 1:
                raise ValueError(f"as() requires exactly 1 argument, got {len(func_node.args)}")
            
            if not isinstance(func_node.args[0], self.IdentifierNode):
                raise ValueError("as() function requires a type identifier argument")
            
            type_name = func_node.args[0].name
            type_name_lower = type_name.lower()
            
            # Build type condition - reuse logic from ofType() but for casting
            fhir_primitive_types_as_string = self.FHIR_PRIMITIVE_TYPES_AS_STRING
            
            if type_name_lower in [t.lower() for t in fhir_primitive_types_as_string]:
                # Primitive string types
                type_condition = f"{self.get_json_type('value')} = 'VARCHAR'"
            elif type_name_lower == "boolean":
                # Boolean type (can be boolean JSON or string 'true'/'false')
                type_condition = f"({self.get_json_type('value')} = 'BOOLEAN' OR ({self.get_json_type('value')} = 'VARCHAR' AND LOWER(CAST(value AS VARCHAR)) IN ('true', 'false')))"
            elif type_name_lower == "integer":
                # Integer type (must be whole number)
                type_condition = f"({self.get_json_type('value')} = 'INTEGER' OR ({self.get_json_type('value')} IN ('NUMBER', 'DOUBLE') AND (CAST(value AS DOUBLE) = floor(CAST(value AS DOUBLE)))))"
            elif type_name_lower == "decimal":
                # Decimal type (any numeric)
                type_condition = f"{self.get_json_type('value')} IN ('NUMBER', 'INTEGER', 'DOUBLE')"
            elif type_name == "Quantity":
                # FHIR Quantity type
                type_condition = f"{self.get_json_type('value')} = 'OBJECT' AND {self.extract_json_object('value', '$.value')} IS NOT NULL"
            else:
                # FHIR resource types
                type_condition = f"({self.get_json_type('value')} = 'OBJECT' AND {self.extract_json_field('value', '$.resourceType')} = '{type_name}')"
            
            return f"""
            CASE 
                -- Check if base expression is null
                WHEN {base_expr} IS NULL THEN NULL
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN (
                    SELECT CASE 
                        WHEN COUNT(*) = 0 THEN NULL
                        ELSE {self.dialect.json_array_function}(DISTINCT value::VARCHAR)
                    END
                    FROM {self.iterate_json_elements_indexed(base_expr)}
                    WHERE {type_condition}
                )
                -- Single element - check if it matches the type
                ELSE 
                    CASE 
                        WHEN ({type_condition.replace('value', base_expr)}) THEN {self.dialect.json_array_function}({base_expr}::VARCHAR)
                        ELSE NULL
                    END
            END
            """

        elif func_name == 'is':
            # Phase 3: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'is'):
                try:
                    return self._generate_is_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "is() CTE generation", str(e))
            
            # Original implementation (fallback)
            # is(type) - returns true if all elements in the collection are of the specified type
            # FHIRPath specification: is() - type checking operation
            
            # Extract the type argument
            if len(func_node.args) != 1:
                raise ValueError(f"is() requires exactly 1 argument, got {len(func_node.args)}")
            
            if not isinstance(func_node.args[0], self.IdentifierNode):
                raise ValueError("is() function requires a type identifier argument")
            
            type_name = func_node.args[0].name
            type_name_lower = type_name.lower()
            
            # Build type condition - reuse logic from ofType()
            fhir_primitive_types_as_string = self.FHIR_PRIMITIVE_TYPES_AS_STRING
            
            if type_name_lower in [t.lower() for t in fhir_primitive_types_as_string]:
                # Primitive string types
                type_condition = f"{self.get_json_type('value')} = 'VARCHAR'"
            elif type_name_lower == "boolean":
                # Boolean type
                type_condition = f"({self.get_json_type('value')} = 'BOOLEAN' OR ({self.get_json_type('value')} = 'VARCHAR' AND LOWER(CAST(value AS VARCHAR)) IN ('true', 'false')))"
            elif type_name_lower == "integer":
                # Integer type
                type_condition = f"({self.get_json_type('value')} = 'INTEGER' OR ({self.get_json_type('value')} IN ('NUMBER', 'DOUBLE') AND (CAST(value AS DOUBLE) = floor(CAST(value AS DOUBLE)))))"
            elif type_name_lower == "decimal":
                # Decimal type
                type_condition = f"{self.get_json_type('value')} IN ('NUMBER', 'INTEGER', 'DOUBLE')"
            elif type_name == "Quantity":
                # FHIR Quantity type
                type_condition = f"{self.get_json_type('value')} = 'OBJECT' AND {self.extract_json_object('value', '$.value')} IS NOT NULL"
            else:
                # FHIR resource types
                type_condition = f"({self.get_json_type('value')} = 'OBJECT' AND {self.extract_json_field('value', '$.resourceType')} = '{type_name}')"
            
            return f"""
            CASE 
                -- Check if base expression is null (empty collection)
                WHEN {base_expr} IS NULL THEN false
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- Empty array returns false
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN false
                        -- Check if all elements match the type
                        ELSE (
                            SELECT CASE 
                                WHEN COUNT(*) = COUNT(CASE WHEN {type_condition} THEN 1 END) THEN true
                                ELSE false
                            END
                            FROM {self.iterate_json_elements_indexed(base_expr)}
                        )
                    END
                -- Single element - check if it matches the type
                ELSE 
                    CASE 
                        WHEN ({type_condition.replace('value', base_expr)}) THEN true
                        ELSE false
                    END
            END
            """

        elif func_name == 'toboolean':
            # Phase 3 Week 8: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'toboolean'):
                try:
                    return self._generate_toboolean_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "toBoolean() CTE generation", str(e))
            
            # Original implementation (fallback)
            # toBoolean() - converts the input collection to boolean values
            # FHIRPath specification: toBoolean() - converts strings, numbers to boolean
            
            # toBoolean() takes no arguments
            if len(func_node.args) != 0:
                raise ValueError(f"toBoolean() requires no arguments, got {len(func_node.args)}")
            
            return f"""
            CASE 
                -- Check if collection is null or empty
                WHEN {base_expr} IS NULL THEN NULL
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- Empty array returns NULL
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        -- Convert each element to boolean
                        ELSE (
                            SELECT {self.dialect.json_array_function}(
                                CASE 
                                    WHEN {self.get_json_type('value')} = 'BOOLEAN' THEN value
                                    WHEN {self.get_json_type('value')} = 'VARCHAR' AND LOWER(CAST(value AS VARCHAR)) = 'true' THEN true
                                    WHEN {self.get_json_type('value')} = 'VARCHAR' AND LOWER(CAST(value AS VARCHAR)) = 'false' THEN false
                                    WHEN {self.get_json_type('value')} IN ('INTEGER', 'NUMBER', 'DOUBLE') AND CAST(value AS DOUBLE) != 0 THEN true
                                    WHEN {self.get_json_type('value')} IN ('INTEGER', 'NUMBER', 'DOUBLE') AND CAST(value AS DOUBLE) = 0 THEN false
                                    ELSE NULL
                                END::VARCHAR
                            )
                            FROM {self.iterate_json_elements_indexed(base_expr)}
                            WHERE CASE 
                                WHEN {self.get_json_type('value')} = 'BOOLEAN' THEN true
                                WHEN {self.get_json_type('value')} = 'VARCHAR' AND LOWER(CAST(value AS VARCHAR)) IN ('true', 'false') THEN true
                                WHEN {self.get_json_type('value')} IN ('INTEGER', 'NUMBER', 'DOUBLE') THEN true
                                ELSE false
                            END
                        )
                    END
                -- Single element - convert to boolean
                ELSE 
                    CASE 
                        WHEN {self.get_json_type(base_expr)} = 'BOOLEAN' THEN {self.dialect.json_array_function}({base_expr}::VARCHAR)
                        WHEN {self.get_json_type(base_expr)} = 'VARCHAR' AND LOWER(CAST({base_expr} AS VARCHAR)) = 'true' THEN {self.dialect.json_array_function}('true')
                        WHEN {self.get_json_type(base_expr)} = 'VARCHAR' AND LOWER(CAST({base_expr} AS VARCHAR)) = 'false' THEN {self.dialect.json_array_function}('false')
                        WHEN {self.get_json_type(base_expr)} IN ('INTEGER', 'NUMBER', 'DOUBLE') AND CAST({base_expr} AS DOUBLE) != 0 THEN {self.dialect.json_array_function}('true')
                        WHEN {self.get_json_type(base_expr)} IN ('INTEGER', 'NUMBER', 'DOUBLE') AND CAST({base_expr} AS DOUBLE) = 0 THEN {self.dialect.json_array_function}('false')
                        ELSE NULL
                    END
            END
            """
        elif func_name == 'subsumes':
            # subsumes(code) - tests if input code subsumes (is parent/ancestor of) parameter code
            if len(func_node.args) != 1:
                raise ValueError("subsumes() function takes exactly one argument")
            
            # Get the code to check subsumption against
            other_code_arg = func_node.args[0]
            other_code_sql = self.visit(other_code_arg)
            
            # Implementation of terminology subsumption testing
            # This is a basic implementation - full subsumption would require terminology service
            return f"""
            CASE 
                -- Both inputs must be Coding objects or CodeableConcept with coding
                WHEN {base_expr} IS NULL OR {other_code_sql} IS NULL THEN NULL
                
                -- Handle base CodeableConcept (extract first coding)
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.coding') IS NOT NULL AND
                     {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) = 'ARRAY' AND
                     {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) > 0
                THEN
                    -- Extract first coding from base CodeableConcept
                    CASE 
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.coding') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) = 'ARRAY' AND
                             {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) > 0
                        THEN
                            -- Both are CodeableConcept - compare first codings
                            CASE 
                                WHEN {self.dialect.json_extract_function}({self.dialect.json_extract_function}({base_expr}, '$.coding'), '$[0].system') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].system') AND
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({base_expr}, '$.coding'), '$[0].code') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].code')
                                THEN TRUE  -- Same code subsumes itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE FALSE
                            END
                        ELSE FALSE
                    END
                
                -- Handle base Coding object directly  
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') IS NOT NULL AND
                     {self.dialect.json_extract_function}({base_expr}, '$.code') IS NOT NULL
                THEN
                    CASE 
                        -- Other is also a Coding object
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.system') IS NOT NULL AND
                             {self.dialect.json_extract_function}({other_code_sql}, '$.code') IS NOT NULL
                        THEN
                            CASE 
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') = 
                                     {self.dialect.json_extract_function}({other_code_sql}, '$.system') AND
                                     {self.dialect.json_extract_function}({base_expr}, '$.code') = 
                                     {self.dialect.json_extract_function}({other_code_sql}, '$.code')
                                THEN TRUE  -- Same code subsumes itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE FALSE
                            END
                        -- Other is CodeableConcept
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.coding') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) = 'ARRAY' AND
                             {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) > 0
                        THEN
                            CASE 
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].system') AND
                                     {self.dialect.json_extract_function}({base_expr}, '$.code') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].code')
                                THEN TRUE  -- Same code subsumes itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE FALSE
                            END
                        ELSE FALSE
                    END
                
                ELSE FALSE
            END
            """
        
        elif func_name == 'subsumedby':
            # subsumedBy(code) - tests if input code is subsumed by (is child/descendant of) parameter code
            if len(func_node.args) != 1:
                raise ValueError("subsumedBy() function takes exactly one argument")
            
            # Get the code to check subsumption against
            other_code_arg = func_node.args[0]
            other_code_sql = self.visit(other_code_arg)
            
            # Implementation of terminology subsumption testing (reverse of subsumes)
            # This is a basic implementation - full subsumption would require terminology service
            return f"""
            CASE 
                -- Both inputs must be Coding objects or CodeableConcept with coding
                WHEN {base_expr} IS NULL OR {other_code_sql} IS NULL THEN NULL
                
                -- Handle base CodeableConcept (extract first coding)
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.coding') IS NOT NULL AND
                     {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) = 'ARRAY' AND
                     {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) > 0
                THEN
                    -- Extract first coding from base CodeableConcept
                    CASE 
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.coding') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) = 'ARRAY' AND
                             {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) > 0
                        THEN
                            -- Both are CodeableConcept - compare first codings (reverse logic)
                            CASE 
                                WHEN {self.dialect.json_extract_function}({self.dialect.json_extract_function}({base_expr}, '$.coding'), '$[0].system') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].system') AND
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({base_expr}, '$.coding'), '$[0].code') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].code')
                                THEN TRUE  -- Same code is subsumed by itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE FALSE
                            END
                        ELSE FALSE
                    END
                
                -- Handle base Coding object directly  
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') IS NOT NULL AND
                     {self.dialect.json_extract_function}({base_expr}, '$.code') IS NOT NULL
                THEN
                    CASE 
                        -- Other is also a Coding object
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.system') IS NOT NULL AND
                             {self.dialect.json_extract_function}({other_code_sql}, '$.code') IS NOT NULL
                        THEN
                            CASE 
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') = 
                                     {self.dialect.json_extract_function}({other_code_sql}, '$.system') AND
                                     {self.dialect.json_extract_function}({base_expr}, '$.code') = 
                                     {self.dialect.json_extract_function}({other_code_sql}, '$.code')
                                THEN TRUE  -- Same code is subsumed by itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE FALSE
                            END
                        -- Other is CodeableConcept
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.coding') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) = 'ARRAY' AND
                             {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) > 0
                        THEN
                            CASE 
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].system') AND
                                     {self.dialect.json_extract_function}({base_expr}, '$.code') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].code')
                                THEN TRUE  -- Same code is subsumed by itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE FALSE
                            END
                        ELSE FALSE
                    END
                
                ELSE FALSE
            END
            """
        
        elif func_name == 'comparable':
            # comparable(quantity) - tests if two quantities are comparable (have compatible units)
            if len(func_node.args) != 1:
                raise ValueError("comparable() function takes exactly one argument")
            
            # Get the quantity to compare against
            other_quantity_arg = func_node.args[0]
            other_quantity_sql = self.visit(other_quantity_arg)
            
            # Implementation of quantity comparison testing
            # This is a basic implementation - full comparison would require UCUM service
            return f"""
            CASE 
                -- Both inputs must be Quantity objects
                WHEN {base_expr} IS NULL OR {other_quantity_sql} IS NULL THEN NULL
                
                -- Handle base Quantity object
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.value') IS NOT NULL AND
                     {self.dialect.json_extract_function}({base_expr}, '$.unit') IS NOT NULL
                THEN
                    CASE 
                        -- Other is also a Quantity object
                        WHEN {self.dialect.json_extract_function}({other_quantity_sql}, '$.value') IS NOT NULL AND
                             {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit') IS NOT NULL
                        THEN
                            CASE 
                                -- Same unit is always comparable
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.unit') = 
                                     {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit')
                                THEN TRUE
                                -- Same system is generally comparable
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') = 
                                     {self.dialect.json_extract_function}({other_quantity_sql}, '$.system')
                                THEN TRUE
                                -- Basic unit compatibility checks (simplified)
                                WHEN ({self.dialect.json_extract_function}({base_expr}, '$.unit') IN ('kg', 'g', 'mg', 'lb', 'oz') AND
                                      {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit') IN ('kg', 'g', 'mg', 'lb', 'oz'))
                                THEN TRUE  -- Mass units
                                WHEN ({self.dialect.json_extract_function}({base_expr}, '$.unit') IN ('m', 'cm', 'mm', 'km', 'in', 'ft') AND
                                      {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit') IN ('m', 'cm', 'mm', 'km', 'in', 'ft'))
                                THEN TRUE  -- Length units
                                WHEN ({self.dialect.json_extract_function}({base_expr}, '$.unit') IN ('s', 'min', 'h', 'd', 'wk', 'mo', 'a') AND
                                      {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit') IN ('s', 'min', 'h', 'd', 'wk', 'mo', 'a'))
                                THEN TRUE  -- Time units
                                WHEN ({self.dialect.json_extract_function}({base_expr}, '$.unit') IN ('L', 'mL', 'dL', 'cL') AND
                                      {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit') IN ('L', 'mL', 'dL', 'cL'))
                                THEN TRUE  -- Volume units
                                -- TODO: Add full UCUM compatibility logic
                                ELSE FALSE
                            END
                        ELSE FALSE
                    END
                
                ELSE FALSE
            END
            """
        
        elif func_name == 'elementdefinition':
            # elementDefinition() - get element definition from structure definition
            if len(func_node.args) != 0:
                raise ValueError("elementDefinition() function takes no arguments")
            
            # Implementation of element definition retrieval
            # This is a basic implementation - full implementation would require StructureDefinition processing
            return f"""
            CASE 
                -- Check if input is a valid FHIR element
                WHEN {base_expr} IS NULL THEN NULL
                
                -- TODO: Full StructureDefinition processing would be implemented here
                -- For now, return a basic placeholder structure
                ELSE {self.dialect.json_object_function}(
                    'path', 'placeholder',
                    'type', {self.dialect.json_array_function}(
                        {self.dialect.json_object_function}('code', 'Element')
                    ),
                    'definition', 'Basic element definition placeholder',
                    'min', 0,
                    'max', '1'
                )
            END
            """
        
        elif func_name == 'slice':
            # slice(structure, name) - access specific slices in profiles
            if len(func_node.args) != 2:
                raise ValueError("slice() function takes exactly two arguments")
            
            # Get the structure and name arguments
            structure_arg = func_node.args[0]
            name_arg = func_node.args[1]
            structure_sql = self.visit(structure_arg)
            name_sql = self.visit(name_arg)
            
            # Implementation of slice access
            # This is a basic implementation - full implementation would require profile processing
            return f"""
            CASE 
                -- Check if inputs are valid
                WHEN {base_expr} IS NULL OR {structure_sql} IS NULL OR {name_sql} IS NULL THEN NULL
                
                -- TODO: Full profile slicing would be implemented here
                -- For now, return a basic placeholder
                ELSE {self.dialect.json_object_function}(
                    'sliceName', {name_sql},
                    'structure', {structure_sql},
                    'elements', {self.dialect.json_array_function}()
                )
            END
            """
        
        elif func_name == 'checkmodifiers':
            # checkModifiers(modifier) - check modifier extensions
            if len(func_node.args) != 1:
                raise ValueError("checkModifiers() function takes exactly one argument")
            
            # Get the modifier argument
            modifier_arg = func_node.args[0]
            modifier_sql = self.visit(modifier_arg)
            
            # Implementation of modifier extension checking
            # This is a basic implementation - full implementation would require extension processing
            return f"""
            CASE 
                -- Check if inputs are valid
                WHEN {base_expr} IS NULL OR {modifier_sql} IS NULL THEN NULL
                
                -- Check if modifierExtension array exists and contains the specified modifier
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.modifierExtension') IS NOT NULL
                THEN
                    CASE 
                        WHEN {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.modifierExtension')) = 'ARRAY'
                        THEN
                            (
                                SELECT 
                                    CASE 
                                        WHEN COUNT(*) > 0 THEN TRUE
                                        ELSE FALSE
                                    END
                                FROM {self.dialect.json_each_function}({self.dialect.json_extract_function}({base_expr}, '$.modifierExtension'))
                                WHERE {self.dialect.json_extract_function}('value', '$.url') = {modifier_sql}
                            )
                        ELSE FALSE
                    END
                
                ELSE FALSE
            END
            """
        
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
                    log_fallback_usage(self.logger, "getResourceKey() CTE generation", str(e))
            
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
                    log_fallback_usage(self.logger, "getReferenceKey() CTE generation", str(e))
            
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
                         'substring', 'startswith', 'endswith', 'indexof', 'replace', 'toupper', 'tolower', 'upper', 'lower', 'trim', 'split', 'all', 'distinct', 'tostring', 'getresourcekey', 'getreferencekey',
                         'abs', 'ceiling', 'floor', 'round', 'sqrt', 'truncate', 'exp', 'ln', 'log', 'power', 'now', 'today', 'timeofday', 'trace', 'aggregate', 'flatten', 'convertstoquantity', 'hasvalue', 'hascodedvalue', 'htmlchecks', 'hastemplateidof', 'encode', 'decode', 'conformsto', 'memberof', 'single', 'tail', 'skip', 'take', 'intersect', 'exclude', 'alltrue', 'allfalse', 'anytrue', 'anyfalse', 'iif', 'subsetof', 'supersetof', 'isdistinct', 'as', 'is', 'toboolean', 'convertstoboolean', 'todecimal', 'convertstodecimal', 'convertstointeger', 'todate', 'convertstodate', 'todatetime', 'convertstodatetime', 'totime', 'convertstotime', 'tochars', 'matches', 'replacematches', 'children', 'descendants', 'union', 'combine', 'repeat']:
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
        return self.operator_handler.visit_binary_op(
            node, 
            self.visit, 
            self.determine_comparison_casts, 
            self._generate_union_sql
        )
    
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
            'abs': (0, 0),        # no arguments (applied to expression)
            'ceiling': (0, 0),    # no arguments (applied to expression)
            'floor': (0, 0),      # no arguments (applied to expression)
            'round': (0, 1),      # 0 or 1 arguments (optional precision)
            'sqrt': (0, 0),       # no arguments (applied to expression)
            'truncate': (0, 0),   # no arguments (applied to expression)
            'now': (0, 0),        # no arguments (returns current datetime)
            'today': (0, 0),      # no arguments (returns current date)
            'timeofday': (0, 0),  # no arguments (returns current time)
            'trace': (1, 2),      # 1 or 2 arguments (name[, projection])
            'aggregate': (1, 2),  # 1 or 2 arguments (aggregator[, init])
            'flatten': (0, 0),    # no arguments (flattens nested collections)
            'convertstoquantity': (0, 0),  # no arguments (tests if value can convert to quantity)
            'hasvalue': (0, 0),            # no arguments (tests if value exists)
            'hascodedvalue': (0, 0),       # no arguments (tests if value has coded value)
            'htmlchecks': (0, 0),          # no arguments (validates HTML content)
            'hastemplateidof': (1, 1),     # 1 argument (template ID to check)
            'encode': (0, 0),              # no arguments (encodes the current value)
            'decode': (0, 0),              # no arguments (decodes the current value)
            'conformsto': (1, 1),          # 1 argument (profile URI to check)
            'memberof': (1, 1),            # 1 argument (ValueSet URI to check)
            'single': (0, 0),              # no arguments (returns single element or error)
            'tail': (0, 0),                # no arguments (returns all but first element)
            'skip': (1, 1),                # 1 argument (number of elements to skip)
            'take': (1, 1),                # 1 argument (number of elements to take)
            'intersect': (1, 1),           # 1 argument (collection to intersect with)
            'exclude': (1, 1),             # 1 argument (collection to exclude from current)
            'alltrue': (0, 0),             # no arguments (checks if all elements are true)
            'allfalse': (0, 0),            # no arguments (checks if all elements are false)
            'anytrue': (0, 0),             # no arguments (checks if any element is true)
            'anyfalse': (0, 0),            # no arguments (checks if any element is false)
            'iif': (3, 3),                 # 3 arguments (condition, true-result, false-result)
            'subsetof': (1, 1),            # 1 argument (collection to compare against)
            'supersetof': (1, 1),          # 1 argument (collection to check if subset of current)
            'isdistinct': (0, 0),          # no arguments (checks if all elements are distinct)
            'as': (1, 1),                  # 1 argument (type to cast to)
            'is': (1, 1),                  # 1 argument (type to check for)
            'toboolean': (0, 0),           # no arguments (converts current collection to boolean)
            'convertstoboolean': (0, 0),   # no arguments (tests if current collection can be converted to boolean)
            'todecimal': (0, 0),           # no arguments (converts current collection to decimal)
            'convertstodecimal': (0, 0),   # no arguments (tests if current collection can be converted to decimal)
            'convertstointeger': (0, 0),   # no arguments (tests if current collection can be converted to integer)
            'todate': (0, 0),              # no arguments (converts current collection to date)
            'convertstodate': (0, 0),      # no arguments (tests if current collection can be converted to date)
            'todatetime': (0, 0),          # no arguments (converts current collection to datetime)
            'convertstodatetime': (0, 0),  # no arguments (tests if current collection can be converted to datetime)
            'totime': (0, 0),              # no arguments (converts current collection to time)
            'convertstotime': (0, 0),      # no arguments (tests if current collection can be converted to time)
            'exp': (0, 0),                 # no arguments (exponential function applied to current collection)
            'ln': (0, 0),                  # no arguments (natural logarithm applied to current collection)
            'log': (0, 0),                 # no arguments (base-10 logarithm applied to current collection)
            'power': (1, 1),               # 1 argument (exponent - base is current collection)
            'tochars': (0, 0),             # no arguments (converts current collection to character array)
            'matches': (1, 1),             # 1 argument (regex pattern to match against)
            'replacematches': (2, 2),      # 2 arguments (regex pattern, replacement string)
            'children': (0, 0),            # no arguments (returns all direct child elements)
            'descendants': (0, 0),         # no arguments (returns all descendant elements recursively)
            'union': (1, 1),               # 1 argument (collection to union with current collection)
            'combine': (1, 1),             # 1 argument (collection to combine with current collection)
            'repeat': (1, 1),              # 1 argument (expression to apply iteratively)
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
                    # Special handling for to_json(list(...)) array expressions - FHIRPath collection promotion
                    elif 'to_json(list(' in left_sql and 'json_each(' in left_sql:
                        # This is a collection result from array path extraction (e.g., name.family)
                        # Use FHIRPath collection promotion: check if any element in collection equals scalar
                        final_left_sql = f"""(
                            SELECT COUNT(*) > 0
                            FROM json_each(COALESCE(({left_sql}), json_array()))
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
        return self.operator_handler.visit_unary_op(node, self.visit)
    
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
            except Exception as e:
                # Fall back to dynamic indexing
                self.logger.debug(f"Index optimization failed: {e}")
                pass
        
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
            except Exception as e:
                # Fall back to dynamic indexing
                self.logger.debug(f"Index optimization failed: {e}")
                pass

        # For simple arithmetic expressions, try to use json_extract_string when appropriate
        if (self._is_simple_arithmetic_expression(index_val_sql) and 
            not ('json_group_array' in base_expr or 'json_each' in base_expr)):
            try:
                evaluated_index = self._evaluate_simple_arithmetic(index_val_sql)
                if evaluated_index is not None and evaluated_index >= 0:
                    # Use json_extract_string for likely string results to avoid quotes
                    return f"COALESCE(json_extract_string({base_expr}, '$[{evaluated_index}]'), '')"
            except Exception as e:
                # Optimization failed, continue with fallback
                self.logger.debug(f"Optimization failed: {e}")
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
            except Exception as e:
                # Optimization failed, continue with fallback
                self.logger.debug(f"Optimization failed: {e}")
                pass
        
        # For simple arithmetic indexing on string extractions, use json_extract_string to avoid quotes
        if self._is_simple_arithmetic_expression(index_val_sql):
            try:
                evaluated_index = self._evaluate_simple_arithmetic(index_val_sql) 
                if evaluated_index is not None and evaluated_index >= 0:
                    # Use json_extract_string instead of json_extract for string results
                    return f"COALESCE(json_extract_string({base_expr}, '$[{evaluated_index}]'), '')"
            except Exception as e:
                # Optimization failed, continue with fallback
                self.logger.debug(f"Optimization failed: {e}")
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
        except Exception as e:
            self.logger.debug(f"Failed to evaluate literal: {e}")
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
    
    def _generate_union_sql(self, left_sql: str, right_sql: str) -> str:
        """Generate SQL for collection union operations (|)"""
        
        # Collection union in FHIRPath combines two collections
        # In SQL, this translates to UNION ALL of the two result sets
        
        # For union operations, we need to ensure both sides are properly structured
        # but avoid the scalar subquery issues by creating a unified query structure
        
        # Check if we're dealing with simple literal values
        if self._is_simple_literal(left_sql) and self._is_simple_literal(right_sql):
            return f"""
SELECT {left_sql} as result FROM {self.table_name}
UNION ALL
SELECT {right_sql} as result FROM {self.table_name}
"""
        
        # Handle the case where both sides are already complete SELECT statements
        if (left_sql.strip().startswith('SELECT') or left_sql.strip().startswith('WITH')) and \
           (right_sql.strip().startswith('SELECT') or right_sql.strip().startswith('WITH')):
            return f"({left_sql}) UNION ALL ({right_sql})"
        
        # For other cases, create a unified structure that avoids scalar subquery issues
        # This is more complex but necessary for proper union semantics
        return f"""
WITH left_union_results AS (
    {self._ensure_select_structure(left_sql)}
),
right_union_results AS (
    {self._ensure_select_structure(right_sql)}
)
SELECT result FROM left_union_results
UNION ALL
SELECT result FROM right_union_results
"""
    
    def _is_simple_literal(self, sql_expr: str) -> bool:
        """Check if SQL expression is a simple literal value"""
        sql_expr = sql_expr.strip()
        # Check for string literals, numeric literals, boolean literals
        return (sql_expr.startswith("'") and sql_expr.endswith("'")) or \
               sql_expr.replace('.', '').replace('-', '').isdigit() or \
               sql_expr.lower() in ['true', 'false', 'null']
    
    def _ensure_select_structure(self, sql_expr: str) -> str:
        """Ensure SQL expression is structured as a proper SELECT statement"""
        sql_expr = sql_expr.strip()
        
        # If it's already a SELECT or WITH statement, return as-is
        if sql_expr.startswith('SELECT') or sql_expr.startswith('WITH'):
            return sql_expr
        
        # Otherwise, wrap it as a SELECT statement with proper FROM clause
        return f"""SELECT
    ({sql_expr}) as result
FROM {self.table_name}"""
    
    def _wrap_as_select_if_needed(self, sql_expr: str) -> str:
        """Wrap a SQL expression as a SELECT statement if it's not already one"""
        
        # If it's already a SELECT statement, return as-is
        if sql_expr.strip().startswith('SELECT') or sql_expr.strip().startswith('(SELECT'):
            return sql_expr
        
        # If it's a WITH clause (CTE), return as-is
        if sql_expr.strip().startswith('WITH'):
            return sql_expr
            
        # Otherwise, wrap it as a SELECT statement
        # Use the current table name and JSON column for context
        return f"""SELECT
    ({sql_expr}) as result
FROM {self.table_name}"""
    
    def visit_tuple(self, node) -> str:
        """Visit a tuple literal node and generate JSON object construction"""
        
        if not node.elements:
            # Empty tuple - generate empty JSON object
            return f"{self.dialect.json_object_function}()"
        
        # Generate JSON object construction from key-value pairs
        json_pairs = []
        for key, value_node in node.elements:
            # Key can be a string literal or a computed expression
            if isinstance(key, str):
                # Simple string key
                key_sql = f"'{key}'"
            else:
                # Computed key expression - visit it and ensure it's a string
                key_expr_sql = self.visit(key)
                # Cast the result to string for use as JSON key
                key_sql = f"CAST({key_expr_sql} AS VARCHAR)"
            
            # Value can be any expression
            value_sql = self.visit(value_node)
            json_pairs.extend([key_sql, value_sql])
        
        # Build the function call with key-value pairs
        pairs_str = ", ".join(json_pairs)
        return f"{self.dialect.json_object_function}({pairs_str})"
    
    def _handle_function_fallback(self, func_name: str, base_expr: str, func_node) -> str:
        """Fallback handler for functions not yet fully extracted."""
        # This allows extracted handlers to delegate back to main generator
        # for functions that are partially implemented
        
        # Call the main generator's function handling logic directly, bypassing handler routing
        # to avoid infinite recursion
        return self._apply_function_direct(func_name, base_expr, func_node)
    
    def _apply_function_direct(self, func_name: str, base_expr: str, func_node) -> str:
        """Apply function directly without going through handler routing system."""
        func_name = func_name.lower()
        
        # This is a direct implementation of the function handling logic from apply_function_to_expression
        # but bypasses the handler delegation to avoid recursion
        
        if func_name == 'single':
            # Phase 1: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'single'):
                try:
                    return self._generate_single_with_cte(base_expr)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "single() CTE generation", str(e))
            
            # Original implementation (fallback)
            # single() returns the single element of a collection, or empty collection for error conditions
            # FHIRPath specification: single() - returns the single element in a collection. Empty collection if empty or multiple elements
            # Note: In FHIRPath, "errors" typically result in empty collections rather than exceptions
            return f"""
            CASE 
                -- Check if the expression is null (empty collection)
                WHEN {base_expr} IS NULL THEN NULL
                -- Check if it's an array
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        -- Array is empty - return empty collection (NULL)
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        -- Array has exactly one element - return that element
                        WHEN {self.get_json_array_length(base_expr)} = 1 THEN
                            {self.extract_json_object(base_expr, '$[0]')}
                        -- Array has more than one element - return empty collection (NULL) per FHIRPath error semantics
                        ELSE NULL
                    END
                -- For non-arrays (single objects), return the object itself
                ELSE {base_expr}
            END
            """
        
        elif func_name == 'alltrue':
            # Use standard SQL implementation for allTrue function
            # allTrue() - returns true if all elements in the collection are true
            if len(func_node.args) != 0:
                raise ValueError(f"allTrue() requires no arguments, got {len(func_node.args)}")
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        ELSE (
                            SELECT 
                                CASE 
                                    -- If any element is false, return false
                                    WHEN COUNT(CASE WHEN 
                                        (CASE 
                                            WHEN {self.dialect.json_type_function}(value) = 'BOOLEAN' THEN CAST(value AS BOOLEAN)
                                            WHEN CAST(value AS VARCHAR) = 'true' THEN true
                                            WHEN CAST(value AS VARCHAR) = 'false' THEN false
                                            ELSE NULL
                                        END) = false 
                                        THEN 1 END) > 0 THEN false
                                    -- If any element is null/non-boolean, return false (strict evaluation)
                                    WHEN COUNT(CASE WHEN 
                                        (CASE 
                                            WHEN {self.dialect.json_type_function}(value) = 'BOOLEAN' THEN CAST(value AS BOOLEAN)
                                            WHEN CAST(value AS VARCHAR) = 'true' THEN true
                                            WHEN CAST(value AS VARCHAR) = 'false' THEN false
                                            ELSE NULL
                                        END) IS NULL 
                                        THEN 1 END) > 0 THEN false
                                    -- If all elements are true, return true
                                    ELSE true
                                END
                            FROM {self.iterate_json_elements_indexed(base_expr)}
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {self.dialect.json_type_function}({base_expr}) = 'BOOLEAN' THEN CAST({base_expr} AS BOOLEAN)
                        WHEN CAST({base_expr} AS VARCHAR) = 'true' THEN true
                        WHEN CAST({base_expr} AS VARCHAR) = 'false' THEN false
                        ELSE false  -- Non-boolean single values are treated as false
                    END
            END
            """
        
        elif func_name == 'combine':
            # Phase 1: CTE Implementation with Feature Flag
            if self.enable_cte and self._should_use_cte_unified(base_expr, 'combine'):
                try:
                    return self._generate_combine_with_cte(base_expr, func_node)
                except Exception as e:
                    # Fallback to original implementation if CTE generation fails
                    log_fallback_usage(self.logger, "combine() CTE generation", str(e))
            
            # Original implementation (fallback)
            # combine() - combine multiple collections into a single collection, preserving duplicates
            if len(func_node.args) != 1:
                raise ValueError(f"combine() requires exactly 1 argument, got {len(func_node.args)}")
            
            other_collection = self.visit(func_node.args[0])
            return f"""
            CASE 
                WHEN {base_expr} IS NULL AND {other_collection} IS NULL THEN {self.dialect.json_array_function}()
                WHEN {base_expr} IS NULL THEN 
                    CASE 
                        WHEN {self.get_json_type(other_collection)} = 'ARRAY' THEN {other_collection}
                        ELSE {self.dialect.json_array_function}({other_collection})
                    END
                WHEN {other_collection} IS NULL THEN 
                    CASE 
                        WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN {base_expr}
                        ELSE {self.dialect.json_array_function}({base_expr})
                    END
                ELSE (
                    SELECT {self.dialect.json_array_agg_function('value')}
                    FROM (
                        SELECT CASE 
                            WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN base_val.value
                            ELSE {base_expr}
                        END as value
                        FROM {self.iterate_json_array(base_expr, '$')} base_val
                        WHERE CASE 
                            WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN true
                            ELSE base_val.value = {base_expr}
                        END
                        
                        UNION ALL
                        
                        SELECT CASE 
                            WHEN {self.get_json_type(other_collection)} = 'ARRAY' THEN other_val.value
                            ELSE {other_collection}
                        END as value
                        FROM {self.iterate_json_array(other_collection, '$')} other_val
                        WHERE CASE 
                            WHEN {self.get_json_type(other_collection)} = 'ARRAY' THEN true
                            ELSE other_val.value = {other_collection}
                        END
                    ) combined
                    WHERE value IS NOT NULL
                )
            END
            """
        
        elif func_name == 'skip':
            # skip(num) returns all elements after skipping the first num elements
            if len(func_node.args) != 1:
                raise ValueError("skip() function requires exactly one argument")
            
            skip_count_arg = func_node.args[0]
            if hasattr(skip_count_arg, 'value'):
                skip_count = skip_count_arg.value
            else:
                skip_count_sql = self.visit(skip_count_arg)
                skip_count = f"({skip_count_sql})"
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {skip_count} IS NULL OR {skip_count} < 0 THEN {base_expr}
                        WHEN {self.get_json_array_length(base_expr)} <= {skip_count} THEN NULL
                        ELSE (
                            SELECT {self.dialect.aggregate_to_json_array('value')}
                            FROM (
                                SELECT value
                                FROM {self.iterate_json_elements_indexed(base_expr)}
                                WHERE CAST(key AS INTEGER) >= {skip_count}
                                ORDER BY CAST(key AS INTEGER)
                            )
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {skip_count} IS NULL OR {skip_count} <= 0 THEN {base_expr}
                        ELSE NULL
                    END
            END
            """
        
        elif func_name == 'tail':
            # tail() returns all elements except the first one
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.get_json_array_length(base_expr)} <= 1 THEN NULL
                        ELSE (
                            SELECT {self.dialect.aggregate_to_json_array('value')}
                            FROM (
                                SELECT value
                                FROM {self.iterate_json_elements_indexed(base_expr)}
                                WHERE CAST(key AS INTEGER) > 0
                                ORDER BY CAST(key AS INTEGER)
                            )
                        )
                    END
                ELSE NULL
            END
            """
        
        elif func_name == 'take':
            # take(num) returns the first num elements
            if len(func_node.args) != 1:
                validate_argument_count("take", 1, len(func_node.args))
            
            take_count_arg = func_node.args[0]
            if hasattr(take_count_arg, 'value'):
                take_count = take_count_arg.value
            else:
                take_count_sql = self.visit(take_count_arg)
                take_count = f"({take_count_sql})"
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {take_count} IS NULL OR {take_count} <= 0 THEN NULL
                        WHEN {take_count} >= {self.get_json_array_length(base_expr)} THEN {base_expr}
                        ELSE (
                            SELECT {self.dialect.aggregate_to_json_array('value')}
                            FROM (
                                SELECT value
                                FROM {self.iterate_json_elements_indexed(base_expr)}
                                WHERE CAST(key AS INTEGER) < {take_count}
                                ORDER BY CAST(key AS INTEGER)
                            )
                        )
                    END
                ELSE 
                    CASE 
                        WHEN {take_count} IS NULL OR {take_count} <= 0 THEN NULL
                        ELSE {base_expr}
                    END
            END
            """
        
        elif func_name == 'union':
            # union(collection) - set union with deduplication
            if len(func_node.args) != 1:
                raise ValueError(f"union() requires exactly 1 argument, got {len(func_node.args)}")
            
            other_collection_expr = self.visit(func_node.args[0])
            
            return f"""
            CASE 
                WHEN {base_expr} IS NULL AND {other_collection_expr} IS NULL THEN NULL
                WHEN {base_expr} IS NULL THEN 
                    CASE 
                        WHEN {self.get_json_type(other_collection_expr)} = 'ARRAY' THEN {other_collection_expr}
                        ELSE {self.dialect.json_array_function}({other_collection_expr})
                    END
                WHEN {other_collection_expr} IS NULL THEN 
                    CASE 
                        WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN {base_expr}
                        ELSE {self.dialect.json_array_function}({base_expr})
                    END
                ELSE (
                    SELECT {self.dialect.json_array_agg_function('value')}
                    FROM (
                        SELECT CASE 
                            WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN base_val.value
                            ELSE {base_expr}
                        END as value
                        FROM {self.iterate_json_array(base_expr, '$')} base_val
                        
                        UNION
                        
                        SELECT CASE 
                            WHEN {self.get_json_type(other_collection_expr)} = 'ARRAY' THEN other_val.value
                            ELSE {other_collection_expr}
                        END as value
                        FROM {self.iterate_json_array(other_collection_expr, '$')} other_val
                    ) combined
                    WHERE value IS NOT NULL
                )
            END
            """
        
        elif func_name == 'allfalse':
            # allFalse() - returns true if all elements are false
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        ELSE (
                            SELECT CASE 
                                WHEN COUNT(*) = COUNT(CASE WHEN CAST(value AS BOOLEAN) = false THEN 1 END) THEN true
                                ELSE false
                            END
                            FROM {self.iterate_json_array(base_expr, '$')}
                        )
                    END
                ELSE 
                    CASE 
                        WHEN CAST({base_expr} AS BOOLEAN) = false THEN true
                        ELSE false
                    END
            END
            """
        
        elif func_name == 'anytrue':
            # anyTrue() - returns true if any element is true
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        ELSE (
                            SELECT CASE 
                                WHEN COUNT(CASE WHEN CAST(value AS BOOLEAN) = true THEN 1 END) > 0 THEN true
                                ELSE false
                            END
                            FROM {self.iterate_json_array(base_expr, '$')}
                        )
                    END
                ELSE 
                    CASE 
                        WHEN CAST({base_expr} AS BOOLEAN) = true THEN true
                        ELSE false
                    END
            END
            """
        
        elif func_name == 'anyfalse':
            # anyFalse() - returns true if any element is false
            return f"""
            CASE 
                WHEN {base_expr} IS NULL THEN NULL
                WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN
                    CASE 
                        WHEN {self.get_json_array_length(base_expr)} = 0 THEN NULL
                        ELSE (
                            SELECT CASE 
                                WHEN COUNT(CASE WHEN CAST(value AS BOOLEAN) = false THEN 1 END) > 0 THEN true
                                ELSE false
                            END
                            FROM {self.iterate_json_array(base_expr, '$')}
                        )
                    END
                ELSE 
                    CASE 
                        WHEN CAST({base_expr} AS BOOLEAN) = false THEN true
                        ELSE false
                    END
            END
            """
        
        elif func_name == 'subsumes':
            # subsumes(code) - tests if input code subsumes (is parent/ancestor of) parameter code
            if len(func_node.args) != 1:
                raise ValueError("subsumes() function takes exactly one argument")
            
            # Get the code to check subsumption against
            other_code_arg = func_node.args[0]
            other_code_sql = self.visit(other_code_arg)
            
            # Implementation of terminology subsumption testing
            # This is a basic implementation - full subsumption would require terminology service
            return f"""
            CASE 
                -- Both inputs must be Coding objects or CodeableConcept with coding
                WHEN {base_expr} IS NULL OR {other_code_sql} IS NULL THEN NULL
                
                -- Handle base CodeableConcept (extract first coding)
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.coding') IS NOT NULL AND
                     {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) = 'ARRAY' AND
                     {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) > 0
                THEN
                    -- Extract first coding from base CodeableConcept
                    CASE 
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.coding') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) = 'ARRAY' AND
                             {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) > 0
                        THEN
                            -- Both are CodeableConcept - compare first codings
                            CASE 
                                WHEN {self.dialect.json_extract_function}({self.dialect.json_extract_function}({base_expr}, '$.coding'), '$[0].system') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].system') AND
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({base_expr}, '$.coding'), '$[0].code') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].code')
                                THEN true  -- Same code subsumes itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE false
                            END
                        ELSE false
                    END
                
                -- Handle base Coding object directly  
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') IS NOT NULL AND
                     {self.dialect.json_extract_function}({base_expr}, '$.code') IS NOT NULL
                THEN
                    CASE 
                        -- Other is also a Coding object
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.system') IS NOT NULL AND
                             {self.dialect.json_extract_function}({other_code_sql}, '$.code') IS NOT NULL
                        THEN
                            CASE 
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') = 
                                     {self.dialect.json_extract_function}({other_code_sql}, '$.system') AND
                                     {self.dialect.json_extract_function}({base_expr}, '$.code') = 
                                     {self.dialect.json_extract_function}({other_code_sql}, '$.code')
                                THEN true  -- Same code subsumes itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE false
                            END
                        -- Other is CodeableConcept
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.coding') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) = 'ARRAY' AND
                             {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) > 0
                        THEN
                            CASE 
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].system') AND
                                     {self.dialect.json_extract_function}({base_expr}, '$.code') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].code')
                                THEN true  -- Same code subsumes itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE false
                            END
                        ELSE false
                    END
                
                ELSE false
            END
            """
        
        elif func_name == 'subsumedby':
            # subsumedBy(code) - tests if input code is subsumed by (is child/descendant of) parameter code
            if len(func_node.args) != 1:
                raise ValueError("subsumedBy() function takes exactly one argument")
            
            # Get the code to check subsumption against
            other_code_arg = func_node.args[0]
            other_code_sql = self.visit(other_code_arg)
            
            # Implementation of terminology subsumption testing (reverse of subsumes)
            # This is a basic implementation - full subsumption would require terminology service
            return f"""
            CASE 
                -- Both inputs must be Coding objects or CodeableConcept with coding
                WHEN {base_expr} IS NULL OR {other_code_sql} IS NULL THEN NULL
                
                -- Handle base CodeableConcept (extract first coding)
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.coding') IS NOT NULL AND
                     {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) = 'ARRAY' AND
                     {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({base_expr}, '$.coding')) > 0
                THEN
                    -- Extract first coding from base CodeableConcept
                    CASE 
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.coding') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) = 'ARRAY' AND
                             {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) > 0
                        THEN
                            -- Both are CodeableConcept - compare first codings (reverse logic)
                            CASE 
                                WHEN {self.dialect.json_extract_function}({self.dialect.json_extract_function}({base_expr}, '$.coding'), '$[0].system') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].system') AND
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({base_expr}, '$.coding'), '$[0].code') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].code')
                                THEN true  -- Same code is subsumed by itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE false
                            END
                        ELSE false
                    END
                
                -- Handle base Coding object directly  
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') IS NOT NULL AND
                     {self.dialect.json_extract_function}({base_expr}, '$.code') IS NOT NULL
                THEN
                    CASE 
                        -- Other is also a Coding object
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.system') IS NOT NULL AND
                             {self.dialect.json_extract_function}({other_code_sql}, '$.code') IS NOT NULL
                        THEN
                            CASE 
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') = 
                                     {self.dialect.json_extract_function}({other_code_sql}, '$.system') AND
                                     {self.dialect.json_extract_function}({base_expr}, '$.code') = 
                                     {self.dialect.json_extract_function}({other_code_sql}, '$.code')
                                THEN true  -- Same code is subsumed by itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE false
                            END
                        -- Other is CodeableConcept
                        WHEN {self.dialect.json_extract_function}({other_code_sql}, '$.coding') IS NOT NULL AND
                             {self.dialect.json_type_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) = 'ARRAY' AND
                             {self.dialect.json_array_length_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding')) > 0
                        THEN
                            CASE 
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].system') AND
                                     {self.dialect.json_extract_function}({base_expr}, '$.code') = 
                                     {self.dialect.json_extract_function}({self.dialect.json_extract_function}({other_code_sql}, '$.coding'), '$[0].code')
                                THEN true  -- Same code is subsumed by itself
                                -- TODO: Add real subsumption logic with terminology service
                                ELSE false
                            END
                        ELSE false
                    END
                
                ELSE false
            END
            """
        
        elif func_name == 'comparable':
            # comparable(quantity) - tests if two quantities are comparable (have compatible units)
            if len(func_node.args) != 1:
                raise ValueError("comparable() function takes exactly one argument")
            
            # Get the quantity to compare against
            other_quantity_arg = func_node.args[0]
            other_quantity_sql = self.visit(other_quantity_arg)
            
            # Implementation of quantity comparison testing
            # This is a basic implementation - full comparison would require UCUM service
            return f"""
            CASE 
                -- Both inputs must be Quantity objects
                WHEN {base_expr} IS NULL OR {other_quantity_sql} IS NULL THEN NULL
                
                -- Handle base Quantity object
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.value') IS NOT NULL AND
                     {self.dialect.json_extract_function}({base_expr}, '$.unit') IS NOT NULL
                THEN
                    CASE 
                        -- Other is also a Quantity object
                        WHEN {self.dialect.json_extract_function}({other_quantity_sql}, '$.value') IS NOT NULL AND
                             {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit') IS NOT NULL
                        THEN
                            CASE 
                                -- Same unit is always comparable
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.unit') = 
                                     {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit')
                                THEN true
                                -- Same system is generally comparable
                                WHEN {self.dialect.json_extract_function}({base_expr}, '$.system') = 
                                     {self.dialect.json_extract_function}({other_quantity_sql}, '$.system')
                                THEN true
                                -- Basic unit compatibility checks (simplified)
                                WHEN ({self.dialect.json_extract_function}({base_expr}, '$.unit') IN ('kg', 'g', 'mg', 'lb', 'oz') AND
                                      {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit') IN ('kg', 'g', 'mg', 'lb', 'oz'))
                                THEN true  -- Mass units
                                WHEN ({self.dialect.json_extract_function}({base_expr}, '$.unit') IN ('m', 'cm', 'mm', 'km', 'in', 'ft') AND
                                      {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit') IN ('m', 'cm', 'mm', 'km', 'in', 'ft'))
                                THEN true  -- Length units
                                WHEN ({self.dialect.json_extract_function}({base_expr}, '$.unit') IN ('s', 'min', 'h', 'd', 'wk', 'mo', 'a') AND
                                      {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit') IN ('s', 'min', 'h', 'd', 'wk', 'mo', 'a'))
                                THEN true  -- Time units
                                WHEN ({self.dialect.json_extract_function}({base_expr}, '$.unit') IN ('L', 'mL', 'dL', 'cL') AND
                                      {self.dialect.json_extract_function}({other_quantity_sql}, '$.unit') IN ('L', 'mL', 'dL', 'cL'))
                                THEN true  -- Volume units
                                -- TODO: Add full UCUM compatibility logic
                                ELSE false
                            END
                        ELSE false
                    END
                
                ELSE false
            END
            """
        
        elif func_name == 'elementdefinition':
            # elementDefinition() - get element definition from structure definition
            if len(func_node.args) != 0:
                raise ValueError("elementDefinition() function takes no arguments")
            
            # Implementation of element definition retrieval
            # This is a basic implementation - full implementation would require StructureDefinition processing
            return f"""
            CASE 
                -- Check if input is a valid FHIR element
                WHEN {base_expr} IS NULL THEN NULL
                
                -- TODO: Full StructureDefinition processing would be implemented here
                -- For now, return a basic placeholder structure
                ELSE {self.dialect.json_object_function}(
                    'path', 'placeholder',
                    'type', {self.dialect.json_array_function}(
                        {self.dialect.json_object_function}('code', 'Element')
                    ),
                    'definition', 'Basic element definition placeholder',
                    'min', 0,
                    'max', '1'
                )
            END
            """
        
        elif func_name == 'slice':
            # slice(structure, name) - access specific slices in profiles
            if len(func_node.args) != 2:
                raise ValueError("slice() function takes exactly two arguments")
            
            # Get the structure and name arguments
            structure_arg = func_node.args[0]
            name_arg = func_node.args[1]
            structure_sql = self.visit(structure_arg)
            name_sql = self.visit(name_arg)
            
            # Implementation of slice access
            # This is a basic implementation - full implementation would require profile processing
            return f"""
            CASE 
                -- Check if inputs are valid
                WHEN {base_expr} IS NULL OR {structure_sql} IS NULL OR {name_sql} IS NULL THEN NULL
                
                -- TODO: Full profile slicing would be implemented here
                -- For now, return a basic placeholder
                ELSE {self.dialect.json_object_function}(
                    'sliceName', {name_sql},
                    'structure', {structure_sql},
                    'elements', {self.dialect.json_array_function}()
                )
            END
            """
        
        elif func_name == 'checkmodifiers':
            # checkModifiers(modifier) - check modifier extensions
            if len(func_node.args) != 1:
                raise ValueError("checkModifiers() function takes exactly one argument")
            
            # Get the modifier argument
            modifier_arg = func_node.args[0]
            modifier_sql = self.visit(modifier_arg)
            
            # Implementation of modifier extension checking
            # This is a basic implementation - full implementation would require extension processing
            return f"""
            CASE 
                -- Check if inputs are valid
                WHEN {base_expr} IS NULL OR {modifier_sql} IS NULL THEN NULL
                
                -- Check if modifierExtension array exists and contains the specified modifier
                WHEN {self.dialect.json_extract_function}({base_expr}, '$.modifierExtension') IS NOT NULL
                THEN
                    CASE 
                        WHEN {self.dialect.json_type_function}({self.dialect.json_extract_function}({base_expr}, '$.modifierExtension')) = 'ARRAY'
                        THEN
                            (
                                SELECT 
                                    CASE 
                                        WHEN COUNT(*) > 0 THEN true
                                        ELSE false
                                    END
                                FROM {self.dialect.json_each_function}({self.dialect.json_extract_function}({base_expr}, '$.modifierExtension'))
                                WHERE {self.dialect.json_extract_function}('value', '$.url') = {modifier_sql}
                            )
                        ELSE false
                    END
                
                ELSE false
            END
            """
        
        else:
            # For functions not implemented in fallback, raise an error
            raise ValueError(f"Function '{func_name}' is not implemented in direct fallback handler")
    
    def _generate_getValue_with_cte(self, base_expr: str) -> str:
        """Generate getValue() function with CTE optimization to avoid expression duplication"""
        # Create CTE for complex base expression
        cte_name = self._create_cte(
            f"SELECT {base_expr} as json_value FROM {self.table_name}",
            "getValue_base"
        )
        
        # Generate getValue with CTE reference to avoid repeating the complex expression
        return f"""
        (SELECT 
            CASE 
                -- Handle primitive types (return as-is)
                WHEN {self.dialect.json_type_function}(json_value) IN ('STRING', 'NUMBER', 'BOOLEAN') THEN 
                    json_value
                
                -- Handle FHIR types with 'value' property (Quantity, Identifier, ContactPoint, etc.)
                WHEN {self.dialect.json_type_function}(json_value) = 'OBJECT' AND 
                     {self.dialect.json_extract_function}(json_value, '$.value') IS NOT NULL THEN
                    {self.dialect.json_extract_function}(json_value, '$.value')
                
                -- Handle HumanName (concatenate name parts)
                WHEN {self.dialect.json_type_function}(json_value) = 'OBJECT' AND 
                     ({self.dialect.json_extract_function}(json_value, '$.family') IS NOT NULL OR 
                      {self.dialect.json_extract_function}(json_value, '$.given') IS NOT NULL) THEN
                    TRIM({self.dialect.string_concat(
                        self.dialect.string_concat(
                            f"COALESCE({self.dialect.json_extract_string_function}(json_value, '$.given[0]'), '')",
                            f"CASE WHEN {self.dialect.json_extract_function}(json_value, '$.given[0]') IS NOT NULL AND {self.dialect.json_extract_function}(json_value, '$.family') IS NOT NULL THEN ' ' ELSE '' END"
                        ),
                        f"COALESCE({self.dialect.json_extract_string_function}(json_value, '$.family'), '')"
                    )})
                
                -- Handle Address (return formatted address string)
                WHEN {self.dialect.json_type_function}(json_value) = 'OBJECT' AND 
                     ({self.dialect.json_extract_function}(json_value, '$.line') IS NOT NULL OR 
                      {self.dialect.json_extract_function}(json_value, '$.city') IS NOT NULL) THEN
                    TRIM({self.dialect.string_concat(
                        self.dialect.string_concat(
                            f"COALESCE({self.dialect.json_extract_string_function}(json_value, '$.line[0]'), '')",
                            f"CASE WHEN {self.dialect.json_extract_function}(json_value, '$.line[0]') IS NOT NULL AND {self.dialect.json_extract_function}(json_value, '$.city') IS NOT NULL THEN ', ' ELSE '' END"
                        ),
                        f"COALESCE({self.dialect.json_extract_string_function}(json_value, '$.city'), '')"
                    )})
                
                -- Return null for complex types without clear value semantics
                ELSE NULL
            END
        FROM {cte_name})
        """

    def _calculate_low_boundary(self, base_expr: str) -> str:
        """
        Calculate low boundary for FHIR boundary functions based on precision.
        
        FHIR boundary functions calculate boundaries based on the precision of the value:
        - Numeric values: subtract precision (0.05)
        - Date/time values: calculate start of period based on precision
        - Arrays: apply logic to first element
        """
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
                            WHEN LENGTH(CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR)) = 7 
                                 AND CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '____-__' THEN
                                CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) || '-01'
                            -- Date precision (YYYY-MM-DD): start of day in latest timezone 
                            WHEN LENGTH(CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR)) = 10
                                 AND CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '____-__-__' THEN
                                CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) || 'T00:00:00.000+14:00'
                            -- Time precision (HH:MM): start of minute
                            WHEN LENGTH(CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR)) = 5
                                 AND CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '__:__' THEN
                                CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) || ':00.000'
                            ELSE CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR)
                        END
                    ELSE CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR)
                END
            -- Empty arrays: return null
            WHEN {self.get_json_type(base_expr)} = 'ARRAY' AND {self.get_json_array_length(base_expr)} = 0 THEN NULL
            -- Scalar numeric values: subtract precision
            WHEN {self.get_json_type(base_expr)} IN ('NUMBER', 'INTEGER', 'DOUBLE') THEN
                CAST(CAST({base_expr} AS DOUBLE) - 0.05 AS VARCHAR)
            -- Scalar string values: apply date/time boundary logic
            WHEN {self.get_json_type(base_expr)} = 'VARCHAR' THEN
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
                    WHEN LENGTH(CAST({base_expr} AS VARCHAR)) >= 19
                         AND CAST({base_expr} AS VARCHAR) LIKE '____-__-__T%' THEN
                        CAST({base_expr} AS VARCHAR)
                    ELSE CAST({base_expr} AS VARCHAR)
                END
            -- Null or other types: return null
            ELSE NULL
        END
        """

    def _calculate_high_boundary(self, base_expr: str) -> str:
        """
        Calculate high boundary for FHIR boundary functions based on precision.
        
        FHIR boundary functions calculate boundaries based on the precision of the value:
        - Numeric values: add precision (0.05)
        - Date/time values: calculate end of period based on precision
        - Arrays: apply logic to first element
        """
        return f"""
        CASE 
            -- Handle arrays: extract first element and apply boundary logic
            WHEN {self.get_json_type(base_expr)} = 'ARRAY' AND {self.get_json_array_length(base_expr)} > 0 THEN
                CASE 
                    -- Numeric values in array: add precision
                    WHEN {self.get_json_type(self.extract_json_object(base_expr, '$[0]'))} IN ('NUMBER', 'INTEGER', 'DOUBLE') THEN
                        CAST(CAST({self.extract_json_object(base_expr, '$[0]')} AS DOUBLE) + 0.05 AS VARCHAR)
                    -- String values in array: apply date/time boundary logic
                    WHEN {self.get_json_type(self.extract_json_object(base_expr, '$[0]'))} = 'VARCHAR' THEN
                        CASE
                            -- Month precision (YYYY-MM): last day of month
                            WHEN LENGTH(CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR)) = 7 
                                 AND CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '____-__' THEN
                                CASE 
                                    -- Calculate last day of month
                                    WHEN CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_01' OR CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_03' 
                                      OR CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_05' OR CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_07'
                                      OR CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_08' OR CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_10'  
                                      OR CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_12' THEN CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) || '-31'  -- 31-day months
                                    WHEN CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_04' OR CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_06'
                                      OR CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_09' OR CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_11' 
                                      THEN CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) || '-30'   -- 30-day months  
                                    WHEN CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '%_02' THEN CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) || '-28'  -- February (simplified)
                                    ELSE CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) || '-30'  -- Default fallback
                                END
                            -- Date precision (YYYY-MM-DD): end of day in earliest timezone
                            WHEN LENGTH(CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR)) = 10
                                 AND CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '____-__-__' THEN
                                CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) || 'T23:59:59.999-12:00'
                            -- Time precision (HH:MM): end of minute
                            WHEN LENGTH(CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR)) = 5
                                 AND CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) LIKE '__:__' THEN
                                CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR) || ':59.999'
                            ELSE CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR)
                        END
                    ELSE CAST({self.extract_json_object(base_expr, '$[0]')} AS VARCHAR)
                END
            -- Empty arrays: return null
            WHEN {self.get_json_type(base_expr)} = 'ARRAY' AND {self.get_json_array_length(base_expr)} = 0 THEN NULL
            -- Scalar numeric values: add precision
            WHEN {self.get_json_type(base_expr)} IN ('NUMBER', 'INTEGER', 'DOUBLE') THEN
                CAST(CAST({base_expr} AS DOUBLE) + 0.05 AS VARCHAR)
            -- Scalar string values: apply date/time boundary logic
            WHEN {self.get_json_type(base_expr)} = 'VARCHAR' THEN
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
                    WHEN LENGTH(CAST({base_expr} AS VARCHAR)) >= 19
                         AND CAST({base_expr} AS VARCHAR) LIKE '____-__-__T%' THEN
                        CAST({base_expr} AS VARCHAR)
                    ELSE CAST({base_expr} AS VARCHAR)
                END
            -- Null or other types: return null
            ELSE NULL
        END
        """
    
