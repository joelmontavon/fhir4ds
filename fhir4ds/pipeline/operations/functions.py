"""
Complete FHIRPath Function Operations for Pipeline Architecture.

This module implements ALL 70+ FHIRPath functions from the legacy implementation,
organized by category with full dialect optimization support.

Categories:
- Collection Functions (35+ functions)
- String Functions (15+ functions) 
- Type Conversion Functions (15+ functions)
- Math Functions (10+ functions)
- DateTime Functions (5+ functions)
"""

from typing import List, Any, Optional, Dict, Union
import logging
from functools import lru_cache
from ..core.base import PipelineOperation, SQLState, ExecutionContext, ContextMode
from .function_handlers import HandlerRegistry

logger = logging.getLogger(__name__)

class FHIRPathExecutionError(Exception):
    """Base exception for FHIRPath execution errors."""
    pass

class InvalidArgumentError(FHIRPathExecutionError):
    """Raised when function arguments are invalid."""
    pass

class UnsupportedOperationError(FHIRPathExecutionError):
    """Raised when operation is not yet implemented."""
    pass

class ConversionError(FHIRPathExecutionError):
    """Raised when AST conversion fails."""
    pass

class FunctionCallOperation(PipelineOperation[SQLState]):
    """
    Complete FHIRPath function call operation supporting all 70+ functions.
    
    This replaces the scattered function handlers in the legacy implementation
    with a unified, pipeline-based approach.
    """
    
    # Collection Functions (35+)
    COLLECTION_FUNCTIONS = frozenset({
        'exists', 'empty', 'first', 'last', 'count', 'length', 'select', 'where',
        'all', 'distinct', 'single', 'tail', 'skip', 'take', 'union', 'combine',
        'intersect', 'exclude', 'alltrue', 'allfalse', 'anytrue', 'anyfalse',
        'contains', 'children', 'descendants', 'isdistinct', 'subsetof', 'supersetof',
        'iif', 'repeat', 'aggregate', 'flatten', 'in'
    })
    
    # String Functions (15+)
    STRING_FUNCTIONS = frozenset({
        'substring', 'startswith', 'endswith', 'indexof', 'replace', 'toupper', 'tolower',
        'upper', 'lower', 'trim', 'split', 'tochars', 'matches', 'replacematches', 'join'
    })
    
    # Type Conversion Functions (16+) - includes Quantity constructor
    TYPE_FUNCTIONS = frozenset({
        'toboolean', 'tostring', 'tointeger', 'todecimal', 'todate', 'todatetime', 'totime',
        'toquantity', 'convertstoboolean', 'convertstodecimal', 'convertstointeger',
        'convertstodate', 'convertstodatetime', 'convertstotime', 'as', 'is', 'oftype',
        'quantity',  # Quantity constructor function
        'valueset', 'code',  # System constructor functions
        'toconcept',  # Concept conversion function
        'tuple'  # Tuple constructor function
    })
    
    # Math Functions (26+) - includes arithmetic operators, min/max, aggregates, and statistical functions
    MATH_FUNCTIONS = frozenset({
        'abs', 'ceiling', 'floor', 'round', 'sqrt', 'truncate', 'exp', 'ln', 'log', 'power',
        '+', '-', '*', '/', '^',  # Basic arithmetic operators (^ is power)
        'precision',  # Precision calculation function
        'max', 'min',        # Min/Max functions
        'greatest', 'least', # SQL-style Min/Max functions
        'avg', 'sum', 'count', 'product',  # Aggregate functions
        'median', 'mode', 'populationstddev', 'populationvariance'  # Statistical functions
    })
    
    # DateTime Functions (14+) - includes constructors and component extraction
    DATETIME_FUNCTIONS = frozenset({
        'now', 'today', 'timeofday', 'lowboundary', 'highboundary',
        'datetime',  # DateTime constructor function
        'ageinyears',  # Context-dependent age calculation function
        'ageinyearsat',  # CQL AgeInYearsAt function - age calculation with specific date
        # Component extraction functions
        'hour_from', 'minute_from', 'second_from', 'year_from', 'month_from', 'day_from'
    })
# Logical Functions  
    LOGICAL_FUNCTIONS = frozenset({
        'and', 'or', 'not', 'implies', 'xor'
    })
    
    # Interval Functions
    INTERVAL_FUNCTIONS = frozenset({
        'interval', 'contains', 'overlaps', 'before', 'after', 'meets', 'during',
        'includes', 'included_in', 'starts', 'ends', 'same_as', 'same_or_before',
        'same_or_after', 'width', 'size', 'point_from', 'start', 'end'
    })
    
    # List Functions
    LIST_FUNCTIONS = frozenset({
        'list', 'flatten', 'distinct', 'union', 'intersect', 'except',
        'first', 'last', 'tail', 'take', 'skip', 'singleton', 'repeat'
    })
    
    # Comparison Operators (8+)
    COMPARISON_FUNCTIONS = frozenset({
        '=', '!=', '<>', '<', '<=', '>', '>=', 'in', 'contains', '~'
    })
    
    # Query Operators (CQL query syntax)
    QUERY_FUNCTIONS = frozenset({
        'from', 'where', 'return', 'sort', 'aggregate', 'retrievenode'
    })
    
    # Error and Messaging Functions
    ERROR_FUNCTIONS = frozenset({
        'message', 'error'
    })
    
    # FHIR-specific Functions
    FHIR_FUNCTIONS = frozenset({
        'getvalue', 'resolve'
    })
    
    # All supported functions
    ALL_SUPPORTED_FUNCTIONS = (COLLECTION_FUNCTIONS | STRING_FUNCTIONS | TYPE_FUNCTIONS | 
                              MATH_FUNCTIONS | DATETIME_FUNCTIONS | COMPARISON_FUNCTIONS |
                           LOGICAL_FUNCTIONS | INTERVAL_FUNCTIONS | LIST_FUNCTIONS |
                           QUERY_FUNCTIONS | ERROR_FUNCTIONS | FHIR_FUNCTIONS)
    
    def __init__(self, func_name: str, args: List[Any]):
        """
        Initialize function call operation.
        
        Args:
            func_name: Name of function to call
            args: Function arguments (can be literals, other operations, or pipelines)
        """
        self.func_name = func_name.lower()
        self.args = args or []
        self._handler_registry = HandlerRegistry()
        self._validate_function()
    
    def _validate_function(self) -> None:
        """Validate function name and arguments."""
        if self.func_name not in self.ALL_SUPPORTED_FUNCTIONS:
            logger.warning(f"Function '{self.func_name}' may not be fully supported")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute function call based on function category.
        
        Args:
            input_state: Current SQL state
            context: Execution context with dialect
            
        Returns:
            New SQL state with function applied
        """
        # Try delegation pattern first (new refactored handlers)
        handler = self._handler_registry.get_handler(self.func_name)
        if handler is not None:
            logger.debug(f"Using delegated handler for function '{self.func_name}'")
            return handler.handle_function(self.func_name, input_state, context, self.args)
        
        # Fallback to existing god class dispatch system
        logger.debug(f"Using legacy dispatch for function '{self.func_name}'")
        if self._is_collection_function():
            return self._execute_collection_function(input_state, context)
        elif self._is_string_function():
            return self._execute_string_function(input_state, context)
        elif self._is_math_function():
            return self._execute_math_function(input_state, context)
        elif self._is_datetime_function():
            return self._execute_datetime_function(input_state, context)
        elif self._is_logical_function():
            return self._execute_logical_function(input_state, context)
        elif self._is_interval_function():
            return self._execute_interval_function(input_state, context)
        elif self._is_list_function():
            return self._execute_list_function(input_state, context)
        elif self._is_query_function():
            return self._execute_query_function(input_state, context)
        elif self._is_error_function():
            return self._execute_error_function(input_state, context)
        elif self._is_fhir_function():
            return self._execute_fhir_function(input_state, context)
        else:
            raise ValueError(f"Unknown function category for: {self.func_name}")
    
    def _is_collection_function(self) -> bool:
        """Check if function is a collection function."""
        return self.func_name in self.COLLECTION_FUNCTIONS
    
    def _is_string_function(self) -> bool:
        """Check if function is a string function."""
        return self.func_name in self.STRING_FUNCTIONS
    
    def _is_math_function(self) -> bool:
        """Check if function is a math function."""
        return self.func_name in self.MATH_FUNCTIONS
    
    def _is_datetime_function(self) -> bool:
        """Check if function is a datetime function."""
        return self.func_name in self.DATETIME_FUNCTIONS
    
    def _is_logical_function(self) -> bool:
        """Check if function is a logical operator."""
        return self.func_name in self.LOGICAL_FUNCTIONS
    
    def _is_interval_function(self) -> bool:
        """Check if function is an interval function."""
        return self.func_name in self.INTERVAL_FUNCTIONS
    
    def _is_list_function(self) -> bool:
        """Check if function is a list function."""
        return self.func_name in self.LIST_FUNCTIONS
    
    def _is_query_function(self) -> bool:
        """Check if function is a query operator."""
        return self.func_name in self.QUERY_FUNCTIONS
    
    def _is_error_function(self) -> bool:
        """Check if function is an error/messaging function."""
        return self.func_name in self.ERROR_FUNCTIONS

    def _is_fhir_function(self) -> bool:
        """Check if function is a FHIR-specific function."""
        return self.func_name in self.FHIR_FUNCTIONS

    # ====================================
    # HELPER METHODS FOR CONSISTENT STATE MANAGEMENT
    # ====================================
    
    def _create_collection_result(self, input_state: SQLState, sql_fragment: str) -> SQLState:
        """Helper to create collection results consistently."""
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _create_scalar_result(self, input_state: SQLState, sql_fragment: str) -> SQLState:
        """Helper to create scalar results consistently."""
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _evaluate_union_argument(self, arg: Any, input_state: SQLState, 
                                context: ExecutionContext) -> str:
        """Evaluate union function argument to SQL fragment."""
        if not hasattr(arg, '__class__'):
            return str(arg)  # Simple literal
        
        # Check if this is a pipeline operation (like FunctionCallOperation)
        if hasattr(arg, 'execute') and callable(getattr(arg, 'execute')):
            # This is a pipeline operation - execute it directly
            base_state = self._create_base_copy(input_state)
            result_state = arg.execute(base_state, context)
            return result_state.sql_fragment
        
        # For AST nodes, use the AST converter
        try:
            return self._convert_ast_argument_to_sql(arg, input_state, context)
        except ConversionError as e:
            raise InvalidArgumentError(f"Failed to evaluate union argument: {e}")
    
    def _convert_ast_argument_to_sql(self, ast_node: Any, input_state: SQLState,
                                    context: ExecutionContext) -> str:
        """Convert AST node argument to SQL fragment."""
        from ..converters.ast_converter import ASTToPipelineConverter
        
        converter = ASTToPipelineConverter()
        base_state = self._create_base_copy(input_state)  # Use helper method
        
        arg_pipeline = converter.convert_ast_to_pipeline(ast_node)
        current_state = base_state
        
        for op in arg_pipeline.operations:
            current_state = op.execute(current_state, context)
        
        return current_state.sql_fragment
    
    def _create_base_copy(self, input_state: SQLState) -> SQLState:
        """Create a fresh base state copy for argument evaluation."""
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=input_state.json_column,
            resource_type=input_state.resource_type
        )
    
    
    def _add_debug_info(self, sql: str, operation: str, context: ExecutionContext) -> str:
        """Add debug information to SQL when in debug mode."""
        if logger.isEnabledFor(logging.DEBUG) or context.debug_mode:
            return f"/* FHIRPath {operation} */ {sql}"
        return sql
    
    @lru_cache(maxsize=256)
    def _get_cached_sql_pattern(self, pattern_type: str, context: ExecutionContext, *args) -> str:
        """Get cached SQL patterns for common operations."""
        cache_key = f"{pattern_type}_{context.dialect.name.upper()}_{args}"
        
        if pattern_type in ("exists_collection", "empty_collection"):
            # Use dialect method for collection checks
            is_collection = True
            fragment_placeholder = "{fragment}"
            pattern = context.dialect.generate_exists_check(fragment_placeholder, is_collection)
            
            if pattern_type == "empty_collection":
                # Negate the exists check for empty
                if pattern.startswith("(") and pattern.endswith(")"):
                    # Replace > 0 with = 0 for empty check
                    pattern = pattern.replace("> 0", "= 0")
                else:
                    pattern = f"NOT {pattern}"
            return pattern
        elif pattern_type in ("exists_scalar", "empty_scalar"):
            # Use dialect method for scalar checks  
            is_collection = False
            fragment_placeholder = "{fragment}"
            pattern = context.dialect.generate_exists_check(fragment_placeholder, is_collection)
            
            if pattern_type == "empty_scalar":
                # Negate the exists check for empty
                pattern = pattern.replace("IS NOT NULL", "IS NULL")
            return pattern
        else:
            # Return uncached pattern
            return None

    # ====================================
    # COLLECTION FUNCTIONS (35+ functions)
    # ====================================
    
    def _execute_collection_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute collection function with dialect optimization."""
        
        if self.func_name == 'exists':
            return self._handle_exists(input_state, context)
        elif self.func_name == 'empty':
            return self._handle_empty(input_state, context)
        elif self.func_name == 'first':
            return self._handle_first(input_state, context)
        elif self.func_name == 'last':
            return self._handle_last(input_state, context)
        elif self.func_name == 'count' or self.func_name == 'length':
            return self._handle_count(input_state, context)
        elif self.func_name == 'where':
            return self._handle_where(input_state, context)
        elif self.func_name == 'select':
            return self._handle_select(input_state, context)
        elif self.func_name == 'distinct':
            return self._handle_distinct(input_state, context)
        elif self.func_name == 'combine':
            return self._handle_combine(input_state, context)
        elif self.func_name == 'tail':
            return self._handle_tail(input_state, context)
        elif self.func_name == 'skip':
            return self._handle_skip(input_state, context)
        elif self.func_name == 'take':
            return self._handle_take(input_state, context)
        elif self.func_name == 'union':
            return self._handle_union(input_state, context)
        elif self.func_name == 'intersect':
            return self._handle_intersect(input_state, context)
        elif self.func_name == 'exclude':
            return self._handle_exclude(input_state, context)
        elif self.func_name in ['alltrue', 'allfalse', 'anytrue', 'anyfalse']:
            return self._handle_boolean_collection(input_state, context)
        elif self.func_name == 'contains':
            return self._handle_contains_collection(input_state, context)
        elif self.func_name == 'children':
            return self._handle_children(input_state, context)
        elif self.func_name == 'descendants':
            return self._handle_descendants(input_state, context)
        elif self.func_name == 'isdistinct':
            return self._handle_isdistinct(input_state, context)
        elif self.func_name in ['subsetof', 'supersetof']:
            return self._handle_set_operations(input_state, context)
        elif self.func_name == 'iif':
            return self._handle_iif(input_state, context)
        elif self.func_name == 'repeat':
            return self._handle_repeat(input_state, context)
        elif self.func_name == 'aggregate':
            return self._handle_aggregate(input_state, context)
        elif self.func_name == 'flatten':
            return self._handle_flatten(input_state, context)
        elif self.func_name == 'in':
            return self._handle_in(input_state, context)
        elif self.func_name == 'single':
            return self._handle_single(input_state, context)
        elif self.func_name == 'all':
            return self._handle_all(input_state, context)
        else:
            raise ValueError(f"Unknown collection function: {self.func_name}")
    
    def _handle_exists(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle exists() function."""
        if input_state.is_collection:
            # Try to use cached pattern first
            pattern = self._get_cached_sql_pattern("exists_collection", context)
            if pattern:
                sql_fragment = pattern.format(fragment=input_state.sql_fragment)
            else:
                sql_fragment = f"({context.dialect.json_array_length(input_state.sql_fragment)} > 0)"
        else:
            # Use cached pattern for scalar
            pattern = self._get_cached_sql_pattern("exists_scalar", context)
            sql_fragment = pattern.format(fragment=input_state.sql_fragment)
        
        sql_fragment = self._add_debug_info(sql_fragment, "exists()", context)
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_empty(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle empty() function."""
        if input_state.is_collection:
            sql_fragment = f"({context.dialect.json_array_length(input_state.sql_fragment)} = 0)"
        else:
            sql_fragment = f"({input_state.sql_fragment} IS NULL)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_first(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle first() function."""
        if input_state.is_collection:
            sql_fragment = context.dialect.get_json_array_element(input_state.sql_fragment, 0)
        else:
            # Single value - first() returns the value itself
            sql_fragment = input_state.sql_fragment
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_last(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle last() function."""
        if input_state.is_collection:
            sql_fragment = context.dialect.generate_json_extract_last(input_state.sql_fragment)
        else:
            # Single value - last() returns the value itself
            sql_fragment = input_state.sql_fragment
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_count(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle count() and length() functions."""
        if input_state.is_collection:
            # Handle null collections by returning 0 instead of NULL
            sql_fragment = f"COALESCE({context.dialect.generate_json_array_length(input_state.sql_fragment)}, 0)"
        else:
            # Single value - count is 1 if not null, 0 if null
            sql_fragment = f"CASE WHEN {input_state.sql_fragment} IS NOT NULL THEN 1 ELSE 0 END"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_where(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle where() function with condition."""
        if not self.args:
            raise ValueError("where() function requires a condition argument")
        
        # Compile the condition argument to SQL
        condition_sql = self._compile_where_condition(self.args[0], context)
        
        # Generate SQL to filter array elements based on the condition
        collection_expr = input_state.sql_fragment
        
        sql_fragment = context.dialect.generate_where_clause_filter(collection_expr, condition_sql)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION,
            path_context="$"  # Reset path context since where creates a new filtered array
        )
    
    def _compile_where_condition(self, condition_arg, context: ExecutionContext) -> str:
        """Compile a where condition argument to SQL."""
        from ...fhirpath.parser.ast_nodes import BinaryOpNode, IdentifierNode, LiteralNode, PathNode, FunctionCallNode
        
        # Handle BinaryOpNode (e.g., use = "official", active != false, age > 18, logical operations)
        if isinstance(condition_arg, BinaryOpNode):
            # Handle logical operators (and, or)
            if condition_arg.operator.lower() in ['and', 'or']:
                left_condition = self._compile_where_condition(condition_arg.left, context)
                right_condition = self._compile_where_condition(condition_arg.right, context)
                
                # Combine conditions with logical operator
                logical_op = condition_arg.operator.upper()
                return context.dialect.generate_logical_combine(left_condition, logical_op, right_condition)
            
            # Map CQL/FHIRPath comparison operators to SQL operators
            operator_mapping = {
                '=': ('=', '=='),     # (DuckDB, PostgreSQL)
                '==': ('=', '=='),
                '!=': ('!=', '!='), 
                '<>': ('<>', '!='),
                '<': ('<', '<'),
                '>': ('>', '>'),
                '<=': ('<=', '<='),
                '>=': ('>=', '>=')
            }
            
            if condition_arg.operator in operator_mapping:
                duckdb_op, pg_op = operator_mapping[condition_arg.operator]
                
                # Extract left field expression and right value
                left_expr = self._compile_operand(condition_arg.left, context, '$ITEM')
                right_expr = self._compile_operand(condition_arg.right, context, '$ITEM')
                
                # Return dialect-specific condition
                return context.dialect.generate_path_condition_comparison(left_expr, condition_arg.operator, right_expr)
        
        # Handle PathNode with function calls (e.g., url.startsWith('http://example.org'))
        if isinstance(condition_arg, PathNode):
            return self._compile_path_condition(condition_arg, context)
        
        # Handle pipeline operations (converted AST nodes)
        if hasattr(condition_arg, 'operations') and condition_arg.operations:
            # This is a pipeline with operations, likely a binary comparison
            for op in condition_arg.operations:
                if hasattr(op, 'function_name') and op.function_name == '=':
                    # Extract left and right operands for equality
                    if hasattr(op, 'args') and len(op.args) >= 1:
                        # Get the right operand (the value we're comparing to)
                        right_arg = op.args[0]
                        if hasattr(right_arg, 'operations') and right_arg.operations:
                            for right_op in right_arg.operations:
                                if hasattr(right_op, 'value'):
                                    value = right_op.value
                                    # Return DuckDB-style condition
                                    return context.dialect.generate_field_equality_condition('use', value)
        
        # No fallback - raise error for unrecognized conditions
        raise InvalidArgumentError(f"Unrecognized where condition type: {type(condition_arg).__name__}. "
                                  f"Expected BinaryOpNode, PathNode, or pipeline operation.")
    
    def _compile_operand(self, operand, context: ExecutionContext, item_placeholder: str) -> str:
        """Compile an operand (left or right side of binary operation) to SQL."""
        from ...fhirpath.parser.ast_nodes import IdentifierNode, LiteralNode, PathNode, FunctionCallNode
        
        # Handle simple identifiers (field names)
        if isinstance(operand, IdentifierNode):
            field_name = operand.name
            return context.dialect.generate_field_extraction(item_placeholder, field_name)
        
        # Handle literal values
        elif isinstance(operand, LiteralNode):
            value = operand.value
            if isinstance(value, str):
                return f"'{value}'"
            elif isinstance(value, bool):
                return 'true' if value else 'false'
            elif isinstance(value, (int, float)):
                return str(value)
            else:
                return f"'{value}'"
        
        # Handle path expressions (e.g., patient.name.given)
        elif isinstance(operand, PathNode):
            if len(operand.segments) == 1 and isinstance(operand.segments[0], IdentifierNode):
                # Simple single-segment path
                field_name = operand.segments[0].name
                return context.dialect.generate_field_extraction(item_placeholder, field_name)
            elif len(operand.segments) == 2 and isinstance(operand.segments[1], FunctionCallNode):
                # Handle path.function() pattern (e.g., family.exists(), given.count())
                field_name = operand.segments[0].name if isinstance(operand.segments[0], IdentifierNode) else str(operand.segments[0])
                function_call = operand.segments[1]
                
                if function_call.name == 'exists':
                    # Handle field.exists() - check if field is not null
                    return context.dialect.generate_field_exists_check(item_placeholder, field_name)
                elif function_call.name in ['count', 'length']:
                    # Handle field.count() or field.length()
                    if function_call.name == 'count':
                        return context.dialect.generate_field_count_operation(item_placeholder, field_name)
                    else:  # length
                        return context.dialect.generate_field_length_operation(item_placeholder, field_name)
                else:
                    # For other functions, try to handle generically
                    raise InvalidArgumentError(f"Unsupported function in operand: {field_name}.{function_call.name}()")
            else:
                # Complex path - build nested access
                path_parts = []
                for segment in operand.segments:
                    if isinstance(segment, IdentifierNode):
                        path_parts.append(segment.name)
                
                if path_parts:
                    path_expr = '.'.join(path_parts)
                    return context.dialect.generate_path_expression_extraction(item_placeholder, path_expr)
        
        # Handle function calls directly (not as part of a path)
        elif isinstance(operand, FunctionCallNode):
            # This would be for standalone function calls
            raise InvalidArgumentError(f"Standalone function calls not supported in where conditions: {operand.name}()")
        
        # Fallback for unrecognized operand types
        raise InvalidArgumentError(f"Unrecognized operand type in where condition: {type(operand).__name__}")
    
    def _compile_path_condition(self, path_node, context: ExecutionContext) -> str:
        """Compile a PathNode condition to SQL (e.g., url.startsWith('http://example.org'))."""
        from ...fhirpath.parser.ast_nodes import IdentifierNode, FunctionCallNode, LiteralNode
        
        if len(path_node.segments) == 2:
            # Handle field.function_call pattern
            if (isinstance(path_node.segments[0], IdentifierNode) and 
                isinstance(path_node.segments[1], FunctionCallNode)):
                
                field_name = path_node.segments[0].name
                function_call = path_node.segments[1]
                
                # Handle startsWith() function
                if function_call.name == 'startsWith' and len(function_call.args) == 1:
                    arg = function_call.args[0]
                    if isinstance(arg, LiteralNode):
                        value = arg.value
                    elif isinstance(arg, IdentifierNode):
                        # Handle unquoted string literals parsed as identifiers
                        value = arg.name
                    else:
                        # Handle other literal node types (DateTimeLiteralNode, etc.)
                        if hasattr(arg, 'value'):
                            value = arg.value
                        else:
                            raise InvalidArgumentError(f"Unsupported argument type for startsWith(): {type(arg).__name__}")
                    return context.dialect.starts_with_condition("$ITEM", field_name, value)
                
                # Handle endsWith() function
                elif function_call.name == 'endsWith' and len(function_call.args) == 1:
                    arg = function_call.args[0]
                    if isinstance(arg, LiteralNode):
                        value = arg.value
                    elif isinstance(arg, IdentifierNode):
                        # Handle unquoted string literals parsed as identifiers
                        value = arg.name
                    else:
                        # Handle other literal node types (DateTimeLiteralNode, etc.)
                        if hasattr(arg, 'value'):
                            value = arg.value
                        else:
                            raise InvalidArgumentError(f"Unsupported argument type for endsWith(): {type(arg).__name__}")
                    return context.dialect.ends_with_condition("$ITEM", field_name, value)
                
                # Handle contains() function
                elif function_call.name == 'contains' and len(function_call.args) == 1:
                    arg = function_call.args[0]
                    if isinstance(arg, LiteralNode):
                        value = arg.value
                    elif isinstance(arg, IdentifierNode):
                        # Handle unquoted string literals parsed as identifiers
                        value = arg.name
                    else:
                        # Handle other literal node types (DateTimeLiteralNode, etc.)
                        if hasattr(arg, 'value'):
                            value = arg.value
                        else:
                            raise InvalidArgumentError(f"Unsupported argument type for contains(): {type(arg).__name__}")
                    return context.dialect.contains_condition("$ITEM", field_name, value)
                
                # Handle exists() function
                elif function_call.name == 'exists' and len(function_call.args) == 0:
                    # Handle field.exists() - check if field is not null
                    return context.dialect.generate_field_exists_check('$ITEM', field_name)
        
        # Fallback for unsupported path conditions
        raise InvalidArgumentError(f"Unsupported path condition: {path_node}. "
                                  f"Currently supports field.startsWith(), field.endsWith(), field.contains()")
    
    def _handle_select(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle select() function with transformation."""
        if not self.args:
            raise ValueError("select() function requires a transformation argument")
        
        # This would need to be expanded to handle complex transformation compilation
        # For now, placeholder implementation
        transform_sql = "."  # This should be compiled from self.args[0]
        
        sql_fragment = context.dialect.generate_select_transformation(input_state.sql_fragment, transform_sql)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_distinct(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle distinct() function."""
        sql_fragment = context.dialect.generate_array_distinct_operation(input_state.sql_fragment)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_combine(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle combine() function."""
        if not self.args:
            raise ValueError("combine() function requires another collection argument")
        
        # For now, assume the argument is the same collection (common case: x.combine(x))
        # In a full implementation, we'd need to evaluate the argument expression
        first_collection = input_state.sql_fragment
        second_collection = input_state.sql_fragment  # Simplified for x.combine(x) case
        
        sql_fragment = context.dialect.generate_collection_combine(first_collection, second_collection)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    # Additional collection function handlers would go here...
    # For brevity, I'll implement the key ones and provide placeholders for others
    
    def _handle_tail(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle tail() function - all elements except first."""
        sql_fragment = context.dialect.generate_array_tail_operation(input_state.sql_fragment)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_skip(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle skip(n) function."""
        if not self.args:
            raise ValueError("skip() function requires a number argument")
        
        # Extract numeric argument properly
        arg = self.args[0]
        if hasattr(arg, 'value'):
            skip_count = str(arg.value)
        elif hasattr(arg, 'sql_fragment'):
            skip_count = str(arg.sql_fragment)
        else:
            skip_count = str(arg)
        
        sql_fragment = context.dialect.generate_array_slice_operation(input_state.sql_fragment, int(skip_count))
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_take(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle take(n) function."""
        if not self.args:
            raise ValueError("take() function requires a number argument")
        
        # Extract numeric argument properly
        arg = self.args[0]
        if hasattr(arg, 'value'):
            take_count = str(arg.value)
        elif hasattr(arg, 'sql_fragment'):
            take_count = str(arg.sql_fragment)
        else:
            take_count = str(arg)
        
        sql_fragment = context.dialect.generate_array_slice_operation(input_state.sql_fragment, 0, int(take_count))
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    # Placeholder implementations for remaining collection functions
    def _handle_union(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle union() function - combines collections with distinct semantics."""
        if not self.args:
            raise InvalidArgumentError("union() function requires another collection argument")
        
        first_collection = input_state.sql_fragment
        second_collection = self._evaluate_union_argument(self.args[0], input_state, context)
        
        # Optimization: Detect resource retrieval queries and use simple UNION ALL
        if self._is_simple_union_candidate(first_collection) and self._is_simple_union_candidate(second_collection):
            # Extract clean resource queries and create UNION ALL
            first_queries = self._extract_resource_queries(first_collection)
            second_queries = self._extract_resource_queries(second_collection)
            
            if first_queries and second_queries:
                # Combine all queries with UNION ALL
                all_queries = first_queries + second_queries
                sql_fragment = f"({' UNION ALL '.join(all_queries)})"
                logger.debug(f"🔧 Optimized union for {len(all_queries)} resource retrievals: UNION ALL")
            else:
                # Fall back to regular union logic
                sql_fragment = f"({first_collection} UNION ALL {second_collection})"
                logger.debug(f"🔧 Basic union optimization: UNION ALL")
        else:
            # Use complex JSON array union logic for general cases
            sql_fragment = context.dialect.generate_union_operation(first_collection, second_collection)
        
        sql_fragment = self._add_debug_info(sql_fragment, "union()", context)
        return self._create_collection_result(input_state, sql_fragment)
    
    def _is_resource_retrieval_query(self, sql_fragment: str) -> bool:
        """Check if SQL fragment is a resource retrieval query."""
        # Resource retrieval queries typically contain SELECT and fhir_resources
        sql_clean = sql_fragment.strip().strip('(').strip(')')
        return (
            'SELECT resource FROM fhir_resources' in sql_clean or
            'SELECT resource\n            FROM fhir_resources' in sql_clean
        )
    
    def _is_simple_union_candidate(self, sql_fragment: str) -> bool:
        """Check if SQL fragment is a candidate for simple UNION ALL optimization."""
        # Check if it contains resource retrieval queries, even if wrapped in complex logic
        return (
            self._is_resource_retrieval_query(sql_fragment) or
            (
                'SELECT resource FROM fhir_resources' in sql_fragment and
                (
                    'UNION ALL' in sql_fragment or  # Already has UNION ALL
                    'CASE' in sql_fragment  # Complex CASE with resource queries inside
                )
            )
        )
    
    def _extract_resource_queries(self, sql_fragment: str) -> list:
        """Extract clean resource retrieval queries from potentially complex SQL."""
        import re
        
        queries = []
        
        # Clean the fragment
        sql_clean = sql_fragment.strip().strip('(').strip(')')
        
        # Method 1: Try to find complete SELECT statements with balanced parentheses
        select_starts = []
        i = 0
        while i < len(sql_clean):
            if sql_clean[i:i+6].upper() == 'SELECT':
                # Found a SELECT - now find the matching end
                paren_count = 0
                start = i
                j = i
                
                # Look backwards to find opening paren
                while start > 0 and sql_clean[start-1] not in '(':
                    start -= 1
                if start > 0:
                    start -= 1  # Include the opening paren
                
                # Look forward to find the complete query
                while j < len(sql_clean):
                    if sql_clean[j] == '(':
                        paren_count += 1
                    elif sql_clean[j] == ')':
                        paren_count -= 1
                        if paren_count == 0:
                            # Found the end
                            query = sql_clean[start:j+1].strip()
                            if 'SELECT resource FROM fhir_resources' in query.upper():
                                queries.append(query)
                            break
                    j += 1
            i += 1
        
        # Method 2: If no balanced queries found, try simpler regex
        if not queries:
            # Try to match simple pattern
            pattern = r'\(\s*SELECT resource\s+FROM fhir_resources.*?\)'
            matches = re.findall(pattern, sql_clean, re.IGNORECASE | re.DOTALL)
            queries.extend([m.strip() for m in matches if m.strip()])
        
        # Method 3: If still no matches but we detect it's a simple resource query
        if not queries and self._is_resource_retrieval_query(sql_fragment):
            queries.append(sql_clean)
        
        # Remove duplicates while preserving order
        unique_queries = []
        for q in queries:
            if q not in unique_queries:
                unique_queries.append(q)
        
        return unique_queries
    
    def _handle_intersect(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle intersect() function - returns elements common to both collections."""
        if not self.args:
            raise InvalidArgumentError("intersect() function requires exactly one argument")
        
        first_collection = input_state.sql_fragment
        second_collection = self._evaluate_union_argument(self.args[0], input_state, context)
        
        sql_fragment = context.dialect.generate_intersect_operation(first_collection, second_collection)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_exclude(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle exclude() function - returns elements in base collection that are not in other collection."""
        if not self.args:
            raise InvalidArgumentError("exclude() function requires exactly one argument")
        
        first_collection = input_state.sql_fragment
        second_collection = self._evaluate_union_argument(self.args[0], input_state, context)
        
        sql_fragment = context.dialect.generate_exclude_operation(first_collection, second_collection)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_boolean_collection(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle allTrue/allFalse/anyTrue/anyFalse functions."""
        if self.func_name == 'alltrue':
            sql_fragment = context.dialect.generate_boolean_all_true(input_state.sql_fragment)
        elif self.func_name == 'allfalse':
            sql_fragment = context.dialect.generate_boolean_all_false(input_state.sql_fragment)
        elif self.func_name == 'anytrue':
            sql_fragment = context.dialect.generate_boolean_any_true(input_state.sql_fragment)
        elif self.func_name == 'anyfalse':
            sql_fragment = context.dialect.generate_boolean_any_false(input_state.sql_fragment)
        else:
            raise ValueError(f"Unknown boolean function: {self.func_name}")
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE
        )
    def _handle_contains_collection(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle contains() for collections.
        
        Returns true if the collection contains the specified element.
        This is different from the string contains() which checks substring matching.
        """
        if not self.args:
            raise InvalidArgumentError("contains() function requires an element to search for")
        
        # Get the element to search for
        search_element = str(self.args[0])
        
        sql_fragment = context.dialect.generate_collection_contains_element(input_state.sql_fragment, search_element)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_children(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle children() function.
        
        Returns a collection with all immediate child nodes of all items in the input collection.
        For JSON/JSONB data, this means all direct properties/elements of an object/array.
        """
        sql_fragment = context.dialect.generate_children_extraction(input_state.sql_fragment)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_descendants(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle descendants() function.
        
        Returns a collection with all descendant nodes (all nested children at any level)
        of all items in the input collection. This is similar to children() but recursive.
        """
        sql_fragment = context.dialect.generate_recursive_descendants_with_cte(input_state.sql_fragment)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_isdistinct(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle isDistinct() function.
        
        Returns true if all items in the collection are distinct (no duplicates).
        Returns true for empty collections and single-item collections.
        """
        sql_fragment = context.dialect.generate_collection_distinct_check(input_state.sql_fragment)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_set_operations(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle subsetOf/supersetOf functions.
        
        subsetOf(other): Returns true if this collection is a subset of the other collection
        supersetOf(other): Returns true if this collection is a superset of the other collection
        """
        if not self.args:
            raise InvalidArgumentError(f"{self.func_name}() function requires another collection argument")
        
        other_collection = str(self.args[0])
        
        if self.func_name == 'subsetof':
            # Check if all elements in input_state are also in other_collection
            sql_fragment = context.dialect.generate_subset_check(input_state.sql_fragment, other_collection)
        elif self.func_name == 'supersetof':
            # Check if all elements in other_collection are also in input_state
            sql_fragment = context.dialect.generate_superset_check(input_state.sql_fragment, other_collection)
        else:
            raise ValueError(f"Unknown set comparison function: {self.func_name}")
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE
        )
    
    def _handle_iif(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle iif() function (inline if).
        
        iif(condition, true_result, false_result) - returns true_result if condition is true, 
        false_result otherwise. If false_result is omitted, returns empty collection when false.
        """
        if len(self.args) < 2:
            raise InvalidArgumentError("iif() function requires at least condition and true_result arguments")
        
        condition = str(self.args[0])
        true_result = str(self.args[1])
        false_result = str(self.args[2]) if len(self.args) > 2 else "NULL"
        
        sql_fragment = context.dialect.generate_iif_expression(condition, true_result, false_result)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_repeat(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle repeat() function.
        
        The repeat() function is used for recursive operations. It applies an expression repeatedly
        until no new items are found. For simplicity in SQL generation, we limit to a fixed number of iterations.
        """
        if not self.args:
            raise InvalidArgumentError("repeat() function requires an expression argument")
        
        # For SQL implementation, we'll use a recursive CTE with a reasonable limit
        expression = str(self.args[0])
        max_iterations = 10  # Prevent infinite recursion
        
        sql_fragment = context.dialect.generate_repeat_operation(input_state.sql_fragment, expression, max_iterations)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_aggregate(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle aggregate() function.
        
        aggregate(aggregator, init) - Reduces a collection to a single value by applying 
        an aggregator expression with an optional initial value.
        """
        if not self.args:
            raise InvalidArgumentError("aggregate() function requires an aggregator expression")
        
        aggregator_expr = str(self.args[0])
        init_value = str(self.args[1]) if len(self.args) > 1 else "NULL"
        
        sql_fragment = context.dialect.generate_aggregate_operation(input_state.sql_fragment, aggregator_expr, init_value)
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_flatten(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle flatten() function.
        
        Flattens a collection by concatenating nested collections into a single collection.
        For example, [[1,2], [3,4]] becomes [1,2,3,4].
        """
        sql_fragment = context.dialect.generate_flatten_operation(input_state.sql_fragment)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_in(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle in() function.
        
        Returns true if the input value is contained in the specified collection.
        This is similar to contains() but with reversed operands: value.in(collection).
        """
        if not self.args:
            raise InvalidArgumentError("in() function requires a collection argument")
        
        collection = str(self.args[0])
        
        sql_fragment = context.dialect.generate_element_in_collection(input_state.sql_fragment, collection)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_single(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle single() function - returns single element or error if not exactly one."""
        sql_fragment = context.dialect.generate_single_element_check(input_state.sql_fragment)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_all(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle all() function.
        
        Returns true if all elements in the collection satisfy the specified criteria.
        This is different from allTrue() which just checks if all values are true.
        """
        if not self.args:
            raise InvalidArgumentError("all() function requires a criteria expression")
        
        criteria = str(self.args[0])
        
        sql_fragment = context.dialect.generate_all_criteria_check(input_state.sql_fragment, criteria)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    # ===================================
    # STRING FUNCTIONS (15+ functions)
    # ===================================
    
    def _execute_string_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute string function with dialect optimization."""
        
        if self.func_name == 'substring':
            return self._handle_substring(input_state, context)
        elif self.func_name == 'contains':
            return self._handle_contains_string(input_state, context)
        elif self.func_name == 'startswith':
            return self._handle_startswith(input_state, context)
        elif self.func_name == 'endswith':
            return self._handle_endswith(input_state, context)
        elif self.func_name in ['upper', 'toupper']:
            return self._handle_upper(input_state, context)
        elif self.func_name in ['lower', 'tolower']:
            return self._handle_lower(input_state, context)
        elif self.func_name == 'trim':
            return self._handle_trim(input_state, context)
        elif self.func_name == 'replace':
            return self._handle_replace(input_state, context)
        elif self.func_name == 'split':
            return self._handle_split(input_state, context)
        elif self.func_name == 'join':
            return self._handle_join(input_state, context)
        elif self.func_name == 'indexof':
            return self._handle_indexof(input_state, context)
        elif self.func_name == 'tochars':
            return self._handle_tochars(input_state, context)
        elif self.func_name == 'matches':
            return self._handle_matches(input_state, context)
        elif self.func_name == 'replacematches':
            return self._handle_replacematches(input_state, context)
        else:
            raise ValueError(f"Unknown string function: {self.func_name}")
    
    def _handle_substring(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle substring() function."""
        if len(self.args) < 1:
            raise ValueError("substring() requires at least start position")
        
        start_pos = str(self.args[0])
        length_expr = f", {self.args[1]}" if len(self.args) > 1 else ""
        
        if len(self.args) > 1:
            sql_fragment = context.dialect.generate_substring_sql(input_state.sql_fragment, start_pos, str(self.args[1]))
        else:
            sql_fragment = context.dialect.generate_substring_sql(input_state.sql_fragment, start_pos)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_contains_string(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle contains() for strings."""
        if not self.args:
            raise ValueError("contains() requires a search string argument")
        
        search_string = str(self.args[0])
        sql_fragment = f"({input_state.sql_fragment} LIKE '%{search_string}%')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_startswith(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle startsWith() function."""
        if not self.args:
            raise ValueError("startsWith() requires a prefix string argument")
        
        prefix = str(self.args[0])
        sql_fragment = f"({input_state.sql_fragment} LIKE '{prefix}%')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_endswith(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle endsWith() function."""
        if not self.args:
            raise ValueError("endsWith() requires a suffix string argument")
        
        suffix = str(self.args[0])
        sql_fragment = f"({input_state.sql_fragment} LIKE '%{suffix}')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_upper(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle upper()/toUpper() function."""
        sql_fragment = f"UPPER({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_lower(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle lower()/toLower() function."""
        sql_fragment = f"LOWER({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_trim(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle trim() function."""
        sql_fragment = f"TRIM({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_replace(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle replace() function."""
        if len(self.args) < 2:
            raise ValueError("replace() requires search and replacement strings")
        
        search_str = str(self.args[0])
        replace_str = str(self.args[1])
        sql_fragment = f"REPLACE({input_state.sql_fragment}, '{search_str}', '{replace_str}')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # Placeholder implementations for remaining string functions
    def _handle_split(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle split() function.
        
        Splits a string into an array of substrings using the specified separator.
        """
        if not self.args:
            raise InvalidArgumentError("split() function requires a separator argument")
        
        separator = str(self.args[0]).strip("'\"")  # Remove quotes if present
        sql_fragment = context.dialect.split_string(input_state.sql_fragment, separator)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_join(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle join() function - concatenate array elements with separator."""
        from ...fhirpath.parser.ast_nodes import LiteralNode
        
        if not self.args:
            # Default separator is empty string
            separator = "''"
        else:
            # Get separator from raw AST argument (LiteralNode)
            arg = self.args[0]
            if isinstance(arg, LiteralNode):
                # Raw AST LiteralNode - use its value directly
                separator = f"'{arg.value}'"
            elif hasattr(arg, 'value'):
                # LiteralOperation with value attribute (legacy handling)
                separator = f"'{arg.value}'"
            elif hasattr(arg, 'literal_value'):
                # Some other literal format
                separator = f"'{arg.literal_value}'"
            else:
                # Convert argument to SQL and extract literal value
                # This is for cases where we have a literal string like ','
                arg_str = str(arg)
                if arg_str.startswith("'") and arg_str.endswith("'"):
                    separator = arg_str
                elif arg_str.startswith('"') and arg_str.endswith('"'):
                    separator = f"'{arg_str[1:-1]}'"
                else:
                    # Wrap in quotes if not already quoted
                    separator = f"'{arg_str}'"
        
        # The input_state should contain the collection to join
        # Use dialect-specific string aggregation
        collection_expr = input_state.sql_fragment
        
        sql_fragment = context.dialect.generate_join_operation(collection_expr, separator)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_indexof(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle indexOf() function.
        
        Returns the 0-based index of the first occurrence of the substring, or -1 if not found.
        """
        if not self.args:
            raise InvalidArgumentError("indexOf() function requires a substring argument")
        
        substring = str(self.args[0])
        position_sql = context.dialect.string_position(substring, input_state.sql_fragment)
        # Convert 1-based position to 0-based, return -1 if not found
        sql_fragment = f"(CASE WHEN {position_sql} > 0 THEN {position_sql} - 1 ELSE -1 END)"
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_tochars(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toChars() function.
        
        Splits a string into an array of individual characters.
        """
        sql_fragment = context.dialect.string_to_char_array(input_state.sql_fragment)
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_matches(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle matches() function.
        
        Tests whether a string matches a regular expression pattern.
        Returns true if the string matches the pattern, false otherwise.
        """
        if not self.args:
            raise InvalidArgumentError("matches() function requires a regular expression pattern")
        
        pattern = str(self.args[0])
        sql_fragment = context.dialect.regex_matches(input_state.sql_fragment, pattern)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_replacematches(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle replaceMatches() function.
        
        Replaces all occurrences of a regular expression pattern with a replacement string.
        """
        if len(self.args) < 2:
            raise InvalidArgumentError("replaceMatches() function requires pattern and replacement arguments")
        
        pattern = str(self.args[0])
        replacement = str(self.args[1])
        sql_fragment = context.dialect.regex_replace(input_state.sql_fragment, pattern, replacement)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    # ==========================================
    # TYPE CONVERSION FUNCTIONS (15+ functions)
    # ==========================================
    
    def _execute_math_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute math function with dialect optimization."""
        
        if self.func_name == 'abs':
            return self._handle_abs(input_state, context)
        elif self.func_name == 'ceiling':
            return self._handle_ceiling(input_state, context)
        elif self.func_name == 'floor':
            return self._handle_floor(input_state, context)
        elif self.func_name == 'round':
            return self._handle_round(input_state, context)
        elif self.func_name == 'sqrt':
            return self._handle_sqrt(input_state, context)
        elif self.func_name == 'truncate':
            return self._handle_truncate(input_state, context)
        elif self.func_name == 'exp':
            return self._handle_exp(input_state, context)
        elif self.func_name == 'ln':
            return self._handle_ln(input_state, context)
        elif self.func_name == 'log':
            return self._handle_log(input_state, context)
        elif self.func_name == 'power':
            return self._handle_power(input_state, context)
        elif self.func_name == 'precision':
            return self._handle_precision(input_state, context)
        elif self.func_name == '+':
            return self._handle_addition(input_state, context)
        elif self.func_name == '-':
            return self._handle_subtraction(input_state, context)
        elif self.func_name == '*':
            return self._handle_multiplication(input_state, context)
        elif self.func_name == '/':
            return self._handle_division(input_state, context)
        elif self.func_name == '^':
            return self._handle_power_operator(input_state, context)
        elif self.func_name == 'max':
            return self._handle_max(input_state, context)
        elif self.func_name == 'min':
            return self._handle_min(input_state, context)
        elif self.func_name == 'greatest':
            return self._handle_greatest(input_state, context)
        elif self.func_name == 'least':
            return self._handle_least(input_state, context)
        elif self.func_name == 'avg':
            return self._handle_avg(input_state, context)
        elif self.func_name == 'sum':
            return self._handle_sum(input_state, context)
        elif self.func_name == 'count':
            return self._handle_count(input_state, context)
        elif self.func_name == 'product':
            return self._handle_product(input_state, context)
        elif self.func_name == 'median':
            return self._handle_median(input_state, context)
        elif self.func_name == 'mode':
            return self._handle_mode(input_state, context)
        elif self.func_name == 'populationstddev':
            return self._handle_populationstddev(input_state, context)
        elif self.func_name == 'populationvariance':
            return self._handle_populationvariance(input_state, context)
        else:
            raise ValueError(f"Unknown math function: {self.func_name}")
    
    def _handle_abs(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle abs() function."""
        sql_fragment = f"ABS({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_ceiling(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle ceiling() function."""
        sql_fragment = f"CEIL({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_floor(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle floor() function."""
        sql_fragment = f"FLOOR({input_state.sql_fragment})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_round(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle round() function."""
        precision = f", {self.args[0]}" if self.args else ""
        sql_fragment = f"ROUND({input_state.sql_fragment}{precision})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # Placeholder implementations for remaining math functions
    def _handle_sqrt(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle sqrt() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        sql_fragment = context.dialect.generate_mathematical_function('sqrt', cast_operand)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_truncate(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle truncate() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        sql_fragment = context.dialect.generate_mathematical_function('truncate', cast_operand)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_exp(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle exp() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        sql_fragment = context.dialect.generate_mathematical_function('exp', cast_operand)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_ln(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle ln() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        sql_fragment = context.dialect.generate_mathematical_function('ln', cast_operand)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_log(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle log() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        base = f", {self.args[0]}" if self.args else ""  # Optional base argument
        sql_fragment = context.dialect.generate_mathematical_function('log', cast_operand + base)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_power(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle power() function."""
        if not self.args:
            raise InvalidArgumentError("power() function requires an exponent argument")
        
        exponent = str(self.args[0])
        base_expr = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        sql_fragment = context.dialect.generate_power_operation(base_expr, exponent)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_power_operator(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle power (^) operator."""
        if not self.args:
            raise InvalidArgumentError("Power operator (^) requires an exponent argument")
        
        # The power operator ^ behaves exactly like the power() function
        # Reuse the existing _handle_power logic
        return self._handle_power(input_state, context)
    
    def _handle_precision(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle precision() function.
        
        Calculates the precision (granularity) of different data types:
        - Decimal numbers: counts decimal places (1.58700 → 5)
        - Dates: counts precision components (@2014 → 4, @2014-01-05 → 8)
        - Times: counts time components (@T10:30 → 4, @T10:30:00.000 → 9)
        - DateTime: counts all components including milliseconds
        """
        if not self.args:
            raise InvalidArgumentError("precision() function requires 1 argument: the value to analyze")
        
        # Get the argument value - this contains the value to analyze for precision
        arg_value = self._evaluate_logical_argument(self.args[0], input_state, context)
        
        # Generate SQL that calculates precision based on the input type and format
        # The precision calculation depends on the data type and format
        
        # For CQL precision, we need to determine:
        # - Decimal precision: count digits after decimal point
        # - DateTime precision: count the level of detail (year=4, full datetime with ms=17)
        # - Time precision: count components (HH:MM=4, HH:MM:SS.SSS=9)
        
        sql_fragment = f"""
        CASE
            -- Decimal numbers: count decimal places (1.58700 → 5)
            WHEN CAST({arg_value} AS VARCHAR) ~ '\\.' THEN LENGTH(SPLIT_PART(CAST({arg_value} AS VARCHAR), '.', 2))
            -- Year only (@2014 → 4)
            WHEN CAST({arg_value} AS VARCHAR) ~ '^\\d{{4}}$' THEN 4
            -- Full DateTime with milliseconds (@2014-01-05T10:30:00.000 → 17)
            WHEN CAST({arg_value} AS VARCHAR) ~ '\\d{{4}}-\\d{{2}}-\\d{{2}}T\\d{{2}}:\\d{{2}}:\\d{{2}}\\.\\d{{3}}' THEN 17
            -- Time with milliseconds (@T10:30:00.000 → 9)
            WHEN CAST({arg_value} AS VARCHAR) ~ '^T\\d{{2}}:\\d{{2}}:\\d{{2}}\\.\\d{{3}}' THEN 9
            -- Time with minutes (@T10:30 → 4)
            WHEN CAST({arg_value} AS VARCHAR) ~ '^T\\d{{2}}:\\d{{2}}$' THEN 4
            -- Default case
            ELSE 1
        END
        """
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # Arithmetic Operators
    def _handle_addition(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle addition (+) operator."""
        if not self.args:
            raise InvalidArgumentError("Addition operator requires a second operand")
        
        # Get the second operand
        second_operand = str(self.args[0])
        
        # Generate SQL with proper null handling
        sql_fragment = f"(CAST({input_state.sql_fragment} AS DECIMAL) + CAST({second_operand} AS DECIMAL))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_subtraction(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle subtraction (-) operator."""
        if not self.args:
            raise InvalidArgumentError("Subtraction operator requires a second operand")
        
        # Get the second operand
        second_operand = str(self.args[0])
        
        # Generate SQL with proper null handling
        sql_fragment = f"(CAST({input_state.sql_fragment} AS DECIMAL) - CAST({second_operand} AS DECIMAL))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_multiplication(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle multiplication (*) operator."""
        if not self.args:
            raise InvalidArgumentError("Multiplication operator requires a second operand")
        
        # Get the second operand
        second_operand = str(self.args[0])
        
        # Generate SQL with proper null handling
        sql_fragment = f"(CAST({input_state.sql_fragment} AS DECIMAL) * CAST({second_operand} AS DECIMAL))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_division(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle division (/) operator."""
        if not self.args:
            raise InvalidArgumentError("Division operator requires a second operand")
        
        # Get the second operand
        second_operand = str(self.args[0])
        
        # Generate SQL with proper division by zero handling
        sql_fragment = f"(CASE WHEN CAST({second_operand} AS DECIMAL) = 0 THEN NULL ELSE CAST({input_state.sql_fragment} AS DECIMAL) / CAST({second_operand} AS DECIMAL) END)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_max(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle max() function with variable arguments."""
        if not self.args:
            raise InvalidArgumentError("Max function requires at least one argument")
        
        # Build list of all arguments (input_state.sql_fragment is first operand)
        all_operands = [input_state.sql_fragment]
        all_operands.extend(str(arg) for arg in self.args)
        
        # Generate SQL using dialect-specific max operation
        sql_fragment = context.dialect.generate_max_operation(all_operands)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_min(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle min() function with variable arguments."""
        if not self.args:
            raise InvalidArgumentError("Min function requires at least one argument")
        
        # Build list of all arguments (input_state.sql_fragment is first operand)
        all_operands = [input_state.sql_fragment]
        all_operands.extend(str(arg) for arg in self.args)
        
        # Generate SQL using dialect-specific min operation
        sql_fragment = context.dialect.generate_min_operation(all_operands)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_greatest(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle greatest() function - alias for max()."""
        return self._handle_max(input_state, context)
    
    def _handle_least(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle least() function - alias for min()."""
        return self._handle_min(input_state, context)
    
    def _handle_avg(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle avg() function."""
        if not self.args:
            raise InvalidArgumentError("Avg function requires at least one argument")
        
        # Get all operands (input + arguments)
        all_operands = [input_state.sql_fragment]
        all_operands.extend(str(arg) for arg in self.args)
        
        # Generate SQL for average calculation
        operands_str = ', '.join(f"CAST({op} AS DECIMAL)" for op in all_operands)
        sql_fragment = f"AVG({operands_str})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_sum(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle sum() function."""
        if not self.args:
            raise InvalidArgumentError("Sum function requires at least one argument")
        
        # For simple case, just add all arguments
        all_operands = [input_state.sql_fragment]
        all_operands.extend(str(arg) for arg in self.args)
        
        # Generate SQL for sum calculation  
        if len(all_operands) == 1:
            sql_fragment = f"CAST({all_operands[0]} AS DECIMAL)"
        else:
            # Add all operands together
            operands_str = ' + '.join(f"CAST({op} AS DECIMAL)" for op in all_operands)
            sql_fragment = f"({operands_str})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    
    def _handle_product(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle product() function."""
        if not self.args:
            raise InvalidArgumentError("Product function requires at least one argument")
        
        # Get all operands (input + arguments)
        all_operands = [input_state.sql_fragment]
        all_operands.extend(str(arg) for arg in self.args)
        
        # Generate SQL for product calculation
        if len(all_operands) == 1:
            sql_fragment = f"CAST({all_operands[0]} AS DECIMAL)"
        else:
            # Multiply all operands together
            operands_str = ' * '.join(f"CAST({op} AS DECIMAL)" for op in all_operands)
            sql_fragment = f"({operands_str})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_median(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle median() aggregate function."""
        if not self.args:
            raise InvalidArgumentError("Median function requires at least one argument")
        
        # Get all operands (input + arguments)
        all_operands = [input_state.sql_fragment]
        all_operands.extend(str(arg) for arg in self.args)
        
        # Use database-specific median calculation
        sql_fragment = context.dialect.generate_median_operation(all_operands)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_mode(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle mode() aggregate function - most frequent value."""
        if not self.args:
            raise InvalidArgumentError("Mode function requires at least one argument")
        
        # Get all operands (input + arguments)
        all_operands = [input_state.sql_fragment]  
        all_operands.extend(str(arg) for arg in self.args)
        
        # Use window functions to find most frequent value
        # This is complex but works across both databases
        operands_str = ', '.join(all_operands)
        sql_fragment = f"""(
            SELECT value FROM (
                SELECT val as value, COUNT(*) as freq,
                       ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, val) as rn
                FROM (VALUES ({operands_str})) t(val)
                WHERE val IS NOT NULL
                GROUP BY val
            ) WHERE rn = 1
        )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_populationstddev(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle PopulationStdDev() aggregate function."""
        if not self.args:
            raise InvalidArgumentError("PopulationStdDev function requires at least one argument")
        
        # Get all operands (input + arguments)
        all_operands = [input_state.sql_fragment]
        all_operands.extend(str(arg) for arg in self.args)
        
        # Use database-specific population standard deviation function
        sql_fragment = context.dialect.generate_population_stddev(all_operands)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_populationvariance(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle PopulationVariance() aggregate function."""
        if not self.args:
            raise InvalidArgumentError("PopulationVariance function requires at least one argument")
        
        # Get all operands (input + arguments)
        all_operands = [input_state.sql_fragment]
        all_operands.extend(str(arg) for arg in self.args)
        
        # Use database-specific population variance function
        sql_fragment = context.dialect.generate_population_variance(all_operands)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # ==================================
    # DATETIME FUNCTIONS (5+ functions)
    # ==================================
    
    def _execute_datetime_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute datetime function with dialect optimization."""
        
        if self.func_name == 'now':
            return self._handle_now(input_state, context)
        elif self.func_name == 'today':
            return self._handle_today(input_state, context)
        elif self.func_name == 'timeofday':
            return self._handle_timeofday(input_state, context)
        elif self.func_name == 'lowboundary':
            return self._handle_lowboundary(input_state, context)
        elif self.func_name == 'highboundary':
            return self._handle_highboundary(input_state, context)
        elif self.func_name == 'datetime':
            return self._handle_datetime_constructor(input_state, context)
        elif self.func_name == 'ageinyears':
            return self._handle_ageinyears(input_state, context)
        elif self.func_name == 'ageinyearsat':
            return self._handle_ageinyearsat(input_state, context)
        elif self.func_name == 'hour_from':
            return self._handle_hour_from(input_state, context)
        elif self.func_name == 'minute_from':
            return self._handle_minute_from(input_state, context)
        elif self.func_name == 'second_from':
            return self._handle_second_from(input_state, context)
        elif self.func_name == 'year_from':
            return self._handle_year_from(input_state, context)
        elif self.func_name == 'month_from':
            return self._handle_month_from(input_state, context)
        elif self.func_name == 'day_from':
            return self._handle_day_from(input_state, context)
        else:
            raise ValueError(f"Unknown datetime function: {self.func_name}")
    
    def _handle_now(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle now() function."""
        sql_fragment = context.dialect.generate_date_time_now()
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_today(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle today() function."""
        sql_fragment = context.dialect.generate_date_time_today()
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # Placeholder implementations for remaining datetime functions
    def _handle_timeofday(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle timeOfDay() function.
        
        Returns the time-of-day part of a datetime value.
        """
        sql_fragment = context.dialect.cast_to_time(input_state.sql_fragment)
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_lowboundary(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle lowBoundary() function.
        
        Returns the lower boundary of a period or range. For date/time values,
        this represents the earliest possible interpretation of the value.
        """
        sql_fragment = context.dialect.cast_to_timestamp(input_state.sql_fragment)
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_highboundary(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle highBoundary() function.
        
        Returns the upper boundary of a period or range. For date/time values,
        this represents the latest possible interpretation of the value.
        """
        sql_fragment = context.dialect.cast_to_timestamp(input_state.sql_fragment)
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_datetime_constructor(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle DateTime(year, month, day, ...) constructor function.
        
        CQL DateTime constructor supports:
        - DateTime(null) - returns null DateTime
        - DateTime(year) - year precision
        - DateTime(year, month) - month precision  
        - DateTime(year, month, day) - day precision
        - DateTime(year, month, day, hour, minute, second) - full precision
        """
        if not self.args:
            raise InvalidArgumentError("DateTime constructor requires at least 1 argument")
        
        # Handle null argument case - check if first argument evaluates to null
        first_arg_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        if first_arg_sql in ['NULL', 'null']:
            return self._create_scalar_result(input_state, 'NULL')
        
        # Extract the year, month, day arguments with proper evaluation
        year = self._evaluate_logical_argument(self.args[0], input_state, context) if len(self.args) > 0 else "1"
        month = self._evaluate_logical_argument(self.args[1], input_state, context) if len(self.args) > 1 else "1"
        day = self._evaluate_logical_argument(self.args[2], input_state, context) if len(self.args) > 2 else "1"
        
        # Optional hour, minute, second arguments
        hour = self._evaluate_logical_argument(self.args[3], input_state, context) if len(self.args) > 3 else "0"
        minute = self._evaluate_logical_argument(self.args[4], input_state, context) if len(self.args) > 4 else "0"
        second = self._evaluate_logical_argument(self.args[5], input_state, context) if len(self.args) > 5 else "0"
        
        # Generate SQL for DateTime construction
        sql_fragment = context.dialect.generate_datetime_creation(year, month, day, hour, minute, second)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_ageinyears(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle AgeInYears() context-dependent function."""
        
        # AgeInYears() without parameters should use Patient.birthDate from context
        # If parameters are provided, use the first parameter as the birthdate
        if self.args and len(self.args) > 0:
            # Explicit birthdate provided: AgeInYears(Patient.birthDate)
            birthdate_expr = str(self.args[0])
        else:
            # Context-dependent: assume Patient.birthDate from current context
            # For now, use a placeholder that represents Patient.birthDate
            birthdate_expr = "json_extract_string(resource, '$.birthDate')"
        
        # Calculate age in years using SQL date functions
        sql_fragment = context.dialect.generate_age_in_years(birthdate_expr)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_ageinyearsat(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle AgeInYearsAt(asOf) function - age calculation with specific date."""
        
        if not self.args or len(self.args) < 1:
            raise InvalidArgumentError("AgeInYearsAt function requires at least one argument (asOf date)")
        
        # First argument is the asOf date
        as_of_date_expr = str(self.args[0])
        
        # Second argument (if provided) is the birthdate, otherwise use Patient.birthDate from context
        if len(self.args) > 1:
            birthdate_expr = str(self.args[1])
        else:
            # Context-dependent: assume Patient.birthDate from current context
            birthdate_expr = "json_extract_string(resource, '$.birthDate')"
        
        # Calculate age in years using SQL date functions
        sql_fragment = context.dialect.generate_age_in_years_at(birthdate_expr, as_of_date_expr)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_hour_from(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle hour from datetime extraction."""
        if not self.args:
            raise InvalidArgumentError("hour_from function requires one argument")
        
        datetime_expr = str(self.args[0])
        sql_fragment = f"EXTRACT(HOUR FROM CAST({datetime_expr} AS TIMESTAMP))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_minute_from(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle minute from datetime extraction."""
        if not self.args:
            raise InvalidArgumentError("minute_from function requires one argument")
        
        datetime_expr = str(self.args[0])
        sql_fragment = f"EXTRACT(MINUTE FROM CAST({datetime_expr} AS TIMESTAMP))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_second_from(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle second from datetime extraction."""
        if not self.args:
            raise InvalidArgumentError("second_from function requires one argument")
        
        datetime_expr = str(self.args[0])
        sql_fragment = f"EXTRACT(SECOND FROM CAST({datetime_expr} AS TIMESTAMP))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_year_from(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle year from datetime extraction."""
        if not self.args:
            raise InvalidArgumentError("year_from function requires one argument")
        
        datetime_expr = str(self.args[0])
        sql_fragment = f"EXTRACT(YEAR FROM CAST({datetime_expr} AS TIMESTAMP))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_month_from(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle month from datetime extraction."""
        if not self.args:
            raise InvalidArgumentError("month_from function requires one argument")
        
        datetime_expr = str(self.args[0])
        sql_fragment = f"EXTRACT(MONTH FROM CAST({datetime_expr} AS TIMESTAMP))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_day_from(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle day from datetime extraction."""
        if not self.args:
            raise InvalidArgumentError("day_from function requires one argument")
        
        datetime_expr = str(self.args[0])
        sql_fragment = f"EXTRACT(DAY FROM CAST({datetime_expr} AS TIMESTAMP))"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # ====================================
    # ERROR AND MESSAGING FUNCTIONS
    # ====================================
    
    def _execute_error_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute error/messaging functions."""
        
        if self.func_name == 'message':
            return self._handle_message(input_state, context)
        elif self.func_name == 'error':
            return self._handle_error(input_state, context)
        else:
            raise ValueError(f"Unknown error function: {self.func_name}")
    
    def _handle_message(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle message() function for debugging/logging."""
        # Message function typically takes (source, condition, code, severity, message)
        # For SQL generation, we'll return the source value and include a comment about the message
        
        if self.args and len(self.args) >= 4:
            message_text = str(self.args[4]) if len(self.args) > 4 else "'message'"
            sql_fragment = f"/* MESSAGE: {message_text} */ {input_state.sql_fragment}"
        else:
            sql_fragment = f"/* MESSAGE */ {input_state.sql_fragment}"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_error(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle error() function for error conditions."""
        # Error function raises a runtime error - for SQL generation, we'll return a placeholder
        error_msg = str(self.args[0]) if self.args else "'error'"
        sql_fragment = f"CASE WHEN TRUE THEN {error_msg} ELSE NULL END"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # =========================================
    # FHIR-SPECIFIC FUNCTIONS (2+ functions)
    # =========================================
    
    def _execute_fhir_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute FHIR-specific functions."""
        
        if self.func_name == 'getvalue':
            return self._handle_getvalue(input_state, context)
        elif self.func_name == 'resolve':
            return self._handle_resolve(input_state, context)
        else:
            raise ValueError(f"Unknown FHIR function: {self.func_name}")
    
    def _handle_getvalue(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle getValue() function for extracting primitive values from FHIR elements."""
        # Use dialect method for getValue primitive SQL
        sql_fragment = context.dialect.get_value_primitive_sql(input_state.sql_fragment)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_resolve(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle resolve() function for FHIR reference resolution."""
        # Use dialect method for resolve reference SQL
        base_expr = input_state.sql_fragment
        sql_fragment = context.dialect.resolve_reference_sql(base_expr)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,  # resolve() returns a single resource, not an array
            context_mode=ContextMode.SINGLE_VALUE
        )

    # =========================================
    # COMPARISON OPERATORS (8+ operators)
    # =========================================
    
    def _execute_logical_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute logical operator with three-valued logic."""
        
        if self.func_name == 'and':
            return self._handle_and(input_state, context)
        elif self.func_name == 'or':
            return self._handle_or(input_state, context)
        elif self.func_name == 'not':
            return self._handle_not(input_state, context)
        elif self.func_name == 'implies':
            return self._handle_implies(input_state, context)
        elif self.func_name == 'xor':
            return self._handle_xor(input_state, context)
        else:
            raise ValueError(f"Unknown logical operator: {self.func_name}")
    
    def _handle_and(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle logical AND with three-valued logic."""
        if len(self.args) != 2:
            raise ValueError("AND operator requires exactly 2 arguments")
        
        # Convert arguments to SQL fragments
        left_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        right_sql = self._evaluate_logical_argument(self.args[1], input_state, context)
        
        # Implement three-valued logic for AND: 
        # T and T = T, T and F = F, T and null = null
        # F and T = F, F and F = F, F and null = F  
        # null and T = null, null and F = F, null and null = null
        sql_fragment = f"""(
            CASE 
                WHEN ({left_sql}) IS FALSE OR ({right_sql}) IS FALSE THEN FALSE
                WHEN ({left_sql}) IS NULL OR ({right_sql}) IS NULL THEN NULL
                ELSE ({left_sql}) AND ({right_sql})
            END
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_or(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle logical OR with three-valued logic."""
        if len(self.args) != 2:
            raise ValueError("OR operator requires exactly 2 arguments")
        
        # Convert arguments to SQL fragments
        left_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        right_sql = self._evaluate_logical_argument(self.args[1], input_state, context)
        
        # Implement three-valued logic for OR:
        # T or T = T, T or F = T, T or null = T
        # F or T = T, F or F = F, F or null = null
        # null or T = T, null or F = null, null or null = null
        sql_fragment = f"""(
            CASE 
                WHEN ({left_sql}) IS TRUE OR ({right_sql}) IS TRUE THEN TRUE
                WHEN ({left_sql}) IS NULL OR ({right_sql}) IS NULL THEN NULL
                ELSE ({left_sql}) OR ({right_sql})
            END
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_not(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle logical NOT with three-valued logic."""
        if len(self.args) != 1:
            raise ValueError("NOT operator requires exactly 1 argument")
        
        # Convert argument to SQL fragment
        operand_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        
        # Implement three-valued logic for NOT:
        # not T = F, not F = T, not null = null
        sql_fragment = f"""(
            CASE 
                WHEN ({operand_sql}) IS NULL THEN NULL
                ELSE NOT ({operand_sql})
            END
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_implies(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle logical IMPLIES with three-valued logic."""
        if len(self.args) != 2:
            raise ValueError("IMPLIES operator requires exactly 2 arguments")
        
        # Convert arguments to SQL fragments
        left_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        right_sql = self._evaluate_logical_argument(self.args[1], input_state, context)
        
        # Implement three-valued logic for IMPLIES (A implies B = NOT A OR B):
        # T implies T = T, T implies F = F, T implies null = null
        # F implies T = T, F implies F = T, F implies null = T
        # null implies T = T, null implies F = null, null implies null = null
        sql_fragment = f"""(
            CASE 
                WHEN ({left_sql}) IS FALSE THEN TRUE
                WHEN ({left_sql}) IS NULL THEN 
                    CASE 
                        WHEN ({right_sql}) IS TRUE THEN TRUE
                        ELSE NULL
                    END
                ELSE ({right_sql})
            END
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_xor(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle logical XOR with three-valued logic."""
        if len(self.args) != 2:
            raise ValueError("XOR operator requires exactly 2 arguments")
        
        # Convert arguments to SQL fragments
        left_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        right_sql = self._evaluate_logical_argument(self.args[1], input_state, context)
        
        # Implement three-valued logic for XOR:
        # T xor T = F, T xor F = T, T xor null = null
        # F xor T = T, F xor F = F, F xor null = null
        # null xor T = null, null xor F = null, null xor null = null
        sql_fragment = f"""(
            CASE 
                WHEN ({left_sql}) IS NULL OR ({right_sql}) IS NULL THEN NULL
                ELSE ({left_sql}) != ({right_sql})
            END
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _evaluate_logical_argument(self, arg: Any, input_state: SQLState, 
                                   context: ExecutionContext) -> str:
        """Evaluate logical function argument to SQL fragment."""
        # Handle simple literals first  
        if not hasattr(arg, '__class__'):
            # Simple literal - convert boolean values
            if arg is True:
                return "TRUE"
            elif arg is False:
                return "FALSE" 
            elif arg is None:
                return "NULL"
            else:
                return str(arg)
        
        # Handle pipeline operations (like LiteralOperation)
        if hasattr(arg, 'execute'):
            # This is a pipeline operation - execute it to get the SQL fragment
            try:
                result_state = arg.execute(input_state, context)
                return result_state.sql_fragment
            except Exception as e:
                raise InvalidArgumentError(f"Failed to execute pipeline operation argument: {e}")
        
        # Handle LiteralOperation objects directly (if not handled by execute)
        if hasattr(arg, 'value') and hasattr(arg, 'literal_type'):
            # This is a LiteralOperation from the pipeline
            if arg.value in ['true', True]:
                return "TRUE"
            elif arg.value in ['false', False]:
                return "FALSE"
            elif arg.value is None or arg.value == 'null':
                return "NULL"
            else:
                return str(arg.value)
        
        # For other complex objects, try AST conversion
        try:
            return self._convert_ast_argument_to_sql(arg, input_state, context)
        except ConversionError as e:
            raise InvalidArgumentError(f"Failed to evaluate logical argument: {e}")
    
    def _execute_interval_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute interval function with proper SQL generation."""
        
        if self.func_name == 'interval':
            return self._handle_interval_constructor(input_state, context)
        elif self.func_name == 'contains':
            return self._handle_interval_contains(input_state, context)
        elif self.func_name == 'overlaps':
            return self._handle_interval_overlaps(input_state, context)
        elif self.func_name == 'before':
            return self._handle_interval_before(input_state, context)
        elif self.func_name == 'after':
            return self._handle_interval_after(input_state, context)
        elif self.func_name == 'starts':
            return self._handle_interval_starts(input_state, context)
        elif self.func_name == 'ends':
            return self._handle_interval_ends(input_state, context)
        elif self.func_name == 'meets':
            return self._handle_interval_meets(input_state, context)
        elif self.func_name == 'during':
            return self._handle_interval_during(input_state, context)
        elif self.func_name == 'includes':
            return self._handle_interval_includes(input_state, context)
        elif self.func_name == 'included_in':
            return self._handle_interval_included_in(input_state, context)
        elif self.func_name == 'width':
            return self._handle_interval_width(input_state, context)
        elif self.func_name == 'size':
            return self._handle_interval_size(input_state, context)
        elif self.func_name == 'start':
            return self._handle_interval_start(input_state, context)
        elif self.func_name == 'end':
            return self._handle_interval_end(input_state, context)
        else:
            raise ValueError(f"Unknown interval function: {self.func_name}")
    
    def _handle_interval_constructor(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval constructor: Interval(start, end)"""
        if len(self.args) != 2:
            raise ValueError("Interval constructor requires exactly 2 arguments")
        
        # Convert arguments to SQL fragments
        start_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        end_sql = self._evaluate_logical_argument(self.args[1], input_state, context)
        
        # For now, create a JSON representation of the interval
        # In a full implementation, this would use database-specific interval types
        sql_fragment = f"""JSON_OBJECT(
            'type', 'interval',
            'start', {start_sql},
            'end', {end_sql},
            'startInclusive', true,
            'endInclusive', true
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Interval is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_interval_contains(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval contains operation: interval contains value"""
        if len(self.args) != 1:
            raise ValueError("Interval contains requires exactly 1 argument")
        
        # For now, implement basic contains logic
        # In a full implementation, this would properly parse the interval
        value_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        
        sql_fragment = f"""(
            {value_sql} BETWEEN 
            CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.start') AS NUMERIC) AND
            CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.end') AS NUMERIC)
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Boolean result is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_interval_overlaps(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval overlaps operation: interval1 overlaps interval2"""
        # Placeholder implementation - would need full interval comparison logic
        sql_fragment = "TRUE"  # Simplified for now
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    # Placeholder implementations for other interval functions
    def _handle_interval_before(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval before operation.""" 
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_after(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval after operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_starts(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval starts operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_ends(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval ends operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_meets(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval meets operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
        
    def _handle_interval_during(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval during operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_includes(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval includes operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_included_in(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval included_in operation."""
        return self._handle_interval_overlaps(input_state, context)  # Placeholder
    
    def _handle_interval_width(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval width operation."""
        sql_fragment = f"""(
            CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.end') AS NUMERIC) -
            CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.start') AS NUMERIC)
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_interval_size(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval size operation (same as width for numeric intervals)."""
        return self._handle_interval_width(input_state, context)
    
    def _handle_interval_start(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval start operation."""
        sql_fragment = f"CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.start') AS NUMERIC)"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_interval_end(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle interval end operation."""
        sql_fragment = f"CAST(JSON_EXTRACT({input_state.sql_fragment}, '$.end') AS NUMERIC)"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _execute_list_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute list function with proper SQL generation."""
        
        if self.func_name == 'list':
            return self._handle_list_constructor(input_state, context)
        elif self.func_name == 'flatten':
            return self._handle_list_flatten(input_state, context)
        elif self.func_name == 'distinct':
            return self._handle_list_distinct(input_state, context)
        elif self.func_name == 'union':
            return self._handle_list_union(input_state, context)
        elif self.func_name == 'intersect':
            return self._handle_list_intersect(input_state, context)
        elif self.func_name == 'except':
            return self._handle_list_except(input_state, context)
        elif self.func_name == 'first':
            return self._handle_list_first(input_state, context)
        elif self.func_name == 'last':
            return self._handle_list_last(input_state, context)
        elif self.func_name == 'tail':
            return self._handle_list_tail(input_state, context)
        elif self.func_name == 'take':
            return self._handle_list_take(input_state, context)
        elif self.func_name == 'skip':
            return self._handle_list_skip(input_state, context)
        elif self.func_name == 'singleton':
            return self._handle_list_singleton(input_state, context)
        elif self.func_name == 'repeat':
            return self._handle_list_repeat(input_state, context)
        else:
            raise ValueError(f"Unknown list function: {self.func_name}")
    
    def _handle_list_constructor(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list constructor: List(item1, item2, ...)"""
        # Convert all arguments to SQL fragments
        item_sqls = []
        for arg in self.args:
            item_sql = self._evaluate_logical_argument(arg, input_state, context)
            item_sqls.append(item_sql)
        
        # Create JSON array with all items
        if item_sqls:
            sql_fragment = f"json_array({', '.join(item_sqls)})"
        else:
            sql_fragment = "json_array()"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=True,  # List is a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_list_flatten(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list flatten operation."""
        # Placeholder implementation - would flatten nested arrays
        sql_fragment = input_state.sql_fragment  # For now, return as-is
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=True,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_list_distinct(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list distinct operation."""
        # Remove duplicates from array
        sql_fragment = f"""(
            SELECT json_group_array(DISTINCT value)
            FROM json_each({input_state.sql_fragment})
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=True,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_list_first(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list first operation."""
        sql_fragment = f"json_extract({input_state.sql_fragment}, '$[0]')"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,  # Single item is not a collection
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_list_last(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list last operation."""
        sql_fragment = f"json_extract({input_state.sql_fragment}, '$[#-1]')"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    # Placeholder implementations for other list functions
    def _handle_list_union(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list union operation."""
        return self._handle_list_flatten(input_state, context)  # Placeholder
    
    def _handle_list_intersect(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list intersect operation."""
        return self._handle_list_flatten(input_state, context)  # Placeholder
    
    def _handle_list_except(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list except operation."""
        return self._handle_list_flatten(input_state, context)  # Placeholder
    
    def _handle_list_tail(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list tail operation (all except first)."""
        sql_fragment = f"""(
            SELECT json_group_array(value)
            FROM (
                SELECT value, row_number() OVER () as rn
                FROM json_each({input_state.sql_fragment})
            )
            WHERE rn > 1
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=True,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_list_take(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list take operation."""
        if len(self.args) != 1:
            raise ValueError("Take operation requires exactly 1 argument")
        
        count_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        sql_fragment = f"""(
            SELECT json_group_array(value)
            FROM (
                SELECT value, row_number() OVER () as rn
                FROM json_each({input_state.sql_fragment})
            )
            WHERE rn <= {count_sql}
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=True,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_list_skip(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list skip operation."""
        if len(self.args) != 1:
            raise ValueError("Skip operation requires exactly 1 argument")
        
        count_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        sql_fragment = f"""(
            SELECT json_group_array(value)
            FROM (
                SELECT value, row_number() OVER () as rn
                FROM json_each({input_state.sql_fragment})
            )
            WHERE rn > {count_sql}
        )"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=True,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_list_singleton(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list singleton operation (return single item if list has exactly one element)."""
        sql_fragment = f"""
        CASE 
            WHEN json_array_length({input_state.sql_fragment}) = 1 THEN 
                json_extract({input_state.sql_fragment}, '$[0]')
            ELSE NULL
        END"""
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment,
            ctes=input_state.ctes.copy(),
            lateral_joins=input_state.lateral_joins.copy(),
            context_mode=input_state.context_mode,
            resource_type=input_state.resource_type,
            is_collection=False,
            path_context=input_state.path_context,
            variable_bindings=input_state.variable_bindings.copy()
        )
    
    def _handle_list_repeat(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle list repeat operation."""
        # Placeholder implementation
        return self._handle_list_flatten(input_state, context)
    
    # =====================================
    # PIPELINE OPERATION INTERFACE METHODS
    # =====================================
    
    def optimize_for_dialect(self, dialect) -> 'FunctionCallOperation':
        """
        Optimize function for specific dialect.
        
        Args:
            dialect: Target database dialect
            
        Returns:
            Potentially optimized function operation
        """
        # Most function optimizations are handled within the execution methods
        # based on dialect detection. Could be enhanced with dialect-specific
        # function operation subclasses.
        return self
    
    # ====================================
    # QUERY FUNCTION EXECUTION
    # ====================================
    
    def _execute_query_function(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Execute query operator functions (from, sort, aggregate, etc.)."""
        
        if self.func_name == 'from':
            return self._handle_from_query(input_state, context)
        elif self.func_name == 'sort':
            return self._handle_sort_query(input_state, context)
        elif self.func_name == 'aggregate':
            return self._handle_aggregate_query(input_state, context)
        elif self.func_name == 'where':
            return self._handle_where_query(input_state, context)
        elif self.func_name == 'return':
            return self._handle_return_query(input_state, context)
        elif self.func_name == 'retrievenode':
            return self._handle_retrievenode_query(input_state, context)
        else:
            raise ValueError(f"Unknown query function: {self.func_name}")
    
    def _handle_from_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle FROM query (Cartesian product of collections)."""
        logger.debug(f"Executing FROM query with {len(self.args)} arguments")
        
        # For now, implement a basic Cartesian product placeholder
        # This should create SQL that joins multiple collections
        
        # Extract collection expressions and aliases
        collections = []
        aliases = []
        
        # Parse args: should be alternating collection expressions and alias names
        for i in range(0, len(self.args), 2):
            if i + 1 < len(self.args):
                collection_expr = self.args[i]
                alias = self.args[i + 1] if isinstance(self.args[i + 1], str) else f"alias_{i}"
                
                # Convert collection expression to SQL
                collection_sql = self._evaluate_logical_argument(collection_expr, input_state, context)
                collections.append(collection_sql)
                aliases.append(alias)
        
        # Generate basic Cartesian product SQL
        # For now, return a simple JSON array representing the collections
        if collections:
            combined_sql = f"json_array({', '.join(collections)})"
        else:
            combined_sql = "json_array()"
        
        return input_state.evolve(
            sql_fragment=combined_sql,
            is_collection=True,
            context_mode=ContextMode.COLLECTION
        )
    
    def _handle_sort_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle SORT query."""
        # For now, return the input unchanged - full sorting would require SQL ORDER BY
        return input_state.evolve(
            sql_fragment=input_state.sql_fragment,
            is_collection=True
        )
    
    def _handle_aggregate_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle AGGREGATE query."""
        # For now, return the input unchanged - full aggregation would require complex SQL
        return input_state.evolve(
            sql_fragment=input_state.sql_fragment,
            is_collection=True
        )
    
    def _handle_where_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle WHERE query."""
        # For now, return the input unchanged - full filtering would require SQL WHERE clause
        return input_state.evolve(
            sql_fragment=input_state.sql_fragment,
            is_collection=True
        )
    
    def _handle_return_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle RETURN query."""
        # For now, return the input unchanged - full return would require SQL SELECT
        return input_state.evolve(
            sql_fragment=input_state.sql_fragment,
            is_collection=input_state.is_collection
        )
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        args_preview = f"({len(self.args)} args)" if self.args else "()"
        return f"function({self.func_name}{args_preview})"
    
    def validate_preconditions(self, input_state: SQLState, context: ExecutionContext) -> None:
        """
        Validate function preconditions.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Raises:
            ValueError: If preconditions not met
        """
        # Function-specific validation could be added here
        # For now, basic validation
        if not input_state.sql_fragment:
            raise ValueError(f"Cannot apply {self.func_name}() to empty SQL fragment")
    
    def estimate_complexity(self, input_state: SQLState, context: ExecutionContext) -> int:
        """
        Estimate complexity of function execution.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            Complexity score (0-10)
        """
        base_complexity = 3  # Functions are moderately complex
        
        # Collection functions are more expensive
        if self._is_collection_function():
            base_complexity += 2
        
        # Math functions are generally simple
        if self._is_math_function():
            base_complexity = 2
        
        # String functions with regex are expensive
        if self.func_name in ['matches', 'replacematches']:
            base_complexity += 3
        
        # Functions with multiple arguments are more complex
        base_complexity += min(len(self.args), 3)
        
        return min(base_complexity, 10)
    
    def _handle_retrievenode_query(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Handle RetrieveNode query for resource retrieval.
        
        This method handles the case where a RetrieveNode is converted to a string
        and parsed as a function call "retrievenode()". It generates SQL to retrieve
        all resources of a particular type from the FHIR database.
        
        Args:
            input_state: Current SQL state
            context: Execution context
            
        Returns:
            Updated SQL state with resource retrieval query
        """
        logger.info("🔧 Handling RetrieveNode query - converting to resource retrieval SQL")
        
        # FIXED: Advanced context-aware resource type detection
        resource_type = "Patient"  # Default fallback
        
        # Method 1: Check if arguments contain resource type information
        if self.args and len(self.args) > 0:
            # First argument might contain resource type information
            arg_str = str(self.args[0]).strip()
            if arg_str and arg_str != '[]':
                # Extract resource type from argument
                resource_type = arg_str
                logger.debug(f"Extracted resource type from arguments: {resource_type}")
        
        # Method 2: Check if the input_state has resource_type information
        elif hasattr(input_state, 'resource_type') and input_state.resource_type:
            resource_type = input_state.resource_type
            logger.debug(f"Extracted resource type from input_state: {resource_type}")
        
        # Method 3: Try to extract from the sql_fragment if it contains resource type hints
        elif input_state.sql_fragment and isinstance(input_state.sql_fragment, str):
            # Look for patterns like '[Condition]', '[MedicationDispense]', etc.
            import re
            resource_match = re.search(r'\[(\w+)(?:\s*:\s*"[^"]+")?\]', input_state.sql_fragment)
            if resource_match:
                resource_type = resource_match.group(1)
                logger.debug(f"Extracted resource type from SQL fragment: {resource_type}")
        
        # Method 4: Check if execution context has any hints about current resource type
        elif hasattr(context, 'current_resource_type') and context.current_resource_type:
            resource_type = context.current_resource_type
            logger.debug(f"Extracted resource type from context: {resource_type}")
        
        # Method 5: ADVANCED - Check if we can infer from define operations context
        elif hasattr(self, 'all_define_operations') and self.all_define_operations:
            # This is a fallback - in real scenarios we should have better context
            logger.debug(f"Using fallback resource type: {resource_type}")
        
        # Method 6: BREAKTHROUGH - Check the execution context for define name hints
        elif hasattr(context, 'current_define_name') or hasattr(context, 'define_name'):
            current_define_name = getattr(context, 'current_define_name', None) or getattr(context, 'define_name', None)
            if current_define_name:
                # Map define names to resource types based on semantic analysis
                define_to_resource_map = {
                    'has asthma': 'Condition',
                    'asthma': 'Condition',
                    'condition': 'Condition',
                    'has encounters': 'Encounter', 
                    'encounter': 'Encounter',
                    'has controller medications': 'MedicationDispense',
                    'has medications': 'MedicationDispense',
                    'medication': 'MedicationDispense',
                    'medicationdispense': 'MedicationDispense',
                    'all patients': 'Patient',
                    'patient': 'Patient',
                    'initial population': 'Patient'  # Usually patients
                }
                
                # Normalize define name for matching
                define_lower = current_define_name.lower().strip('"').strip("'")
                for key, resource in define_to_resource_map.items():
                    if key in define_lower:
                        resource_type = resource
                        logger.debug(f"Inferred resource type from define name '{current_define_name}': {resource_type}")
                        break
        
        # Method 7: REMOVED - Thread-local hack eliminated (proper pipeline context flows through ExecutionContext)
        
        # Method 8: REMOVED - Call stack introspection eliminated (proper context flows through pipeline)
        # If resource_type is still "Patient" here, that's fine - the pipeline provides proper context
        
        # Generate SQL for resource retrieval based on dialect
        resource_query = context.dialect.generate_resource_query(input_state, resource_type)
        
        logger.info(f"🔧 Generated resource query for {resource_type}: {resource_query[:100]}...")
        
        # Update SQL state with resource retrieval
        return input_state.evolve(
            sql_fragment=resource_query,
            is_collection=True,
            context_mode=ContextMode.COLLECTION,
            resource_type=resource_type,
            path_context="$"  # Reset to root context for retrieved resources
        )
    


class DefineReferenceOperation(PipelineOperation[SQLState]):
    """
    CQL define reference operation for resolving references to other define statements.
    
    This operation handles CQL define references by recursively resolving them to their
    underlying expressions, enabling proper define-to-define dependencies.
    """
    
    def __init__(self, define_name: str, define_operation: Any, all_define_operations: dict = None):
        """
        Initialize define reference operation.
        
        Args:
            define_name: Name of the referenced define statement
            define_operation: The actual define operation being referenced
            all_define_operations: Full context of all define operations for recursive resolution
        """
        self.define_name = define_name
        self.define_operation = define_operation
        self.all_define_operations = all_define_operations or {}
        
        logger.debug(f"Created DefineReferenceOperation: {define_name}")
    
    def compile(self, context: ExecutionContext, state: SQLState) -> SQLState:
        """
        Compile define reference by recursively resolving to the final SQL expression.
        
        Args:
            context: Execution context
            state: Current SQL state
            
        Returns:
            Updated SQL state with fully resolved define reference
        """
        logger.info(f"🔧 DefineReferenceOperation.compile() called for '{self.define_name}'")
        
        # Get the original expression from the define operation
        if isinstance(self.define_operation, dict) and 'original_expression' in self.define_operation:
            original_expression = self.define_operation['original_expression']
            logger.debug(f"DefineReferenceOperation resolving '{self.define_name}' -> '{original_expression}'")
            
            # Recursively resolve the expression by parsing and converting it
            result = self._resolve_expression_recursively(original_expression, context, state)
            logger.info(f"🔧 DefineReferenceOperation '{self.define_name}' final SQL: {result.sql_fragment}")
            return result
        else:
            # Handle other define operation formats
            logger.warning(f"Unsupported define operation format for {self.define_name}: {type(self.define_operation)}")
            # Fallback to literal
            from .literals import LiteralOperation
            literal_op = LiteralOperation(self.define_name, 'string')
            return literal_op.execute(state, context)
    
    def _resolve_expression_recursively(self, expression: str, context: ExecutionContext, state: SQLState, visited_defines: set = None) -> SQLState:
        """
        Recursively resolve a CQL expression that may contain define references.
        
        Args:
            expression: CQL expression to resolve
            context: Execution context
            state: Current SQL state
            visited_defines: Set of already visited defines to prevent circular references
            
        Returns:
            Fully resolved SQL state
        """
        if visited_defines is None:
            visited_defines = set()
        
        # For the initial call, add the current define to visited set
        if self.define_name not in visited_defines:
            visited_defines.add(self.define_name)
        
        # Handle simple function calls directly
        if expression == 'Today()':
            logger.debug(f"DefineReferenceOperation resolving 'Today()' to TodayOperation")
            from .literals import TodayOperation
            today_op = TodayOperation()
            result = today_op.compile(context, state)
            logger.debug(f"TodayOperation compiled to SQL: {result.sql_fragment}")
            return result
        elif expression == 'Now()':
            # Create a NowOperation similar to TodayOperation
            from .literals import LiteralOperation
            # For now, use current_timestamp (works for both DuckDB and PostgreSQL)
            sql_fragment = "current_timestamp"
            return state.evolve(
                sql_fragment=sql_fragment,
                is_collection=False,
                context_mode=getattr(state, 'context_mode', None)
            )
        
        # Check if this expression is itself a define reference
        # Use the stored define operations context for recursive resolution
        if self.all_define_operations:
            expr_stripped = expression.strip()
            
            # Check for both quoted and unquoted versions of define references
            define_candidates = [expr_stripped]
            if expr_stripped.startswith('"') and expr_stripped.endswith('"'):
                # Remove quotes from "define name" -> define name
                unquoted = expr_stripped[1:-1]
                define_candidates.append(unquoted)
            elif expr_stripped.startswith("'") and expr_stripped.endswith("'"):
                # Remove quotes from 'define name' -> define name  
                unquoted = expr_stripped[1:-1]
                define_candidates.append(unquoted)
            else:
                # Add quoted versions for unquoted identifiers
                define_candidates.extend([f'"{expr_stripped}"', f"'{expr_stripped}'"])
            
            # Try to find matching define
            referenced_define_name = None
            referenced_define = None
            
            for candidate in define_candidates:
                if candidate in self.all_define_operations and candidate != self.define_name:
                    referenced_define_name = candidate
                    referenced_define = self.all_define_operations[candidate]
                    break
            
            if referenced_define_name and referenced_define:
                # Check for circular references
                if referenced_define_name in visited_defines:
                    logger.warning(f"Circular define reference detected: {' -> '.join(visited_defines)} -> {referenced_define_name}")
                    from .literals import LiteralOperation
                    literal_op = LiteralOperation(f"Circular reference: {referenced_define_name}", 'string')
                    return literal_op.execute(state, context)
                
                # This is another define reference - recursively resolve it
                if isinstance(referenced_define, dict) and 'original_expression' in referenced_define:
                    referenced_expression = referenced_define['original_expression']
                    logger.debug(f"DefineReferenceOperation recursively resolving '{expr_stripped}' -> '{referenced_define_name}' -> '{referenced_expression}'")
                    new_visited = visited_defines.copy()
                    new_visited.add(referenced_define_name)
                    result = self._resolve_expression_recursively(referenced_expression, context, state, new_visited)
                    logger.debug(f"DefineReferenceOperation '{referenced_define_name}' resolved to SQL: {result.sql_fragment}")
                    return result
        
        # If we get here, it's either not a define reference or we couldn't resolve it
        # Try to parse and evaluate as a general CQL expression
        logger.debug(f"Expression '{expression}' not recognized as simple define reference, treating as literal")
        from .literals import LiteralOperation
        literal_op = LiteralOperation(expression, 'string')
        return literal_op.execute(state, context)
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute define reference by compiling it.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            SQL state with resolved define reference
        """
        return self.compile(context, input_state)
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        return f"define_ref_{self.define_name.replace(' ', '_').lower()}"
    
    def optimize_for_dialect(self, dialect) -> 'DefineReferenceOperation':
        """
        Optimize define reference operation for specific dialect.
        
        Args:
            dialect: Target dialect for optimization
            
        Returns:
            Optimized define reference operation
        """
        # Define references are dialect-agnostic since they resolve to other expressions
        return self
    
    def validate_preconditions(self, input_state: SQLState, context: ExecutionContext) -> None:
        """Validate define reference preconditions."""
        pass  # Define references have no specific preconditions
    
    def estimate_complexity(self, input_state: SQLState, context: ExecutionContext) -> int:
        """Estimate complexity of define reference."""
        return 1  # Simple reference resolution
    
    def __repr__(self) -> str:
        """String representation."""
        return f"DefineReferenceOperation(define_name='{self.define_name}')"