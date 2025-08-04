"""
CQL-to-FHIRPath Translator.

Translates CQL AST nodes to FHIRPath expressions that can be processed
by the existing FHIR4DS infrastructure.
"""

import logging
from typing import Dict, Any, Optional, Union, List

from .parser import CQLASTNode, LibraryNode, DefineNode, RetrieveNode, QueryNode, ContextNode, ParameterNode, IncludeNode, WithClauseNode, SortClauseNode, LetClauseNode
from ...fhirpath.parser.ast_nodes import *
from ..functions.clinical import ClinicalFunctions, TerminologyFunctions, ClinicalLogicFunctions
from ..functions.math_functions import CQLMathFunctionHandler
from ..functions.nullological_functions import CQLNullologicalFunctionHandler
from ..functions.datetime_functions import CQLDateTimeFunctionHandler
from ..functions.interval_functions import CQLIntervalFunctionHandler

logger = logging.getLogger(__name__)

class CQLTranslationContext:
    """Context information for CQL translation."""
    
    def __init__(self):
        self.current_context = "Patient"  # Default context
        self.library_definitions = {}     # Library define statements
        self.parameters = {}              # Library parameters
        self.includes = {}                # Included libraries
        
class CQLTranslator:
    """
    Translates CQL expressions to FHIRPath expressions.
    
    This translator converts CQL-specific constructs to equivalent FHIRPath
    expressions that can be processed by the existing SQL generation engine.
    """
    
    def __init__(self, dialect: str = "duckdb"):
        self.context = CQLTranslationContext()
        self.dialect = dialect
        
        # Initialize function handlers
        self.math_functions = CQLMathFunctionHandler(dialect)
        self.nullological_functions = CQLNullologicalFunctionHandler(dialect)
        self.datetime_functions = CQLDateTimeFunctionHandler(dialect)
        self.interval_functions = CQLIntervalFunctionHandler(dialect)
        
        # Function registry for lookup
        self.function_registry = {
            # Mathematical functions
            'min': self.math_functions,
            'max': self.math_functions,
            'sum': self.math_functions,
            'avg': self.math_functions,
            'average': self.math_functions,
            'count': self.math_functions,
            'first': self.math_functions,
            'last': self.math_functions,
            # CQL statistical functions
            'stddev': self.math_functions,
            'stdev': self.math_functions,
            'variance': self.math_functions,
            'median': self.math_functions,
            'mode': self.math_functions,
            'percentile': self.math_functions,
            'predecessor': self.math_functions,
            'successor': self.math_functions,
            # All FHIRPath math functions are inherited
            'abs': self.math_functions,
            'ceiling': self.math_functions,
            'floor': self.math_functions,
            'round': self.math_functions,
            'sqrt': self.math_functions,
            'truncate': self.math_functions,
            'exp': self.math_functions,
            'ln': self.math_functions,
            'log': self.math_functions,
            'power': self.math_functions,
            
            # Nullological functions
            'coalesce': self.nullological_functions,
            'isnull': self.nullological_functions,
            'isnotnull': self.nullological_functions,
            'istrue': self.nullological_functions,
            'isfalse': self.nullological_functions,
            'ifnull': self.nullological_functions,
            'nullif': self.nullological_functions,
            'toboolean': self.nullological_functions,
            
            # Date/time functions
            'year': self.datetime_functions,
            'month': self.datetime_functions,
            'day': self.datetime_functions,
            'hour': self.datetime_functions,
            'minute': self.datetime_functions,
            'second': self.datetime_functions,
            'date': self.datetime_functions,
            'time': self.datetime_functions,
            
            # Component extraction functions
            'year_from': self.datetime_functions,
            'month_from': self.datetime_functions,
            'day_from': self.datetime_functions,
            'hour_from': self.datetime_functions,
            'minute_from': self.datetime_functions,
            'second_from': self.datetime_functions,
            'date_from': self.datetime_functions,
            # CQL datetime functions
            'ageinyears': self.datetime_functions,
            'time_from': self.datetime_functions,
            'years_between': self.datetime_functions,
            'months_between': self.datetime_functions,
            'days_between': self.datetime_functions,
            'hours_between': self.datetime_functions,
            'minutes_between': self.datetime_functions,
            'seconds_between': self.datetime_functions,
            'difference_in_years': self.datetime_functions,
            'difference_in_months': self.datetime_functions,
            'difference_in_days': self.datetime_functions,
            'difference_in_hours': self.datetime_functions,
            'difference_in_minutes': self.datetime_functions,
            'difference_in_seconds': self.datetime_functions,
            'datetime': self.datetime_functions,
            'now': self.datetime_functions,
            'today': self.datetime_functions,
            'timeofday': self.datetime_functions,
            'add_years': self.datetime_functions,
            'add_months': self.datetime_functions,
            'add_days': self.datetime_functions,
            'add_hours': self.datetime_functions,
            'add_minutes': self.datetime_functions,
            'add_seconds': self.datetime_functions,
            'start_of_year': self.datetime_functions,
            'end_of_year': self.datetime_functions,
            'start_of_month': self.datetime_functions,
            'end_of_month': self.datetime_functions,
            'start_of_day': self.datetime_functions,
            'end_of_day': self.datetime_functions,
            
            # Interval functions
            'starts': self.interval_functions,
            'ends': self.interval_functions,
            'meets': self.interval_functions,
            'overlaps': self.interval_functions,
            'before': self.interval_functions,
            'after': self.interval_functions,
            'during': self.interval_functions,
            'same_as': self.interval_functions,
            'includes': self.interval_functions,
            'included_in': self.interval_functions,
            'properly_includes': self.interval_functions,
            'properly_included_in': self.interval_functions,
            'start_of': self.interval_functions,
            'end_of': self.interval_functions,
            'width_of': self.interval_functions,
            'size_of': self.interval_functions,
            'union': self.interval_functions,
            'intersection': self.interval_functions,
            'difference': self.interval_functions,
            'expand': self.interval_functions,
            'collapse': self.interval_functions,
        }
        
    def translate_expression(self, cql_ast) -> Any:
        """
        Translate CQL AST to FHIRPath AST.
        
        Args:
            cql_ast: CQL AST node or FHIRPath AST node
            
        Returns:
            FHIRPath AST node that can be processed by existing engine
        """
        logger.debug(f"Translating CQL AST: {type(cql_ast)}")
        
        # If it's already a FHIRPath AST node, check if it's a function call that needs translation
        if not isinstance(cql_ast, CQLASTNode):
            # Handle FHIRPath function calls that might be CQL functions
            if isinstance(cql_ast, FunctionCallNode):
                func_name = cql_ast.name.lower()
                # First check unified registry if available, then fall back to function_registry
                if hasattr(self, 'unified_registry') and self.unified_registry.can_handle_function(func_name):
                    return self._translate_function_via_registry(cql_ast)
                elif func_name in self.function_registry:
                    return self._translate_general_function(cql_ast)
            return cql_ast
            
        # Handle CQL-specific nodes
        if isinstance(cql_ast, RetrieveNode):
            return self._translate_retrieve(cql_ast)
        elif isinstance(cql_ast, DefineNode):
            return self._translate_define(cql_ast)
        elif isinstance(cql_ast, QueryNode):
            return self._translate_query(cql_ast)
        elif isinstance(cql_ast, ContextNode):
            return self._translate_context(cql_ast)
        elif isinstance(cql_ast, ParameterNode):
            return self._translate_parameter(cql_ast)
        elif isinstance(cql_ast, IncludeNode):
            return self._translate_include(cql_ast)
        elif isinstance(cql_ast, WithClauseNode):
            return self._translate_with_clause(cql_ast)
        elif isinstance(cql_ast, SortClauseNode):
            return self._translate_sort_clause(cql_ast)
        elif isinstance(cql_ast, LetClauseNode):
            return self._translate_let_clause(cql_ast)
        elif isinstance(cql_ast, BinaryOpNode) and self._is_temporal_operator(cql_ast.operator):
            return self._translate_temporal_operation(cql_ast)
        elif isinstance(cql_ast, FunctionCallNode):
            # Handle all function calls - first check if it's a clinical function
            if self._is_clinical_function(cql_ast.name):
                return self._translate_clinical_function(cql_ast)
            else:
                # Handle general function calls using the function registry
                return self._translate_general_function(cql_ast)
        else:
            logger.warning(f"Unknown CQL AST node type: {type(cql_ast)}")
            return cql_ast
            
    def translate_library(self, library: LibraryNode) -> Dict[str, Any]:
        """
        Translate entire CQL library.
        
        Args:
            library: LibraryNode representing parsed library
            
        Returns:
            Dictionary containing translated definitions
        """
        logger.info(f"Translating CQL library: {library.name}")
        
        result = {
            'name': library.name,
            'version': library.version,
            'definitions': {},
            'parameters': {},
            'includes': {},
            'context': library.context
        }
        
        # Process includes first
        for include_node in library.includes:
            translated = self.translate_expression(include_node)
            result['includes'][include_node.library_name] = translated
        
        # Process parameters
        for param_node in library.parameters:
            translated = self.translate_expression(param_node)
            result['parameters'][param_node.name] = translated
            
        # Set context
        self.context.current_context = library.context
        
        # Process definitions
        for define_node in library.definitions:
            translated = self.translate_expression(define_node)
            result['definitions'][define_node.name] = translated
            
        return result
        
    def _translate_retrieve(self, retrieve: RetrieveNode) -> Any:
        """
        Translate CQL retrieve expression to FHIRPath.
        
        CQL: [Condition: "Diabetes mellitus"]
        FHIRPath: Condition (with implicit filtering based on context)
        
        Args:
            retrieve: RetrieveNode to translate
            
        Returns:
            FHIRPath expression for resource retrieval
        """
        logger.debug(f"Translating retrieve: {retrieve.resource_type}")
        
        # Phase 1: Simple implementation
        # CQL retrieve [ResourceType] -> FHIRPath resource type identifier
        
        # Create FHIRPath expression: ResourceType
        resource_identifier = IdentifierNode(retrieve.resource_type)
        
        if retrieve.terminology:
            # Phase 1: Simple terminology handling
            # TODO: Implement proper terminology filtering in Phase 2
            logger.debug(f"Retrieve with terminology: {retrieve.terminology}")
            
            # For now, create a simple filter expression
            # This would need to be enhanced for proper terminology binding
            # [ResourceType: "terminology"] -> ResourceType.where(code in "terminology")
            
            # Create a where clause for terminology filtering
            # This is a simplified approach for Phase 1
            where_expr = BinaryOpNode(
                left=PathNode([IdentifierNode("code")]),
                operator="=",
                right=LiteralNode(retrieve.terminology, "string")
            )
            
            # Return as a filtered expression (simplified)
            # In a full implementation, this would involve proper terminology services
            return resource_identifier
            
        return resource_identifier
        
    def _translate_define(self, define: DefineNode) -> Any:
        """
        Translate CQL define statement.
        
        Args:
            define: DefineNode to translate
            
        Returns:
            Translated expression
        """
        logger.debug(f"Translating define: {define.name}")
        
        # Store definition in context for reference
        if define.expression:
            translated_expr = self.translate_expression(define.expression)
            self.context.library_definitions[define.name] = {
                'expression': translated_expr,
                'access_level': define.access_level
            }
            return translated_expr
        else:
            # Phase 1: Placeholder for expressions not yet parsed
            return IdentifierNode(define.name)
            
    def _translate_query(self, query: QueryNode) -> Any:
        """
        Translate CQL query expression to FHIRPath.
        
        Args:
            query: QueryNode to translate
            
        Returns:
            Translated FHIRPath expression
        """
        logger.debug("Translating enhanced query expression")
        
        # Start with source expression
        source_expr = self.translate_expression(query.source)
        
        # Handle with clauses if present
        if hasattr(query, 'with_clauses') and query.with_clauses:
            for with_clause in query.with_clauses:
                # With clauses in CQL are similar to let expressions
                # For Phase 2, we'll translate them to nested where clauses
                with_expr = self.translate_expression(with_clause)
                # This would need more sophisticated handling in a full implementation
                logger.debug(f"Processing with clause: {with_clause.identifier}")
        
        # Apply where clause if present
        if query.where_clause:
            where_expr = self.translate_expression(query.where_clause)
            # Create FHIRPath where() function call
            source_expr = FunctionCallNode("where", [where_expr])
        
        # Apply return clause if present
        if query.return_clause:
            return_expr = self.translate_expression(query.return_clause)
            # Create FHIRPath select() function call  
            source_expr = FunctionCallNode("select", [return_expr])
        
        # Apply sort clause if present
        if query.sort_clause:
            sort_expr = self.translate_expression(query.sort_clause)
            # For sorting, we'll need to generate SQL-level ORDER BY
            # This is handled in the SQL generation phase
            source_expr.sort_info = sort_expr
        
        return source_expr
        
    def _translate_context(self, context: ContextNode) -> Dict[str, Any]:
        """
        Translate CQL context statement.
        
        Args:
            context: ContextNode to translate
            
        Returns:
            Context information
        """
        logger.debug(f"Translating context: {context.context_name}")
        
        self.context.current_context = context.context_name
        return {
            'type': 'context',
            'name': context.context_name
        }
        
    def _translate_parameter(self, parameter: ParameterNode) -> Dict[str, Any]:
        """
        Translate CQL parameter definition.
        
        Args:
            parameter: ParameterNode to translate
            
        Returns:
            Parameter information
        """
        logger.debug(f"Translating parameter: {parameter.name}")
        
        param_info = {
            'name': parameter.name,
            'type': parameter.parameter_type
        }
        
        if parameter.default_value:
            param_info['default'] = self.translate_expression(parameter.default_value)
        
        # Store in context
        self.context.parameters[parameter.name] = param_info
        
        return param_info
        
    def _translate_include(self, include: IncludeNode) -> Dict[str, Any]:
        """
        Translate CQL include statement.
        
        Args:
            include: IncludeNode to translate
            
        Returns:
            Include information
        """
        logger.debug(f"Translating include: {include.library_name}")
        
        include_info = {
            'library': include.library_name,
            'version': include.version,
            'alias': include.alias
        }
        
        # Store in context
        self.context.includes[include.library_name] = include_info
        
        return include_info
            
    def set_context(self, context_name: str):
        """
        Set evaluation context.
        
        Args:
            context_name: Context name (e.g., "Patient", "Population")
        """
        logger.debug(f"Setting CQL context: {context_name}")
        self.context.current_context = context_name
        
    def get_definition(self, definition_name: str) -> Optional[Any]:
        """
        Get a definition from the current context.
        
        Args:
            definition_name: Name of definition to retrieve
            
        Returns:
            Definition expression or None if not found
        """
        return self.context.library_definitions.get(definition_name)
        
    def get_parameter(self, parameter_name: str) -> Optional[Any]:
        """
        Get a parameter from the current context.
        
        Args:
            parameter_name: Name of parameter to retrieve
            
        Returns:
            Parameter information or None if not found
        """
        return self.context.parameters.get(parameter_name)
        
    def _translate_with_clause(self, with_clause: WithClauseNode) -> Dict[str, Any]:
        """
        Translate CQL with clause.
        
        Args:
            with_clause: WithClauseNode to translate
            
        Returns:
            Translated with clause information
        """
        logger.debug(f"Translating with clause: {with_clause.identifier}")
        
        return {
            'type': 'with_clause',
            'identifier': with_clause.identifier,
            'expression': self.translate_expression(with_clause.expression)
        }
        
    def _translate_sort_clause(self, sort_clause: SortClauseNode) -> Dict[str, Any]:
        """
        Translate CQL sort clause.
        
        Args:
            sort_clause: SortClauseNode to translate
            
        Returns:
            Translated sort clause information
        """
        logger.debug("Translating sort clause")
        
        return {
            'type': 'sort_clause',
            'expression': self.translate_expression(sort_clause.expression),
            'direction': sort_clause.direction
        }
        
    def _translate_let_clause(self, let_clause: LetClauseNode) -> Dict[str, Any]:
        """
        Translate CQL let clause.
        
        Args:
            let_clause: LetClauseNode to translate
            
        Returns:
            Translated let clause information
        """
        logger.debug(f"Translating let clause: {let_clause.identifier}")
        
        return {
            'type': 'let_clause',
            'identifier': let_clause.identifier,
            'expression': self.translate_expression(let_clause.expression)
        }
        
    def _translate_temporal_operation(self, binary_op: BinaryOpNode) -> Any:
        """
        Translate CQL temporal operations.
        
        Args:
            binary_op: BinaryOpNode with temporal operator
            
        Returns:
            Translated temporal operation
        """
        logger.debug(f"Translating temporal operation: {binary_op.operator}")
        
        left_expr = self.translate_expression(binary_op.left)
        right_expr = self.translate_expression(binary_op.right)
        
        # Use comprehensive interval functions for temporal operations
        op_lower = binary_op.operator.lower()
        
        # Handle arithmetic operations with temporal units (e.g., + 1 year, - 3 months)
        # Note: These are now handled by the parser as function calls (add_years, etc.)
        if op_lower in ['+', '-']:
            # This path may not be reached now due to enhanced parsing
            logger.debug(f"Basic arithmetic operation {op_lower} - may need special handling")
        
        if op_lower in self.interval_functions.function_map:
            # Direct mapping to interval function
            handler = self.interval_functions.function_map[op_lower]
            if op_lower == 'during':
                return self.interval_functions.during_proper(left_expr, right_expr)
            elif op_lower == 'overlaps':
                return self.interval_functions.overlaps_proper(left_expr, right_expr)
            elif op_lower == 'starts':
                return self.interval_functions.starts(left_expr, right_expr)
            elif op_lower == 'ends':
                return self.interval_functions.ends(left_expr, right_expr)
            elif op_lower == 'meets':
                return self.interval_functions.meets(left_expr, right_expr)
            elif op_lower == 'before':
                return self.interval_functions.before(left_expr, right_expr)
            elif op_lower == 'after':
                return self.interval_functions.after(left_expr, right_expr)
            elif op_lower == 'includes':
                return self.interval_functions.includes(left_expr, right_expr)
            elif op_lower == 'same_as':
                return self.interval_functions.same_as(left_expr, right_expr)
            else:
                # Generic interval function call
                return FunctionCallNode(f"interval_{binary_op.operator}", [left_expr, right_expr])
        else:
            # For operators not yet implemented, create a function call
            return FunctionCallNode(f"temporal_{binary_op.operator}", [left_expr, right_expr])
            
    def _translate_clinical_function(self, func_call: FunctionCallNode) -> Any:
        """
        Translate CQL clinical functions.
        
        Args:
            func_call: FunctionCallNode for clinical function
            
        Returns:
            Translated clinical function
        """
        logger.debug(f"Translating clinical function: {func_call.name}")
        
        func_name = func_call.name.lower()
        args = [self.translate_expression(arg) for arg in func_call.args]
        
        # Map CQL clinical functions to implementations
        if func_name == 'ageinears':
            return ClinicalFunctions.age_in_years(args[0] if args else 'birthDate')
        elif func_name == 'ageinmonths':
            return ClinicalFunctions.age_in_months(args[0] if args else 'birthDate')
        elif func_name == 'mostrecent':
            return ClinicalLogicFunctions.most_recent(args[0] if args else [], args[1] if len(args) > 1 else 'effectiveDateTime')
        elif func_name == 'averagevalue':
            return ClinicalLogicFunctions.average_value(args[0] if args else [], args[1] if len(args) > 1 else 'valueQuantity.value')
        elif func_name == 'withinnormalrange':
            return ClinicalLogicFunctions.within_normal_range(args[0] if args else 0, args[1] if len(args) > 1 else 0, args[2] if len(args) > 2 else 100)
        else:
            # Return as regular function call for functions not yet implemented
            return FunctionCallNode(func_name, args)
    
    def _translate_general_function(self, func_call: FunctionCallNode) -> Any:
        """
        Translate general CQL functions using the function registry.
        
        Args:
            func_call: FunctionCallNode for general function
            
        Returns:
            Translated function call or SQL expression
        """
        logger.debug(f"Translating general function: {func_call.name}")
        
        func_name = func_call.name.lower()
        
        # Enhanced handling for statistical functions with complex arguments
        if func_name in ['stddev', 'stdev', 'variance', 'median', 'mode', 'percentile']:
            return self._translate_statistical_function_with_query(func_call)
        
        args = [self.translate_expression(arg) for arg in func_call.args]
        
        # Look up function in registry
        if func_name in self.function_registry:
            handler = self.function_registry[func_name]
            try:
                # Call the appropriate handler method based on handler type
                if hasattr(handler, 'generate_cql_datetime_function_sql'):
                    # DateTime function handler - use specific method
                    return handler.generate_cql_datetime_function_sql(func_name, args, self.dialect)
                elif hasattr(handler, 'generate_nullological_function_sql'):
                    # Nullological function handler - use specific method
                    return handler.generate_nullological_function_sql(func_name, args, self.dialect)
                elif hasattr(handler, 'generate_function_sql'):
                    # Generic function handler
                    return handler.generate_function_sql(func_name, args, self.dialect)
                elif hasattr(handler, func_name):
                    # Call the specific function method directly
                    method = getattr(handler, func_name)
                    return method(*args)
                else:
                    logger.warning(f"Handler for {func_name} found but no method available")
                    return FunctionCallNode(func_name, args)
            except Exception as e:
                logger.error(f"Error calling function handler for {func_name}: {e}")
                return FunctionCallNode(func_name, args)
        else:
            logger.warning(f"Function {func_name} not found in registry")
            return FunctionCallNode(func_name, args)
    
    def _translate_statistical_function_with_query(self, func_call: FunctionCallNode) -> Any:
        """
        Translate statistical functions that have complex query arguments.
        
        Handles patterns like:
        - StdDev([Observation: "Systolic Blood Pressure"] O return O.valueQuantity.value)
        - Count([Patient] P where P.active = true)
        
        Args:
            func_call: FunctionCallNode for statistical function with query
            
        Returns:
            Enhanced FunctionCallNode with proper query structure
        """
        logger.debug(f"Translating statistical function with query: {func_call.name}")
        
        func_name = func_call.name.lower()
        
        # Process each argument carefully
        processed_args = []
        for arg in func_call.args:
            if isinstance(arg, QueryNode):
                # This is a resource query with potential return clause
                logger.debug(f"Processing QueryNode for {func_name}")
                
                # Translate the query components
                source_expr = self.translate_expression(arg.source)
                
                # Build the query chain: source -> where -> return
                query_expr = source_expr
                
                # Apply where clause if present
                if arg.where_clause:
                    where_expr = self.translate_expression(arg.where_clause)
                    query_expr = FunctionCallNode("where", [query_expr, where_expr])
                
                # Apply return clause - this is crucial for statistical functions
                if arg.return_clause:
                    return_expr = self.translate_expression(arg.return_clause)
                    query_expr = FunctionCallNode("select", [query_expr, return_expr])
                else:
                    # If no explicit return clause, default to extracting the resource itself
                    logger.debug(f"No return clause found for {func_name}, using default resource selection")
                
                processed_args.append(query_expr)
                
            else:
                # Regular argument, translate normally
                processed_args.append(self.translate_expression(arg))
        
        # Now call the function handler with the properly structured arguments
        if func_name in self.function_registry:
            handler = self.function_registry[func_name]
            try:
                # For statistical functions, prefer the unified registry if available
                if hasattr(self, 'unified_registry') and self.unified_registry.can_handle_function(func_name):
                    return self._translate_function_via_registry(FunctionCallNode(func_name, processed_args))
                
                # Otherwise use standard handler
                if hasattr(handler, func_name):
                    method = getattr(handler, func_name)
                    return method(*processed_args)
                elif hasattr(handler, 'generate_function_sql'):
                    return handler.generate_function_sql(func_name, processed_args, self.dialect)
                else:
                    logger.warning(f"No appropriate method found for statistical function {func_name}")
                    return FunctionCallNode(func_name, processed_args)
                    
            except Exception as e:
                logger.error(f"Error translating statistical function {func_name}: {e}")
                return FunctionCallNode(func_name, processed_args)
        else:
            logger.warning(f"Statistical function {func_name} not found in registry")
            return FunctionCallNode(func_name, processed_args)

    def _translate_function_via_registry(self, func_call: FunctionCallNode) -> Any:
        """
        Translate CQL functions using the unified function registry.
        
        Args:
            func_call: FunctionCallNode for function call
            
        Returns:
            Translated function call with enhanced routing
        """
        logger.debug(f"Translating function via unified registry: {func_call.name}")
        
        func_name = func_call.name.lower()
        args = [self.translate_expression(arg) for arg in func_call.args]
        
        if not hasattr(self, 'unified_registry') or not self.unified_registry:
            logger.warning("Unified registry not available, falling back to standard function registry")
            return self._translate_general_function(func_call)
        
        try:
            # Get handler from unified registry
            handler = self.unified_registry.get_handler_for_function(func_name)
            if not handler:
                logger.warning(f"No handler found in unified registry for {func_name}")
                return FunctionCallNode(func_name, args)
            
            # Get handler info for debugging
            handler_info = self.unified_registry.get_handler_info(func_name)
            logger.debug(f"Using {handler_info['handler_name']} handler for {func_name}")
            
            # Try different handler method patterns
            if hasattr(handler, 'function_map') and func_name in handler.function_map:
                # Use function map for direct method access
                func_method = handler.function_map[func_name]
                logger.debug(f"Calling {func_name} via function map")
                result = func_method(*args)
                
                # Return LiteralNode for SQL expressions, or create FunctionCallNode
                if hasattr(result, 'value') and hasattr(result, 'type'):
                    # This is a LiteralNode with SQL
                    return result
                else:
                    # Return as literal value
                    return LiteralNode(value=str(result), type='value')
                    
            elif hasattr(handler, func_name):
                # Call method directly by name
                func_method = getattr(handler, func_name)
                logger.debug(f"Calling {func_name} via direct method")
                result = func_method(*args)
                
                if hasattr(result, 'value') and hasattr(result, 'type'):
                    return result
                else:
                    return LiteralNode(value=str(result), type='value')
                    
            elif hasattr(handler, 'generate_cql_function_sql'):
                # Use generic SQL generation method
                logger.debug(f"Calling {func_name} via generate_cql_function_sql")
                sql = handler.generate_cql_function_sql(func_name, args, self.dialect)
                return LiteralNode(value=sql, type='sql')
                
            else:
                logger.warning(f"Handler found but no callable method for {func_name}")
                return FunctionCallNode(func_name, args)
                
        except Exception as e:
            logger.error(f"Error translating function {func_name} via unified registry: {e}")
            # Fall back to standard function registry
            return self._translate_general_function(func_call)
            
    def _is_temporal_operator(self, operator: str) -> bool:
        """Check if operator is a temporal operator."""
        temporal_ops = ['during', 'overlaps', 'before', 'after', 'meets', 'starts', 'ends', 'includes', '+', '-']
        return operator.lower() in temporal_ops
        
    def _is_clinical_function(self, function_name: str) -> bool:
        """Check if function is a clinical function."""
        clinical_funcs = ['ageinyears', 'ageinmonths', 'mostrecent', 'averagevalue', 'withinnormalrange']
        return function_name.lower() in clinical_funcs