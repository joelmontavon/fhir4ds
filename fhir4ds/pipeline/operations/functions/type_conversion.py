"""
Type conversion function handler for FHIRPath operations.

This module implements type conversion functions including:
- Basic type conversions (toBoolean, toString, toInteger, toDecimal, toDate, toDateTime, toTime)
- Type checking functions (as, is, ofType)
- Type test functions (convertsTo*)
- FHIR constructors (Quantity, Code, ValueSet, Concept, Tuple)
"""

from typing import List, Any
from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class ConversionError(Exception):
    """Raised when AST conversion fails."""
    pass


class TypeConversionFunctionHandler(FunctionHandler):
    """Handler for type conversion and type checking FHIRPath functions."""
    
    def __init__(self, args: List[Any] = None):
        """Initialize with function arguments."""
        self.args = args or []
    
    def get_supported_functions(self) -> List[str]:
        """Return list of type conversion function names this handler supports."""
        return [
            'toboolean', 'tostring', 'tointeger', 'todecimal', 'todate', 'todatetime', 'totime',
            'toquantity', 'convertstoboolean', 'convertstodecimal', 'convertstointeger',
            'convertstodate', 'convertstodatetime', 'convertstotime', 'as', 'is', 'oftype',
            'quantity', 'valueset', 'code', 'toconcept', 'tuple'
        ]
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Execute the specified type conversion function."""
        # Set args for this function call
        self.args = args
        
        # Route to appropriate handler method
        handler_map = {
            'toboolean': self._handle_toboolean,
            'tostring': self._handle_tostring,
            'tointeger': self._handle_tointeger,
            'todecimal': self._handle_todecimal,
            'todate': self._handle_todate,
            'todatetime': self._handle_todatetime,
            'totime': self._handle_totime,
            'toquantity': self._handle_toquantity,
            'quantity': self._handle_quantity,
            'valueset': self._handle_valueset,
            'code': self._handle_code,
            'toconcept': self._handle_toconcept,
            'tuple': self._handle_tuple,
            'as': self._handle_type_checking,
            'is': self._handle_type_checking,
            'oftype': self._handle_type_checking,
        }
        
        # Handle convertsTo* functions
        if function_name.startswith('convertsto'):
            return self._handle_converts_functions(input_state, context)
        
        handler_func = handler_map.get(function_name.lower())
        if not handler_func:
            raise InvalidArgumentError(f"Unsupported type conversion function: {function_name}")
        
        return handler_func(input_state, context)
    
    # =================
    # Handler Methods
    # =================
    
    def _handle_toboolean(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toBoolean() function."""
        sql_fragment = f"""
        CASE 
            WHEN LOWER({input_state.sql_fragment}) IN ('true', '1', 't', 'yes', 'y') THEN TRUE
            WHEN LOWER({input_state.sql_fragment}) IN ('false', '0', 'f', 'no', 'n') THEN FALSE
            ELSE NULL
        END
        """
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_tostring(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toString() function."""
        sql_fragment = f"CAST({input_state.sql_fragment} AS TEXT)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_tointeger(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toInteger() function."""
        sql_fragment = f"CAST({input_state.sql_fragment} AS INTEGER)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_todecimal(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toDecimal() function."""
        sql_fragment = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_todate(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toDate() function.
        
        Converts a string to a date value, or returns empty if conversion fails.
        """
        sql_fragment = context.dialect.try_cast(input_state.sql_fragment, 'date')
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_todatetime(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toDateTime() function.
        
        Converts a string to a datetime/timestamp value, or returns empty if conversion fails.
        """
        sql_fragment = context.dialect.try_cast(input_state.sql_fragment, 'timestamp')
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_totime(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toTime() function.
        
        Converts a string to a time value, or returns empty if conversion fails.
        """
        sql_fragment = context.dialect.try_cast(input_state.sql_fragment, 'time')
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_toquantity(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle toQuantity() function.
        
        Converts a string or number to a FHIR Quantity. For SQL purposes, we'll try to
        parse numeric values and return them as decimals.
        """
        sql_fragment = context.dialect.try_cast(input_state.sql_fragment, 'decimal')
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_quantity(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle Quantity() constructor function.
        
        Creates a FHIR Quantity from value and unit arguments.
        Expected usage: Quantity(value, unit) or Quantity(value, 'unit_string')
        """
        if len(self.args) < 2:
            raise InvalidArgumentError("Quantity function requires at least 2 arguments: value and unit")
        
        # Extract raw values from arguments, handling literal() wrappers
        value_arg = str(self.args[0])
        if value_arg.startswith("literal(") and value_arg.endswith(")"):
            value_arg = value_arg[8:-1]  # Remove literal() wrapper
        
        unit_arg = str(self.args[1])
        if unit_arg.startswith("literal(") and unit_arg.endswith(")"):
            unit_arg = unit_arg[8:-1]  # Remove literal() wrapper
        
        # Remove quotes from unit if it's a string literal
        if unit_arg.startswith("'") and unit_arg.endswith("'"):
            unit_arg = unit_arg[1:-1]
        
        # Generate JSON object for the quantity
        json_str = f'{{"value": {value_arg}, "unit": "{unit_arg}"}}'
        sql_fragment = f"'{json_str}'"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_valueset(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle System.ValueSet constructor function.
        
        Creates a FHIR ValueSet system object with provided properties.
        Expected usage: System.ValueSet{id: '123'} or ValueSet{id: '123', name: 'test'}
        """
        # For system constructors, the arguments come as property assignments
        # The parser handles these as key-value pairs from the {} syntax
        if not self.args:
            # Empty constructor - create minimal ValueSet object
            json_str = '{"resourceType": "ValueSet"}'
        else:
            # Build ValueSet object from provided properties
            properties = []
            properties.append('"resourceType": "ValueSet"')
            
            # Process arguments as property assignments
            for i, arg in enumerate(self.args):
                arg_str = str(arg)
                # Handle key-value pairs from {id: '123'} syntax
                if ':' in arg_str:
                    # Split key-value pair and clean up
                    key_value = arg_str.split(':', 1)
                    key = key_value[0].strip().strip("'\"")
                    value = key_value[1].strip()
                    
                    # Ensure value is properly quoted for JSON
                    if not (value.startswith('"') and value.endswith('"')) and not (value.startswith("'") and value.endswith("'")):
                        if value.isdigit():
                            # Numeric value - don't quote
                            pass
                        else:
                            # String value - quote it
                            value = f'"{value}"'
                    elif value.startswith("'") and value.endswith("'"):
                        # Convert single quotes to double quotes for JSON
                        value = f'"{value[1:-1]}"'
                    
                    properties.append(f'"{key}": {value}')
            
            json_str = '{' + ', '.join(properties) + '}'
        
        sql_fragment = f"'{json_str}'"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_code(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle Code constructor function.
        
        Creates a FHIR Code system object with provided properties.
        Expected usage: Code { code: '8480-6' } or Code { code: '8480-6', system: 'http://loinc.org' }
        """
        # For system constructors, the arguments come as property assignments
        # The parser handles these as key-value pairs from the {} syntax
        if not self.args:
            # Empty constructor - create minimal Code object
            json_str = '{"resourceType": "Code"}'
        else:
            # Build Code object from provided properties
            properties = []
            properties.append('"resourceType": "Code"')
            
            # Process arguments as property assignments
            for i, arg in enumerate(self.args):
                arg_str = str(arg)
                # Handle key-value pairs from {code: '8480-6'} syntax
                if ':' in arg_str:
                    # Split key-value pair and clean up
                    key_value = arg_str.split(':', 1)
                    key = key_value[0].strip().strip("'\"")
                    value = key_value[1].strip()
                    
                    # Ensure value is properly quoted for JSON
                    if not (value.startswith('"') and value.endswith('"')) and not (value.startswith("'") and value.endswith("'")):
                        if value.isdigit():
                            # Numeric value - don't quote
                            pass
                        else:
                            # String value - quote it
                            value = f'"{value}"'
                    elif value.startswith("'") and value.endswith("'"):
                        # Convert single quotes to double quotes for JSON
                        value = f'"{value[1:-1]}"'
                    
                    properties.append(f'"{key}": {value}')
            
            json_str = '{' + ', '.join(properties) + '}'
        
        sql_fragment = f"'{json_str}'"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_toconcept(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle ToConcept function.
        
        Converts a Code object to a Concept object.
        Expected usage: ToConcept(Code { code: '8480-6' })
        """
        if not self.args:
            raise InvalidArgumentError("ToConcept function requires 1 argument: a Code object")
        
        # The input argument should be a Code object (JSON string)
        # We need to wrap it in a Concept structure
        code_sql = self._evaluate_logical_argument(self.args[0], input_state, context)
        
        # Generate Concept JSON structure with the Code as the 'codes' property
        # The expected output format is: Concept { codes: Code { code: '8480-6' } }
        concept_json = f'{{"resourceType": "Concept", "codes": {code_sql}}}'
        sql_fragment = f"'{concept_json}'"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_tuple(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle Tuple constructor function.
        
        Creates a CQL Tuple object from key-value pairs.
        Expected usage: Tuple { Id : 1, Name : 'John' }
        """
        # Tuple constructor takes arguments as property assignments from {} syntax
        if not self.args:
            # Empty tuple constructor - create empty tuple object
            json_str = '{}'
        else:
            # Build tuple object from provided property assignments
            properties = []
            
            # Process arguments as property assignments
            for i, arg in enumerate(self.args):
                arg_str = str(arg)
                # Handle key-value pairs from { Id : 1, Name : 'John' } syntax
                if ':' in arg_str:
                    # Split key-value pair and clean up
                    key_value = arg_str.split(':', 1)
                    key = key_value[0].strip().strip("'\"")
                    value = key_value[1].strip()
                    
                    # Ensure value is properly quoted for JSON
                    if not (value.startswith('"') and value.endswith('"')) and not (value.startswith("'") and value.endswith("'")):
                        if value.isdigit():
                            # Numeric value - don't quote
                            pass
                        else:
                            # String value - quote it
                            value = f'"{value}"'
                    elif value.startswith("'") and value.endswith("'"):
                        # Convert single quotes to double quotes for JSON
                        value = f'"{value[1:-1]}"'
                    
                    properties.append(f'"{key}": {value}')
            
            json_str = '{' + ', '.join(properties) + '}'
        
        sql_fragment = f"'{json_str}'"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_converts_functions(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle convertsTo* functions.
        
        These functions test whether a value can be converted to a specific type.
        They return true/false without performing the actual conversion.
        """
        # Extract the target type from function name (e.g., 'convertstoboolean' -> 'boolean')
        target_type = self.func_name.replace('convertsto', '').lower()
        
        sql_fragment = context.dialect.generate_converts_to_check(input_state.sql_fragment, target_type)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_type_checking(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle as/is/ofType functions.
        
        - as(type): Cast to the specified type, returns empty if cast fails
        - is(type): Returns true if the value is of the specified type  
        - ofType(type): Filters a collection to include only items of the specified type
        """
        if not self.args:
            raise InvalidArgumentError(f"{self.func_name}() function requires a type argument")
        
        target_type = str(self.args[0]).lower().strip("'\"")
        
        if self.func_name == 'as':
            # Cast to target type, return null if cast fails
            sql_fragment = context.dialect.generate_type_cast(input_state.sql_fragment, target_type)
            
            return self._create_scalar_result(input_state, sql_fragment)
            
        elif self.func_name == 'is':
            # Check if value is of the specified type
            if target_type == 'string':
                sql_fragment = f"({context.dialect.generate_json_typeof(input_state.sql_fragment)} = 'string')"
            elif target_type == 'number':
                sql_fragment = f"({context.dialect.generate_json_typeof(input_state.sql_fragment)} = 'number')"
            elif target_type == 'boolean':
                sql_fragment = f"({context.dialect.generate_json_typeof(input_state.sql_fragment)} = 'boolean')"
            else:
                # Default to true for basic type checking
                sql_fragment = "true"
            
            return self._create_scalar_result(input_state, sql_fragment)
            
        elif self.func_name == 'oftype':
            # Filter collection to include only items of specified type
            sql_fragment = context.dialect.generate_of_type_filter(input_state.sql_fragment, target_type)
            
            return self._create_collection_result(input_state, sql_fragment)
        
        else:
            raise InvalidArgumentError(f"Unknown type checking function: {self.func_name}")
    
    # =================
    # Helper Methods
    # =================
    
    def _create_scalar_result(self, input_state: SQLState, sql_fragment: str) -> SQLState:
        """Helper to create scalar results consistently."""
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _create_collection_result(self, input_state: SQLState, sql_fragment: str) -> SQLState:
        """Helper to create collection results consistently."""
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True,
            context_mode=ContextMode.COLLECTION
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
    
    def _convert_ast_argument_to_sql(self, ast_node: Any, input_state: SQLState,
                                    context: ExecutionContext) -> str:
        """Convert AST node argument to SQL fragment."""
        from ...converters.ast_converter import ASTToPipelineConverter
        
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