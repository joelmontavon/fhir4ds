"""
FHIR-specific function handler for FHIRPath operations.

This module implements FHIR-specific functions including:
- getValue() for extracting primitive values from FHIR elements
- resolve() for FHIR reference resolution
"""

from typing import List, Any
from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class FHIRFunctionHandler(FunctionHandler):
    """Handler for FHIR-specific FHIRPath functions."""
    
    def get_supported_functions(self) -> List[str]:
        """Return list of FHIR function names this handler supports."""
        return ['getvalue', 'resolve', 'extension']
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Execute the specified FHIR function."""
        func_name = function_name.lower()
        
        # Route to appropriate handler method
        if func_name == 'getvalue':
            return self._handle_getvalue(input_state, context, args)
        elif func_name == 'resolve':
            return self._handle_resolve(input_state, context, args)
        elif func_name == 'extension':
            return self._handle_extension(input_state, context, args)
        else:
            raise InvalidArgumentError(f"Unsupported FHIR function: {function_name}")
    
    # =================
    # Handler Methods
    # =================
    
    def _handle_getvalue(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle getValue() function for extracting primitive values from FHIR elements."""
        # getValue() function takes no arguments
        if args:
            raise ValueError("getValue() function takes no arguments")
        
        # Use dialect method for getValue primitive SQL
        sql_fragment = context.dialect.get_value_primitive_sql(input_state.sql_fragment)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_resolve(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle resolve() function for FHIR reference resolution."""
        # resolve() function takes no arguments
        if args:
            raise ValueError("resolve() function takes no arguments")
        
        # Use dialect method for resolve reference SQL
        base_expr = input_state.sql_fragment
        sql_fragment = context.dialect.resolve_reference_sql(base_expr)
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,  # resolve() returns a single resource, not an array
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_extension(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle extension(url) function for accessing FHIR extensions by URL."""
        # extension() function requires exactly one argument: the extension URL
        if len(args) != 1:
            raise ValueError("extension() function requires exactly one argument: the extension URL")
        
        # Extract URL from argument (handling LiteralOperation if needed)
        arg = args[0]
        if hasattr(arg, 'value'):
            # This is a LiteralOperation - extract the actual value
            extension_url = str(arg.value).strip("'\"")
        else:
            # Fallback to string conversion
            extension_url = str(arg).strip("'\"")
        
        # Generate SQL to find extension with matching URL
        # FHIR extensions are arrays of objects with 'url' and 'value*' properties
        sql_fragment = f"""
        (
            SELECT json_group_array(ext.value)
            FROM json_each({input_state.sql_fragment}, '$.extension') AS ext
            WHERE json_extract_string(ext.value, '$.url') = '{extension_url}'
        )
        """
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=True,  # extension() can return multiple matching extensions
            context_mode=ContextMode.COLLECTION
        )