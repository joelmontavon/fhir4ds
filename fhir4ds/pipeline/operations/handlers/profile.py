"""
Profile validation function handler for FHIRPath operations.

This module implements profile validation functions including:
- elementDefinition() for retrieving element definitions from profiles
- slice() for profile slicing operations
- checkModifiers() for modifier extension validation
"""

from typing import List, Any
from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class ProfileFunctionHandler(FunctionHandler):
    """Handler for profile validation FHIRPath functions."""
    
    def get_supported_functions(self) -> List[str]:
        """Return list of profile function names this handler supports."""
        return ['elementdefinition', 'slice', 'checkmodifiers']
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Execute the specified profile function."""
        func_name = function_name.lower()
        
        # Route to appropriate handler method
        if func_name == 'elementdefinition':
            return self._handle_elementdefinition(input_state, context, args)
        elif func_name == 'slice':
            return self._handle_slice(input_state, context, args)
        elif func_name == 'checkmodifiers':
            return self._handle_checkmodifiers(input_state, context, args)
        else:
            raise InvalidArgumentError(f"Unsupported profile function: {function_name}")
    
    def _handle_elementdefinition(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle elementDefinition() function for profile element definitions."""
        # elementDefinition() takes no arguments
        if args:
            raise ValueError("elementDefinition() function takes no arguments")
        
        # Use dialect method for profile element definition SQL
        sql_fragment = context.dialect.generate_profile_element_definition(input_state.sql_fragment)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_slice(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle slice() function for profile slicing."""
        # slice() requires exactly two arguments: slice name and discriminator
        if len(args) != 2:
            raise ValueError("slice() function requires exactly two arguments")
        
        slice_name = str(args[0])
        discriminator = str(args[1])
        
        # Use dialect method for profile slicing SQL
        sql_fragment = context.dialect.generate_profile_slice(input_state.sql_fragment, slice_name, discriminator)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_checkmodifiers(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle checkModifiers() function for modifier extension validation."""
        # checkModifiers() requires exactly one argument: list of known modifier URLs
        if len(args) != 1:
            raise ValueError("checkModifiers() function requires exactly one argument")
        
        known_modifiers = str(args[0])
        
        # Use dialect method for modifier checking SQL
        sql_fragment = context.dialect.generate_check_modifiers(input_state.sql_fragment, known_modifiers)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )