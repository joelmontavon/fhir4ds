"""
Function Handler Classes for FunctionCallOperation Refactoring.

This module contains the base classes and specific handler implementations
to break down the FunctionCallOperation god class into manageable components.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Callable

from ..core.base import PipelineOperation, SQLState, ExecutionContext

logger = logging.getLogger(__name__)


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class FunctionHandler(ABC):
    """
    Base class for function handlers.
    
    Each handler focuses on a specific category of functions to improve
    maintainability and reduce complexity.
    """
    
    def __init__(self):
        self.supported_functions = self._get_supported_functions()
    
    @abstractmethod
    def _get_supported_functions(self) -> List[str]:
        """Return list of function names this handler supports."""
        pass
    
    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can process the given function."""
        return function_name.lower() in [f.lower() for f in self.supported_functions]
    
    @abstractmethod
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle the function call and return updated SQL state."""
        pass


class MathFunctionHandler(FunctionHandler):
    """Handler for mathematical operations."""
    
    def _get_supported_functions(self) -> List[str]:
        return [
            'abs', 'ceiling', 'floor', 'round', 'sqrt', 'power',
            'min', 'max', 'sum', 'avg', 'add', 'subtract', 
            'multiply', 'divide', 'mod'
        ]
    
    def handle_function(self, function_name: str, input_state: SQLState,
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle mathematical function calls."""
        func_name = function_name.lower()
        
        if func_name == 'abs':
            return self._handle_abs(input_state, context, args)
        elif func_name in ['add', 'addition']:
            return self._handle_addition(input_state, context, args)
        elif func_name == 'ceiling':
            return self._handle_ceiling(input_state, context, args)
        elif func_name == 'floor':
            return self._handle_floor(input_state, context, args)
        else:
            raise InvalidArgumentError(f"Math function '{function_name}' not yet implemented in handler")
    
    def _handle_abs(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle absolute value function."""
        logger.debug("Math handler: Processing abs() function")
        
        if not args:
            raise InvalidArgumentError("abs() function requires at least one argument")
        
        arg_expression = str(args[0]) if args else input_state.sql_fragment
        
        # Generate SQL based on dialect
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"abs({arg_expression})"
        else:  # PostgreSQL
            sql_fragment = f"abs({arg_expression})"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment
        )
    
    def _handle_addition(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle addition operation."""
        logger.debug("Math handler: Processing addition() function")
        
        if len(args) < 2:
            raise InvalidArgumentError("Addition requires at least two arguments")
        
        # Convert all arguments to SQL expressions
        arg_expressions = [str(arg) for arg in args]
        
        # Generate addition SQL
        sql_fragment = f"({' + '.join(arg_expressions)})"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment
        )
    
    def _handle_ceiling(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle ceiling function."""
        logger.debug("Math handler: Processing ceiling() function")
        
        if not args:
            raise InvalidArgumentError("ceiling() function requires at least one argument")
        
        arg_expression = str(args[0])
        
        # Generate SQL based on dialect
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"ceiling({arg_expression})"
        else:  # PostgreSQL
            sql_fragment = f"ceil({arg_expression})"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment
        )
    
    def _handle_floor(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle floor function."""
        logger.debug("Math handler: Processing floor() function")
        
        if not args:
            raise InvalidArgumentError("floor() function requires at least one argument")
        
        arg_expression = str(args[0])
        
        sql_fragment = f"floor({arg_expression})"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment
        )


class StringFunctionHandler(FunctionHandler):
    """Handler for string operations."""
    
    def _get_supported_functions(self) -> List[str]:
        return [
            'upper', 'lower', 'startswith', 'endswith', 'contains',
            'matches', 'replace', 'split', 'substring', 'indexof', 'length'
        ]
    
    def handle_function(self, function_name: str, input_state: SQLState,
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle string function calls."""
        func_name = function_name.lower()
        
        if func_name == 'upper':
            return self._handle_upper(input_state, context, args)
        elif func_name == 'lower':
            return self._handle_lower(input_state, context, args)
        elif func_name == 'length':
            return self._handle_length(input_state, context, args)
        else:
            raise InvalidArgumentError(f"String function '{function_name}' not yet implemented in handler")
    
    def _handle_upper(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle upper case conversion."""
        logger.debug("String handler: Processing upper() function")
        
        arg_expression = str(args[0]) if args else input_state.sql_fragment
        sql_fragment = f"upper({arg_expression})"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment
        )
    
    def _handle_lower(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle lower case conversion."""
        logger.debug("String handler: Processing lower() function")
        
        arg_expression = str(args[0]) if args else input_state.sql_fragment
        sql_fragment = f"lower({arg_expression})"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment
        )
    
    def _handle_length(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle string length function."""
        logger.debug("String handler: Processing length() function")
        
        arg_expression = str(args[0]) if args else input_state.sql_fragment
        
        # Generate SQL based on dialect
        if context.dialect.name.upper() == 'DUCKDB':
            sql_fragment = f"length({arg_expression})"
        else:  # PostgreSQL
            sql_fragment = f"char_length({arg_expression})"
        
        return SQLState(
            base_table=input_state.base_table,
            json_column=input_state.json_column,
            sql_fragment=sql_fragment
        )


class HandlerRegistry:
    """Registry for function handlers to enable delegation pattern."""
    
    def __init__(self):
        self.handlers: List[FunctionHandler] = []
        self._initialize_handlers()
    
    def _initialize_handlers(self):
        """Initialize all available handlers."""
        from .functions import (
            CollectionFunctionHandler,
            StringFunctionHandler as ModularStringHandler,
            MathFunctionHandler as ModularMathHandler,
            DateTimeFunctionHandler,
            TypeConversionFunctionHandler,
            ComparisonFunctionHandler
        )
        
        self.handlers = [
            CollectionFunctionHandler(),
            ModularStringHandler(),
            ModularMathHandler(),
            DateTimeFunctionHandler(),
            TypeConversionFunctionHandler(),
            ComparisonFunctionHandler(),
            # Legacy handlers for backward compatibility
            MathFunctionHandler(),
            StringFunctionHandler(),
        ]
        
        logger.info(f"Initialized handler registry with {len(self.handlers)} handlers")
    
    def get_handler(self, function_name: str) -> Optional[FunctionHandler]:
        """Get the appropriate handler for a function name."""
        for handler in self.handlers:
            if handler.can_handle(function_name):
                logger.debug(f"Found {type(handler).__name__} for function '{function_name}'")
                return handler
        
        logger.debug(f"No specialized handler found for function '{function_name}'")
        return None
    
    def get_supported_functions(self) -> Dict[str, str]:
        """Get mapping of supported functions to their handler class names."""
        supported = {}
        for handler in self.handlers:
            handler_name = type(handler).__name__
            for func_name in handler.supported_functions:
                supported[func_name.lower()] = handler_name
        return supported