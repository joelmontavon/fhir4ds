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


class HandlerRegistry:
    """Registry for function handlers to enable delegation pattern."""
    
    def __init__(self):
        self.handlers: List[FunctionHandler] = []
        self._initialize_handlers()
    
    def _initialize_handlers(self):
        """Initialize all available handlers."""
        from .handlers import (
            CollectionFunctionHandler,
            StringFunctionHandler as ModularStringHandler,
            MathFunctionHandler as ModularMathHandler,
            DateTimeFunctionHandler,
            TypeConversionFunctionHandler,
            ComparisonFunctionHandler,
            LogicalFunctionHandler,
            FHIRFunctionHandler,
            ProfileFunctionHandler,
            TerminologyFunctionHandler,
            ErrorFunctionHandler,
            QueryFunctionHandler,
            ListFunctionHandler,
            IntervalFunctionHandler
        )
        
        self.handlers = [
            CollectionFunctionHandler(),
            ModularStringHandler(),
            ModularMathHandler(),
            DateTimeFunctionHandler(),
            TypeConversionFunctionHandler(),
            ComparisonFunctionHandler(),
            LogicalFunctionHandler(),
            FHIRFunctionHandler(),
            ProfileFunctionHandler(),
            TerminologyFunctionHandler(),
            ErrorFunctionHandler(),
            QueryFunctionHandler(),
            ListFunctionHandler(),
            IntervalFunctionHandler(),
        ]
        
        logger.info(f"Initialized handler registry with {len(self.handlers)} handlers")
    
    def get_handler(self, function_name: str) -> Optional[FunctionHandler]:
        """Get the appropriate handler for a function name."""
        for handler in self.handlers:
            # Use supports_function for modular handlers, can_handle for legacy ones
            if hasattr(handler, 'supports_function'):
                can_handle = handler.supports_function(function_name)
            elif hasattr(handler, 'can_handle'):
                can_handle = handler.can_handle(function_name)
            else:
                can_handle = False
                
            if can_handle:
                logger.debug(f"Found {type(handler).__name__} for function '{function_name}'")
                return handler
        
        logger.debug(f"No specialized handler found for function '{function_name}'")
        return None
    
    def get_supported_functions(self) -> Dict[str, str]:
        """Get mapping of supported functions to their handler class names."""
        supported = {}
        for handler in self.handlers:
            handler_name = type(handler).__name__
            # Use get_supported_functions for modular handlers, supported_functions for legacy ones
            if hasattr(handler, 'get_supported_functions'):
                func_names = handler.get_supported_functions()
            elif hasattr(handler, 'supported_functions'):
                func_names = handler.supported_functions
            else:
                func_names = []
                
            for func_name in func_names:
                supported[func_name.lower()] = handler_name
        return supported