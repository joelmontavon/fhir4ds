"""
Base class for function handlers in the modular FHIRPath operation system.
"""

from abc import ABC, abstractmethod
from typing import List, Any
from ...core.base import SQLState, ExecutionContext


class FunctionHandler(ABC):
    """
    Abstract base class for specialized function handlers.
    
    Each handler is responsible for a specific category of FHIRPath functions
    (e.g., collection, string, math, etc.) and implements the execution logic
    for those functions while delegating SQL generation to dialect classes.
    """
    
    @abstractmethod
    def get_supported_functions(self) -> List[str]:
        """Return list of function names this handler supports."""
        pass
    
    @abstractmethod
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """
        Execute the specified function with given arguments.
        
        Args:
            function_name: Name of the function to execute
            input_state: Current SQL state
            context: Execution context containing dialect and other settings
            args: Function arguments
            
        Returns:
            Updated SQL state after function execution
        """
        pass
    
    def supports_function(self, function_name: str) -> bool:
        """Check if this handler supports the given function."""
        return function_name.lower() in [f.lower() for f in self.get_supported_functions()]
    
    def _create_scalar_result(self, input_state: SQLState, sql_fragment: str) -> SQLState:
        """Helper to create scalar result state."""
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=getattr(input_state, 'context_mode', None)
        )
    
    def _create_collection_result(self, input_state: SQLState, sql_fragment: str) -> SQLState:
        """Helper to create collection result state."""
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=True,
            context_mode=getattr(input_state, 'context_mode', None)
        )