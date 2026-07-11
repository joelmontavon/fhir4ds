"""
Terminology function handler for FHIRPath operations.

This module implements terminology-related functions including:
- Unit compatibility functions (comparable)
- Concept hierarchy functions (subsumes, subsumedBy)
"""

from typing import List, Any
from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class TerminologyFunctionHandler(FunctionHandler):
    """Handler for terminology-related FHIRPath functions."""
    
    def __init__(self, args: List[Any] = None):
        """Initialize with function arguments."""
        self.args = args or []
    
    def get_supported_functions(self) -> List[str]:
        """Return list of terminology function names this handler supports."""
        return [
            'comparable', 'subsumes', 'subsumedby'
        ]
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Execute the specified terminology function."""
        # Set args for this function call
        self.args = args
        
        # Route to appropriate handler method
        handler_map = {
            'comparable': self._handle_comparable,
            'subsumes': self._handle_subsumes,
            'subsumedby': self._handle_subsumedby,
        }
        
        handler_func = handler_map.get(function_name.lower())
        if not handler_func:
            raise InvalidArgumentError(f"Unsupported terminology function: {function_name}")
        
        return handler_func(input_state, context)
    
    # =================
    # Handler Methods
    # =================
    
    def _handle_comparable(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle comparable(unit) function.
        
        Tests if the quantity is comparable to the specified unit (compatible units).
        """
        if not self.args:
            raise InvalidArgumentError("comparable() function requires one argument: the target unit")
        
        # Extract unit from argument (handling LiteralOperation if needed)
        arg = self.args[0]
        if hasattr(arg, 'value'):
            # This is a LiteralOperation - extract the actual value
            target_unit = str(arg.value).strip("'\"")
        else:
            # Fallback to string conversion
            target_unit = str(arg).strip("'\"")
        
        # Generate SQL for unit compatibility check
        sql_fragment = f"""
        CASE 
            WHEN {input_state.sql_fragment} IS NULL THEN NULL
            WHEN json_extract_string({input_state.sql_fragment}, '$.unit') = '{target_unit}' THEN TRUE
            WHEN json_extract_string({input_state.sql_fragment}, '$.code') = '{target_unit}' THEN TRUE
            ELSE FALSE
        END
        """
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_subsumes(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle subsumes(concept) function.
        
        Tests if the current concept subsumes (is a parent/ancestor of) the specified concept.
        """
        if not self.args:
            raise InvalidArgumentError("subsumes() function requires one argument: the concept to test")
        
        # Extract concept from argument (handling LiteralOperation if needed)
        arg = self.args[0]
        if hasattr(arg, 'value'):
            # This is a LiteralOperation - extract the actual value
            target_concept = str(arg.value).strip("'\"")
        else:
            # Fallback to string conversion
            target_concept = str(arg).strip("'\"")
        
        # Generate SQL for concept subsumption check
        # For now, implement simple equality check - would need terminology service for full hierarchy
        sql_fragment = f"""
        CASE 
            WHEN {input_state.sql_fragment} IS NULL THEN NULL
            WHEN json_extract_string({input_state.sql_fragment}, '$.code') = '{target_concept}' THEN TRUE
            ELSE FALSE
        END
        """
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_subsumedby(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle subsumedBy(concept) function.
        
        Tests if the current concept is subsumed by (is a child/descendant of) the specified concept.
        """
        if not self.args:
            raise InvalidArgumentError("subsumedBy() function requires one argument: the parent concept")
        
        # Extract concept from argument (handling LiteralOperation if needed)
        arg = self.args[0]
        if hasattr(arg, 'value'):
            # This is a LiteralOperation - extract the actual value
            parent_concept = str(arg.value).strip("'\"")
        else:
            # Fallback to string conversion
            parent_concept = str(arg).strip("'\"")
        
        # Generate SQL for concept subsumption check
        # For now, implement simple equality check - would need terminology service for full hierarchy
        sql_fragment = f"""
        CASE 
            WHEN {input_state.sql_fragment} IS NULL THEN NULL
            WHEN json_extract_string({input_state.sql_fragment}, '$.code') = '{parent_concept}' THEN TRUE
            ELSE FALSE
        END
        """
        
        return input_state.evolve(
            sql_fragment=sql_fragment.strip(),
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )