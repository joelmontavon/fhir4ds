"""
String Function Handler for FHIRPath operations.

This module implements all string-related functions from the FunctionCallOperation class,
providing specialized handling for string operations with dialect-specific optimizations.
"""

from typing import List, Any
import logging
from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode

logger = logging.getLogger(__name__)


def _extract_literal_value(arg) -> str:
    """
    Extract the actual value from an AST node argument.
    
    This handles LiteralNode and other AST node types to extract their values
    instead of returning the string representation of the node object.
    
    Args:
        arg: AST node argument (typically LiteralNode)
        
    Returns:
        The actual value as a string
    """
    from ...fhirpath.parser.ast_nodes import LiteralNode
    
    if isinstance(arg, LiteralNode):
        return str(arg.value)
    elif hasattr(arg, 'value'):
        return str(arg.value)
    elif hasattr(arg, 'literal_value'):
        return str(arg.literal_value)
    else:
        # Fallback to string representation
        return str(arg)


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class StringFunctionHandler(FunctionHandler):
    """
    Handler for string-related FHIRPath functions.
    
    Supports all string manipulation functions including:
    - substring, contains, startsWith, endsWith
    - upper, lower, trim, replace
    - split, join, indexOf, toChars
    - matches, replaceMatches
    """
    
    def get_supported_functions(self) -> List[str]:
        """Return list of function names this handler supports."""
        return [
            'substring', 'contains_string', 'startswith', 'endswith',
            'upper', 'lower', 'trim', 'replace', 'split', 'join', 
            'indexof', 'tochars', 'matches', 'replacematches', '+'
        ]
    
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
        func_name = function_name.lower()
        logger.debug(f"StringFunctionHandler: Processing {func_name}() function")
        
        # Route to appropriate handler method
        if func_name == 'substring':
            return self._handle_substring(input_state, context, args)
        elif func_name == 'contains_string':
            return self._handle_contains_string(input_state, context, args)
        elif func_name == 'startswith':
            return self._handle_startswith(input_state, context, args)
        elif func_name == 'endswith':
            return self._handle_endswith(input_state, context, args)
        elif func_name == 'upper':
            return self._handle_upper(input_state, context, args)
        elif func_name == 'lower':
            return self._handle_lower(input_state, context, args)
        elif func_name == 'trim':
            return self._handle_trim(input_state, context, args)
        elif func_name == 'replace':
            return self._handle_replace(input_state, context, args)
        elif func_name == 'split':
            return self._handle_split(input_state, context, args)
        elif func_name == 'join':
            return self._handle_join(input_state, context, args)
        elif func_name == 'indexof':
            return self._handle_indexof(input_state, context, args)
        elif func_name == 'tochars':
            return self._handle_tochars(input_state, context, args)
        elif func_name == 'matches':
            return self._handle_matches(input_state, context, args)
        elif func_name == 'replacematches':
            return self._handle_replacematches(input_state, context, args)
        elif func_name == '+':
            return self._handle_concatenation(input_state, context, args)
        else:
            raise InvalidArgumentError(f"Unsupported string function: {function_name}")
    
    def _handle_substring(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle substring() function."""
        if len(args) < 1:
            raise ValueError("substring() requires at least start position")
        
        start_pos = _extract_literal_value(args[0])
        
        if len(args) > 1:
            sql_fragment = context.dialect.generate_substring_sql(input_state.sql_fragment, start_pos, _extract_literal_value(args[1]))
        else:
            sql_fragment = context.dialect.generate_substring_sql(input_state.sql_fragment, start_pos)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_contains_string(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle contains() for strings."""
        if not args:
            raise ValueError("contains() requires a search string argument")
        
        search_string = _extract_literal_value(args[0])
        cast_operand = f"CAST({input_state.sql_fragment} AS VARCHAR)"
        sql_fragment = f"({cast_operand} LIKE '%{search_string}%')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_startswith(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle startsWith() function."""
        if not args:
            raise ValueError("startsWith() requires a prefix string argument")
        
        prefix = _extract_literal_value(args[0])
        cast_operand = f"CAST({input_state.sql_fragment} AS VARCHAR)"
        sql_fragment = f"({cast_operand} LIKE '{prefix}%')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_endswith(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle endsWith() function."""
        if not args:
            raise ValueError("endsWith() requires a suffix string argument")
        
        suffix = _extract_literal_value(args[0])
        cast_operand = f"CAST({input_state.sql_fragment} AS VARCHAR)"
        sql_fragment = f"({cast_operand} LIKE '%{suffix}')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_upper(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle upper()/toUpper() function."""
        # Cast to string to handle JSON values properly
        cast_operand = f"CAST({input_state.sql_fragment} AS VARCHAR)"
        sql_fragment = f"UPPER({cast_operand})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_lower(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle lower()/toLower() function."""
        # Cast to string to handle JSON values properly
        cast_operand = f"CAST({input_state.sql_fragment} AS VARCHAR)"
        sql_fragment = f"LOWER({cast_operand})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_trim(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle trim() function."""
        # Cast to string to handle JSON values properly
        cast_operand = f"CAST({input_state.sql_fragment} AS VARCHAR)"
        sql_fragment = f"TRIM({cast_operand})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_replace(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle replace() function."""
        if len(args) < 2:
            raise ValueError("replace() requires search and replacement strings")
        
        search_str = _extract_literal_value(args[0])
        replace_str = _extract_literal_value(args[1])
        cast_operand = f"CAST({input_state.sql_fragment} AS VARCHAR)"
        sql_fragment = f"REPLACE({cast_operand}, '{search_str}', '{replace_str}')"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_split(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle split() function.
        
        Splits a string into an array of substrings using the specified separator.
        """
        if not args:
            raise InvalidArgumentError("split() function requires a separator argument")
        
        separator = str(args[0]).strip("'\"")  # Remove quotes if present
        sql_fragment = context.dialect.split_string(input_state.sql_fragment, separator)
        
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_join(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle join() function - concatenate array elements with separator."""
        from ...fhirpath.parser.ast_nodes import LiteralNode
        
        if not args:
            # Default separator is empty string
            separator = "''"
        else:
            # Get separator from raw AST argument (LiteralNode)
            arg = args[0]
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
    
    def _handle_indexof(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle indexOf() function.
        
        Returns the 0-based index of the first occurrence of the substring, or -1 if not found.
        """
        if not args:
            raise InvalidArgumentError("indexOf() function requires a substring argument")
        
        substring = str(args[0])
        position_sql = context.dialect.string_position(substring, input_state.sql_fragment)
        # Convert 1-based position to 0-based, return -1 if not found
        sql_fragment = f"(CASE WHEN {position_sql} > 0 THEN {position_sql} - 1 ELSE -1 END)"
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_tochars(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle toChars() function.
        
        Splits a string into an array of individual characters.
        """
        sql_fragment = context.dialect.string_to_char_array(input_state.sql_fragment)
        return self._create_collection_result(input_state, sql_fragment)
    
    def _handle_matches(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle matches() function.
        
        Tests whether a string matches a regular expression pattern.
        Returns true if the string matches the pattern, false otherwise.
        """
        if not args:
            raise InvalidArgumentError("matches() function requires a regular expression pattern")
        
        pattern = str(args[0])
        sql_fragment = context.dialect.regex_matches(input_state.sql_fragment, pattern)
        
        return self._create_scalar_result(input_state, sql_fragment)
    
    def _handle_replacematches(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle replaceMatches() function.
        
        Replaces all occurrences of a regular expression pattern with a replacement string.
        """
        if len(args) < 2:
            raise InvalidArgumentError("replaceMatches() function requires pattern and replacement arguments")
        
        pattern = str(args[0])
        replacement = str(args[1])
        sql_fragment = context.dialect.regex_replace(input_state.sql_fragment, pattern, replacement)
        
        return self._create_scalar_result(input_state, sql_fragment)

    def _handle_concatenation(self, input_state: SQLState, context: ExecutionContext, args: List[Any]) -> SQLState:
        """Handle + (concatenation) operator for strings."""
        if len(args) != 2:
            raise InvalidArgumentError("Concatenation (+) requires exactly two arguments (left and right operands)")
        
        # For binary operators, args[0] is left operand, args[1] is right operand
        from ...operations.literals import LiteralOperation
        from ...core.base import SQLState, ContextMode
        
        # Convert left operand
        left_arg = args[0]
        if hasattr(left_arg, 'sql_fragment'):
            left_operand = left_arg.sql_fragment
        else:
            left_operand = str(left_arg)
            if not left_operand.startswith("'") and not left_operand.startswith('"'):
                left_operand = f"'{left_operand}'"
        
        # Convert right operand
        right_arg = args[1]
        if hasattr(right_arg, 'sql_fragment'):
            right_operand = right_arg.sql_fragment
        else:
            right_operand = str(right_arg)
            if not right_operand.startswith("'") and not right_operand.startswith('"'):
                right_operand = f"'{right_operand}'"
        
        # Use dialect-specific concatenation
        sql_fragment = context.dialect.string_concat(left_operand, right_operand)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )