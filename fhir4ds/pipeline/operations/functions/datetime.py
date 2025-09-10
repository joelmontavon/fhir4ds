"""
DateTime function handler for FHIRPath operations.

This module implements datetime-related functions including:
- now(), today(), timeOfDay()
- DateTime constructor
- Age calculations (AgeInYears, AgeInYearsAt) 
- Date/time component extraction (hour, minute, second, year, month, day)
- Boundary functions (lowBoundary, highBoundary)
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


class DateTimeFunctionHandler(FunctionHandler):
    """Handler for datetime-related FHIRPath functions."""
    
    def __init__(self, args: List[Any] = None):
        """Initialize with function arguments."""
        self.args = args or []
    
    def get_supported_functions(self) -> List[str]:
        """Return list of datetime function names this handler supports."""
        return [
            'now', 'today', 'timeofday', 'datetime', 'datetime_constructor',
            'ageinyears', 'ageinyearsat', 'hour_from', 'minute_from', 
            'second_from', 'year_from', 'month_from', 'day_from',
            'lowboundary', 'highboundary'
        ]
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """Execute the specified datetime function."""
        # Set args for this function call
        self.args = args
        
        # Route to appropriate handler method
        handler_map = {
            'now': self._handle_now,
            'today': self._handle_today,
            'timeofday': self._handle_timeofday,
            'datetime': self._handle_datetime_constructor,
            'datetime_constructor': self._handle_datetime_constructor,
            'ageinyears': self._handle_ageinyears,
            'ageinyearsat': self._handle_ageinyearsat,
            'hour_from': self._handle_hour_from,
            'minute_from': self._handle_minute_from,
            'second_from': self._handle_second_from,
            'year_from': self._handle_year_from,
            'month_from': self._handle_month_from,
            'day_from': self._handle_day_from,
            'lowboundary': self._handle_lowboundary,
            'highboundary': self._handle_highboundary,
        }
        
        handler_func = handler_map.get(function_name.lower())
        if not handler_func:
            raise InvalidArgumentError(f"Unsupported datetime function: {function_name}")
        
        return handler_func(input_state, context)
    
    # =================
    # Handler Methods
    # =================
    
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