"""
Mathematical function handlers for FHIRPath operations.

This module implements all mathematical functions including:
- Basic math: abs, ceiling, floor, round, sqrt, truncate
- Advanced math: exp, ln, log, power, power_operator
- Arithmetic operators: division
- Aggregate functions: max, min, median, mode, populationstddev, populationvariance
"""

from typing import List, Any
from .base import FunctionHandler
from ...core.base import SQLState, ExecutionContext, ContextMode


class InvalidArgumentError(Exception):
    """Raised when function arguments are invalid."""
    pass


class MathFunctionHandler(FunctionHandler):
    """Handler for mathematical FHIRPath functions."""
    
    def get_supported_functions(self) -> List[str]:
        """Return list of mathematical function names this handler supports."""
        return [
            'abs', 'ceiling', 'floor', 'round', 'sqrt', 'truncate',
            'exp', 'ln', 'log', 'power', 'power_operator', 'division',
            'max', 'min', 'median', 'mode', 'populationstddev', 'populationvariance'
        ]
    
    def handle_function(self, function_name: str, input_state: SQLState, 
                       context: ExecutionContext, args: List[Any]) -> SQLState:
        """
        Execute the specified mathematical function.
        
        Args:
            function_name: Name of the function to execute
            input_state: Current SQL state
            context: Execution context containing dialect and other settings
            args: Function arguments
            
        Returns:
            Updated SQL state after function execution
        """
        # Store args for method access (matching original implementation pattern)
        self.args = args
        
        # Route to appropriate handler method
        handler_map = {
            'abs': self._handle_abs,
            'ceiling': self._handle_ceiling,
            'floor': self._handle_floor,
            'round': self._handle_round,
            'sqrt': self._handle_sqrt,
            'truncate': self._handle_truncate,
            'exp': self._handle_exp,
            'ln': self._handle_ln,
            'log': self._handle_log,
            'power': self._handle_power,
            'power_operator': self._handle_power_operator,
            'division': self._handle_division,
            'max': self._handle_max,
            'min': self._handle_min,
            'median': self._handle_median,
            'mode': self._handle_mode,
            'populationstddev': self._handle_populationstddev,
            'populationvariance': self._handle_populationvariance
        }
        
        handler = handler_map.get(function_name.lower())
        if handler:
            return handler(input_state, context)
        else:
            raise ValueError(f"Unknown math function: {function_name}")
    
    # ===========================
    # Basic Mathematical Functions
    # ===========================
    
    def _handle_abs(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle abs() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        sql_fragment = f"ABS({cast_operand})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_ceiling(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle ceiling() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        sql_fragment = f"CEIL({cast_operand})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_floor(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle floor() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        sql_fragment = f"FLOOR({cast_operand})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_round(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle round() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        if self.args:
            # Extract the actual value from LiteralOperation if needed
            arg = self.args[0]
            precision_val = arg.value if hasattr(arg, 'value') else arg
            precision = f", {precision_val}"
        else:
            precision = ""
        sql_fragment = f"ROUND({cast_operand}{precision})"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_sqrt(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle sqrt() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        # Handle edge case: sqrt(negative) should return NULL
        safe_operand = f"CASE WHEN {cast_operand} < 0 THEN NULL ELSE {cast_operand} END"
        sql_fragment = context.dialect.generate_mathematical_function('sqrt', safe_operand)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_truncate(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle truncate() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        sql_fragment = context.dialect.generate_mathematical_function('truncate', cast_operand)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_exp(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle exp() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        sql_fragment = context.dialect.generate_mathematical_function('exp', cast_operand)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_ln(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle ln() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        # Handle edge cases: ln(0) and ln(negative) should return NULL
        safe_operand = f"CASE WHEN {cast_operand} <= 0 THEN NULL ELSE {cast_operand} END"
        sql_fragment = context.dialect.generate_mathematical_function('ln', safe_operand)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_log(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle log() function."""
        cast_operand = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        if self.args:
            # Extract the actual value from LiteralOperation if needed
            arg = self.args[0]
            base_val = arg.value if hasattr(arg, 'value') else arg
            base = f", {base_val}"
        else:
            base = ""
        sql_fragment = context.dialect.generate_mathematical_function('log', cast_operand + base)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_power(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle power() function."""
        if not self.args:
            raise InvalidArgumentError("power() function requires an exponent argument")
        
        # Extract the actual value from LiteralOperation if needed
        arg = self.args[0]
        if hasattr(arg, 'value'):
            exponent = str(arg.value)
        else:
            exponent = str(arg)
        base_expr = f"CAST({input_state.sql_fragment} AS DECIMAL)"
        sql_fragment = context.dialect.generate_power_operation(base_expr, exponent)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_power_operator(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle power (^) operator."""
        if not self.args:
            raise InvalidArgumentError("Power operator (^) requires an exponent argument")
        
        # The power operator ^ behaves exactly like the power() function
        # Reuse the existing _handle_power logic
        return self._handle_power(input_state, context)
    
    def _handle_division(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle division (/) operator."""
        if not self.args:
            raise InvalidArgumentError("Division operator requires a second operand")
        
        # Extract the actual value from LiteralOperation if needed
        arg = self.args[0]
        second_operand = str(arg.value if hasattr(arg, 'value') else arg)
        
        # Generate SQL with proper division by zero handling
        sql_fragment = f"(CASE WHEN CAST({second_operand} AS DECIMAL) = 0 THEN NULL ELSE CAST({input_state.sql_fragment} AS DECIMAL) / CAST({second_operand} AS DECIMAL) END)"
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    # ===========================
    # Aggregate Mathematical Functions
    # ===========================
    
    def _handle_max(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle max() function with variable arguments."""
        if not self.args:
            raise InvalidArgumentError("Max function requires at least one argument")
        
        # Build list of all arguments (input_state.sql_fragment is first operand)
        all_operands = [input_state.sql_fragment]
        # Extract actual values from LiteralOperation objects if needed
        for arg in self.args:
            operand_val = arg.value if hasattr(arg, 'value') else arg
            all_operands.append(str(operand_val))
        
        # Generate SQL using dialect-specific max operation
        sql_fragment = context.dialect.generate_max_operation(all_operands)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_min(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle min() function with variable arguments."""
        if not self.args:
            raise InvalidArgumentError("Min function requires at least one argument")
        
        # Build list of all arguments (input_state.sql_fragment is first operand)
        all_operands = [input_state.sql_fragment]
        # Extract actual values from LiteralOperation objects if needed
        for arg in self.args:
            operand_val = arg.value if hasattr(arg, 'value') else arg
            all_operands.append(str(operand_val))
        
        # Generate SQL using dialect-specific min operation
        sql_fragment = context.dialect.generate_min_operation(all_operands)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_median(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle median() aggregate function."""
        if not self.args:
            raise InvalidArgumentError("Median function requires at least one argument")
        
        # Get all operands (input + arguments)
        all_operands = [input_state.sql_fragment]
        # Extract actual values from LiteralOperation objects if needed
        for arg in self.args:
            operand_val = arg.value if hasattr(arg, 'value') else arg
            all_operands.append(str(operand_val))
        
        # Use database-specific median calculation
        sql_fragment = context.dialect.generate_median_operation(all_operands)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_mode(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle mode() aggregate function - most frequent value."""
        if not self.args:
            raise InvalidArgumentError("Mode function requires at least one argument")
        
        # Get all operands (input + arguments)
        all_operands = [input_state.sql_fragment]  
        # Extract actual values from LiteralOperation objects if needed
        for arg in self.args:
            operand_val = arg.value if hasattr(arg, 'value') else arg
            all_operands.append(str(operand_val))
        
        # Use window functions to find most frequent value
        # This is complex but works across both databases
        operands_str = ', '.join(all_operands)
        sql_fragment = f"""(
            SELECT value FROM (
                SELECT val as value, COUNT(*) as freq,
                       ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, val) as rn
                FROM (VALUES ({operands_str})) t(val)
                WHERE val IS NOT NULL
                GROUP BY val
            ) WHERE rn = 1
        )"""
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_populationstddev(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle PopulationStdDev() aggregate function."""
        if not self.args:
            raise InvalidArgumentError("PopulationStdDev function requires at least one argument")
        
        # Get all operands (input + arguments)
        all_operands = [input_state.sql_fragment]
        # Extract actual values from LiteralOperation objects if needed
        for arg in self.args:
            operand_val = arg.value if hasattr(arg, 'value') else arg
            all_operands.append(str(operand_val))
        
        # Use database-specific population standard deviation function
        sql_fragment = context.dialect.generate_population_stddev(all_operands)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )
    
    def _handle_populationvariance(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """Handle PopulationVariance() aggregate function."""
        if not self.args:
            raise InvalidArgumentError("PopulationVariance function requires at least one argument")
        
        # Get all operands (input + arguments)
        all_operands = [input_state.sql_fragment]
        # Extract actual values from LiteralOperation objects if needed
        for arg in self.args:
            operand_val = arg.value if hasattr(arg, 'value') else arg
            all_operands.append(str(operand_val))
        
        # Use database-specific population variance function
        sql_fragment = context.dialect.generate_population_variance(all_operands)
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=False,
            context_mode=ContextMode.SINGLE_VALUE
        )