"""
CQL define statement operations for named expression definitions.

These operations handle CQL define statements which create named expressions
that can be referenced and executed within CQL libraries.
"""

from typing import Optional, Dict, Any, Union
import logging
from ....pipeline.core.base import PipelineOperation, SQLState, ExecutionContext, ContextMode
from ....pipeline.core.builder import FHIRPathPipeline

logger = logging.getLogger(__name__)

class CQLDefineOperation(PipelineOperation[SQLState]):
    """
    CQL define statement operation for named expression definitions.
    
    This handles CQL define statements which create reusable named expressions:
    
    Example CQL:
        define "ActivePatients": [Patient] P where P.active = true
        define "BloodPressureReadings": [Observation: "Blood Pressure"] O where O.status = 'final'
    """
    
    def __init__(self, 
                 define_name: str,
                 expression_pipeline: PipelineOperation,
                 access_level: str = "PRIVATE",
                 definition_metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize CQL define operation.
        
        Args:
            define_name: Name of the defined expression
            expression_pipeline: Pipeline operation for the defined expression
            access_level: Access level ("PUBLIC" or "PRIVATE")  
            definition_metadata: Additional metadata for the definition
        """
        self.define_name = define_name
        self.expression_pipeline = expression_pipeline
        self.access_level = access_level
        self.definition_metadata = definition_metadata or {}
        
        logger.debug(f"Created CQLDefineOperation: {define_name} ({access_level})")
    
    def compile(self, context: ExecutionContext, state: SQLState) -> SQLState:
        """
        Compile define statement to SQL by creating a CTE (Common Table Expression).
        
        Args:
            context: Execution context
            state: Current SQL state
            
        Returns:
            Updated SQL state with define as CTE
        """
        logger.debug(f"Compiling define statement: {self.define_name}")
        
        # Execute the expression pipeline to get its SQL
        if hasattr(self.expression_pipeline, 'compile'):
            expression_state = self.expression_pipeline.compile(context, state)
        elif hasattr(self.expression_pipeline, 'execute'):
            expression_state = self.expression_pipeline.execute(state, context)
        else:
            # Fallback: use the input state
            logger.warning(f"Expression pipeline {type(self.expression_pipeline)} has neither compile nor execute method")
            expression_state = state
        
        # Create a CTE for the define statement
        clean_name = self.define_name.replace(' ', '_').replace('"', '')
        cte_name = f"define_{clean_name}"
        
        # Get the SQL from the expression state  
        if hasattr(expression_state, 'sql_fragment'):
            expression_sql = expression_state.sql_fragment
        elif hasattr(expression_state, 'main_sql'):
            expression_sql = expression_state.main_sql
        else:
            expression_sql = str(expression_state)
            
        # Generate CTE SQL
        cte_sql = f"{cte_name} AS (\n    {expression_sql}\n)"
        
        # Add CTE to the current state
        updated_ctes = state.ctes.copy() if state.ctes else []
        updated_ctes.append(cte_sql)
        
        # Create new state with the CTE reference
        return SQLState(
            base_table=state.base_table,
            json_column=state.json_column,
            sql_fragment=f"SELECT * FROM {cte_name}",  # This is what engine uses for execute path
            ctes=updated_ctes,
            lateral_joins=getattr(state, 'lateral_joins', []).copy() if hasattr(state, 'lateral_joins') else [],
            context_mode=getattr(state, 'context_mode', None),
            resource_type=getattr(state, 'resource_type', None),
            is_collection=getattr(expression_state, 'is_collection', False),
            path_context=getattr(state, 'path_context', '$'),
            variable_bindings=getattr(state, 'variable_bindings', {}).copy()
        )
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute define statement by compiling the expression.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            Updated SQL state with define execution result
        """
        logger.debug(f"Executing define statement: {self.define_name}")
        return self.compile(context, input_state)
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        clean_name = self.define_name.replace(' ', '_').replace('"', '').lower()
        return f"define_{clean_name}"
    
    def optimize_for_dialect(self, dialect) -> 'CQLDefineOperation':
        """
        Optimize define operation for specific dialect.
        
        Args:
            dialect: Target dialect for optimization
            
        Returns:
            Optimized define operation
        """
        # Optimize the underlying expression pipeline
        optimized_expression = self.expression_pipeline.optimize_for_dialect(dialect) if hasattr(self.expression_pipeline, 'optimize_for_dialect') else self.expression_pipeline
        
        return CQLDefineOperation(
            define_name=self.define_name,
            expression_pipeline=optimized_expression,
            access_level=self.access_level,
            definition_metadata=self.definition_metadata.copy()
        )
    
    def get_sql_representation(self, context: ExecutionContext, state: SQLState) -> str:
        """
        Get SQL representation of the define statement.
        
        Args:
            context: Execution context
            state: Current SQL state
            
        Returns:
            SQL representation
        """
        compiled_state = self.compile(context, state)
        return compiled_state.main_sql
    
    def get_define_info(self) -> Dict[str, Any]:
        """
        Get information about this define statement.
        
        Returns:
            Dictionary with define statement info
        """
        return {
            'name': self.define_name,
            'access_level': self.access_level,
            'metadata': self.definition_metadata.copy()
        }
    
    def can_be_referenced(self) -> bool:
        """
        Check if this define statement can be referenced from other expressions.
        
        Returns:
            True if accessible for referencing
        """
        return self.access_level == "PUBLIC"
    
    def __repr__(self) -> str:
        """String representation of the define operation."""
        return f"CQLDefineOperation(name='{self.define_name}', access_level='{self.access_level}')"