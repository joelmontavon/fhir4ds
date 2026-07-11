"""
CQL Bridge - Enhanced with full CQL engine integration.
"""

from typing import Dict, Any, Optional, Union
import logging

from ..cql.core.engine import CQLEngine
from ..view_runner import ViewRunner
from ..datastore.core import FHIRDataStore

logger = logging.getLogger(__name__)

class CQLBridge:
    """Enhanced bridge with full CQL functionality."""
    
    def __init__(self, datastore: FHIRDataStore):
        self.datastore = datastore
        self.cql_engine = CQLEngine(dialect=datastore.dialect)
        self.view_runner = None  # Create as needed
        
    def execute_cql(self, cql_expression: str, table_name: str = None) -> Any:
        """
        Execute CQL expression with full functionality.
        """
        logger.info(f"Executing CQL: {cql_expression}")
        
        try:
            # Use datastore's table name if not specified
            if table_name is None:
                table_name = getattr(self.datastore, 'table_name', 'fhir_resources')
            
            # Get SQL from CQL engine
            sql = self.cql_engine.evaluate_expression(cql_expression, table_name)
            
            logger.debug(f"Generated SQL: {sql}")
            
            # Execute using existing datastore
            result = self.datastore.execute_raw_sql(sql)
            return result
            
        except Exception as e:
            logger.error(f"CQL execution failed: {e}")
            raise
            
    def execute_cql_with_view_runner(self, cql_expression: str) -> Any:
        """
        Execute CQL using ViewRunner integration.
        """
        if not self.view_runner:
            self.view_runner = ViewRunner(self.datastore)
            
        # Convert CQL to view definition format
        view_def = self._cql_to_view_definition(cql_expression)
        
        # Execute using ViewRunner
        return self.view_runner.execute_view(view_def)
    
    def load_cql_library(self, library_name: str, library_content: str) -> Dict[str, Any]:
        """
        Load CQL library with full parsing.
        """
        return self.cql_engine.load_library(library_name, library_content)
    
    def set_cql_context(self, context: str):
        """Set CQL evaluation context."""
        self.cql_engine.set_context(context)
    
    def get_library_definition(self, library_name: str, definition_name: str) -> Optional[Any]:
        """Get definition from loaded library."""
        return self.cql_engine.get_library_definition(library_name, definition_name)
        
    def _cql_to_view_definition(self, cql_expression: str) -> Dict[str, Any]:
        """
        Convert CQL expression to ViewRunner view definition format.
        
        This allows CQL to work with existing ViewRunner infrastructure.
        """
        # Phase 1: Basic implementation
        # TODO: Enhance in later phases
        return {
            'resource': 'Patient',  # Default
            'select': [
                {
                    'column': [
                        {'path': cql_expression, 'name': 'result'}
                    ]
                }
            ]
        }