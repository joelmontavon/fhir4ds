"""
FHIRPath Public API Facade

This module provides a clean, simplified interface for FHIRPath operations, 
hiding implementation complexity while supporting both simple and advanced usage patterns.
"""

from typing import Any, Dict, List, Optional, Union
import json

from .parser.parser import FHIRPathParser
# Legacy translator removed - using pipeline system only
from .core.choice_types import fhir_choice_types
from .parser.ast_nodes import ASTNode

# Import new pipeline architecture components
try:
    from ..pipeline.converters.ast_converter import ASTToPipelineConverter, PipelineASTBridge
    from ..pipeline.core.base import ExecutionContext, SQLState
    from ..dialects.duckdb import DuckDBDialect
    PIPELINE_AVAILABLE = True
    _DEFAULT_DIALECT = None  # Lazy initialization
except ImportError:
    PIPELINE_AVAILABLE = False
    _DEFAULT_DIALECT = None


def _get_default_dialect():
    """Get default dialect with lazy initialization."""
    global _DEFAULT_DIALECT
    if _DEFAULT_DIALECT is None and PIPELINE_AVAILABLE:
        _DEFAULT_DIALECT = DuckDBDialect()
    return _DEFAULT_DIALECT


class FHIRPath:
    """
    Simplified FHIRPath interface for parsing, translating, and executing FHIRPath expressions.
    
    This class provides both simple and advanced usage patterns:
    - Simple: Direct expression evaluation
    - Advanced: Access to AST, SQL generation, and custom dialects
    """
    
    def __init__(self, dialect=None, use_pipeline: bool = False):
        """
        Initialize FHIRPath processor.
        
        Args:
            dialect: Database dialect for SQL generation (optional)
            use_pipeline: Whether to use new pipeline architecture (default: False)
        """
        self.dialect = dialect
        self.use_pipeline = use_pipeline and PIPELINE_AVAILABLE
        
        # Initialize pipeline components if available and requested
        if self.use_pipeline:
            self.pipeline_bridge = PipelineASTBridge()
            self.pipeline_bridge.set_migration_mode('gradual')  # Use gradual mode by default
        else:
            self.pipeline_bridge = None
    
    def parse(self, expression: str) -> ASTNode:
        """
        Parse a FHIRPath expression into an Abstract Syntax Tree.
        
        Args:
            expression: FHIRPath expression string
            
        Returns:
            ASTNode: Root node of the parsed AST
            
        Example:
            >>> fp = FHIRPath()
            >>> ast = fp.parse("Patient.name.family")
            >>> print(ast)
        """
        from .parser.parser import FHIRPathLexer, FHIRPathParser
        lexer = FHIRPathLexer(expression)
        tokens = lexer.tokenize()
        parser = FHIRPathParser(tokens)
        return parser.parse()
    
    def to_sql(self, expression: str, resource_type: str = "Patient", table_alias: str = "p") -> str:
        """
        Convert FHIRPath expression to SQL query fragment.
        
        Args:
            expression: FHIRPath expression string
            resource_type: FHIR resource type (default: "Patient")
            table_alias: SQL table alias (default: "p")
            
        Returns:
            str: SQL query fragment
            
        Example:
            >>> fp = FHIRPath()
            >>> sql = fp.to_sql("Patient.name.family", "Patient", "p")
            >>> print(sql)  # JSON_EXTRACT(p.data, '$.name[*].family')
        """
        # Always use pipeline system - legacy translator removed
        if not self.use_pipeline or not self.pipeline_bridge:
            # Enable pipeline if not already enabled
            if PIPELINE_AVAILABLE:
                self.use_pipeline = True
                self.pipeline_bridge = PipelineASTBridge()
                self.pipeline_bridge.set_migration_mode('gradual')
            else:
                raise RuntimeError("Pipeline system not available and legacy translator has been removed")
        
        try:
            # Parse expression to AST
            ast_node = self.parse(expression)
            
            # Create execution context with default dialect if none specified
            if not self.dialect:
                self.dialect = _get_default_dialect()
                if not self.dialect:
                    raise RuntimeError("No dialect available - pipeline system dependencies missing")
            
            context = ExecutionContext(dialect=self.dialect)
            
            # Use pipeline bridge to process
            sql = self.pipeline_bridge.process_fhirpath_expression(ast_node, context)
            
            # Adjust table alias if needed
            if table_alias != "fhir_resources":
                sql = sql.replace("fhir_resources.resource", f"{table_alias}.data")
            
            return sql
            
        except Exception as e:
            # Log pipeline failure but don't fall back to deprecated translator
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Pipeline SQL generation failed for '{expression}': {e}")
            raise RuntimeError(f"FHIRPath to SQL conversion failed: {e}") from e
    
    def set_pipeline_mode(self, mode: str) -> None:
        """
        Set the pipeline migration mode.
        
        Args:
            mode: Migration mode ('gradual', 'pipeline_only', 'ast_only')
        """
        if self.use_pipeline and self.pipeline_bridge:
            self.pipeline_bridge.set_migration_mode(mode)
        else:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning("Pipeline not available - cannot set pipeline mode")
    
    def get_pipeline_stats(self) -> dict:
        """
        Get pipeline usage statistics.
        
        Returns:
            Dictionary with pipeline statistics
        """
        if self.use_pipeline and self.pipeline_bridge:
            return self.pipeline_bridge.get_pipeline_coverage_stats()
        else:
            return {"pipeline_available": False, "use_pipeline": self.use_pipeline}
    
    def validate_expression(self, expression: str) -> tuple[bool, Optional[str]]:
        """
        Validate a FHIRPath expression for syntax errors.
        
        Args:
            expression: FHIRPath expression string
            
        Returns:
            tuple: (is_valid, error_message)
            
        Example:
            >>> fp = FHIRPath()
            >>> valid, error = fp.validate_expression("Patient.name.family")
            >>> print(f"Valid: {valid}, Error: {error}")
        """
        try:
            self.parse(expression)
            return True, None
        except Exception as e:
            return False, str(e)
    
    def resolve_choice_type(self, path: str) -> Optional[str]:
        """
        Resolve choice type for a given FHIRPath.
        
        Args:
            path: FHIRPath containing choice type (e.g., "value.ofType(Quantity)")
            
        Returns:
            str: Resolved path (e.g., "valueQuantity") or None if not a choice type
            
        Example:
            >>> fp = FHIRPath()
            >>> resolved = fp.resolve_choice_type("value.ofType(Quantity)")
            >>> print(resolved)  # "valueQuantity"
        """
        # Extract choice type from ofType() function
        if ".ofType(" in path:
            parts = path.split(".ofType(")
            if len(parts) == 2:
                field_name = parts[0].split('.')[-1]  # Get last part (e.g., "value")
                type_part = parts[1].rstrip(")")
                
                # Use the choice types method for direct mapping
                return fhir_choice_types.get_choice_field_mapping_direct(field_name, type_part)
        
        return None
    
    def get_supported_functions(self) -> List[str]:
        """
        Get list of supported FHIRPath functions.
        
        Returns:
            List[str]: List of function names
        """
        return [
            "exists", "empty", "first", "last", "count", "where", "ofType",
            "extension", "join", "getReferenceKey", "getResourceKey",
            "lowBoundary", "highBoundary", "forEach", "forEachOrNull"
        ]
    
    def get_choice_types(self) -> Dict[str, any]:
        """
        Get the complete choice types mapping.
        
        Returns:
            Dict[str, any]: Mapping of choice type expressions to resolved paths
        """
        # Return the internal choice mappings from the FHIRChoiceTypes instance
        return fhir_choice_types._choice_mappings.copy() if fhir_choice_types._choice_mappings else {}


class SimpleFHIRPath:
    """
    Ultra-simplified interface for basic FHIRPath operations.
    
    For users who just need basic path evaluation without configuration.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._fhirpath = FHIRPath()
        return cls._instance
    
    def parse(self, expression: str) -> str:
        """Parse expression and return string representation."""
        try:
            ast = self._fhirpath.parse(expression)
            return str(ast)
        except Exception as e:
            return f"Error: {e}"
    
    def to_sql(self, expression: str, resource: str = "Patient") -> str:
        """Convert expression to SQL."""
        try:
            return self._fhirpath.to_sql(expression, resource)
        except Exception as e:
            return f"Error: {e}"
    
    def is_valid(self, expression: str) -> bool:
        """Check if expression is valid."""
        valid, _ = self._fhirpath.validate_expression(expression)
        return valid


# Convenience functions for quick access
def parse_fhirpath(expression: str) -> ASTNode:
    """Quick parse function."""
    return FHIRPath().parse(expression)


def fhirpath_to_sql(expression: str, resource_type: str = "Patient", table_alias: str = "p") -> str:
    """Quick FHIRPath to SQL conversion."""
    return FHIRPath().to_sql(expression, resource_type, table_alias)


def validate_fhirpath(expression: str) -> bool:
    """Quick validation function."""
    valid, _ = FHIRPath().validate_expression(expression)
    return valid


# Export main classes and convenience functions
__all__ = [
    "FHIRPath",
    "SimpleFHIRPath", 
    "parse_fhirpath",
    "fhirpath_to_sql",
    "validate_fhirpath"
]