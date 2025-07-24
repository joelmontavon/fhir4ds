"""
CQL Engine - Main entry point for Clinical Quality Language processing.

This engine orchestrates CQL parsing, translation to FHIRPath, and execution
using the existing FHIR4DS infrastructure.
"""

from typing import Dict, Any, Optional
import logging

from ...fhirpath.core.generator import SQLGenerator
from ...dialects import DuckDBDialect, PostgreSQLDialect
from .parser import CQLParser, CQLLexer
from .translator import CQLTranslator
from .advanced_parser import AdvancedCQLParser
from .advanced_translator import AdvancedCQLTranslator
from .context import CQLContext, CQLContextManager, CQLEvaluationContext, CQLContextType
from ..functions.clinical import ClinicalFunctions, TerminologyFunctions
from ..functions.math_functions import CQLMathFunctionHandler
from ..functions.nullological_functions import CQLNullologicalFunctionHandler
from ..functions.datetime_functions import CQLDateTimeFunctionHandler
from ..functions.interval_functions import CQLIntervalFunctionHandler

logger = logging.getLogger(__name__)

class CQLEngine:
    """
    Clinical Quality Language (CQL) engine.
    
    Processes CQL expressions by translating them to FHIRPath and leveraging
    the existing SQL generation infrastructure.
    """
    
    def __init__(self, dialect=None, initial_context: str = "Population", 
                 terminology_client=None, db_connection=None):
        """
        Initialize CQL engine with context management and terminology services.
        
        Args:
            dialect: Database dialect for SQL generation (reuses FHIRPath dialects)
            initial_context: Initial evaluation context
            terminology_client: Custom terminology client (uses default if None)
            db_connection: Database connection for caching terminology operations
        """
        # Initialize proper dialect object
        self.dialect_name = dialect or "duckdb"
        self.dialect = self._get_dialect_instance(self.dialect_name)
        self.db_connection = db_connection
        self.parser = CQLParser([])  # Will be updated with tokens per expression
        self.translator = CQLTranslator(dialect or "duckdb")
        self.advanced_parser = AdvancedCQLParser()  # Phase 6: Advanced constructs
        self.advanced_translator = AdvancedCQLTranslator(dialect or "duckdb")  # Phase 6: Advanced SQL generation
        self.libraries = {}  # Will hold loaded libraries
        
        # Enhanced context management
        self.context_manager = CQLContextManager()
        self.context_manager.current_context = CQLContext(initial_context, dialect=self.dialect_name)
        self.evaluation_context = CQLEvaluationContext(self.context_manager.current_context)
        
        # Terminology and clinical functions with caching using actual database
        self.terminology = TerminologyFunctions(
            terminology_client=terminology_client,
            db_connection=db_connection,
            dialect=self.dialect_name
        )
        self.clinical_functions = ClinicalFunctions(
            terminology_client=terminology_client,
            db_connection=db_connection,
            dialect=self.dialect_name
        )
        
        # Mathematical, nullological, date/time, and interval function handlers  
        self.math_functions = CQLMathFunctionHandler(self.dialect_name)
        self.nullological_functions = CQLNullologicalFunctionHandler(self.dialect_name)
        self.datetime_functions = CQLDateTimeFunctionHandler(self.dialect_name)
        self.interval_functions = CQLIntervalFunctionHandler(self.dialect_name)
        
        # Make all function handlers available to translator
        self.translator.terminology = self.terminology
        self.translator.clinical_functions = self.clinical_functions
        self.translator.math_functions = self.math_functions
        self.translator.nullological_functions = self.nullological_functions
        self.translator.datetime_functions = self.datetime_functions
        self.translator.interval_functions = self.interval_functions
        
    def _get_dialect_instance(self, dialect_name: str):
        """Create proper dialect instance from dialect name string."""
        if dialect_name == "duckdb":
            return DuckDBDialect()
        elif dialect_name == "postgresql":
            return PostgreSQLDialect()
        else:
            # Default to DuckDB if unknown dialect
            return DuckDBDialect()
        
    def evaluate_expression(self, cql_expression: str, table_name: str = "fhir_resources", 
                          json_column: str = "resource") -> str:
        """
        Evaluate CQL expression to SQL.
        
        Args:
            cql_expression: CQL expression string
            table_name: Database table name
            json_column: JSON column name
            
        Returns:
            SQL query string
        """
        logger.info(f"CQL Engine evaluating: {cql_expression}")
        
        try:
            # Check if this is an advanced CQL construct (Phase 6)
            if self._has_advanced_constructs(cql_expression):
                return self.evaluate_advanced_expression(cql_expression, table_name, json_column)
            
            # Step 1: Parse CQL expression to AST
            lexer = CQLLexer(cql_expression)
            tokens = lexer.tokenize()
            self.parser.tokens = tokens
            self.parser.current = 0
            
            # Use the smart parsing method that detects CQL vs FHIRPath
            cql_ast = self.parser.parse_expression_or_fhirpath(cql_expression)
            
            # Step 2: Translate CQL AST to FHIRPath AST
            fhirpath_ast = self.translator.translate_expression(cql_ast)
            
            # Step 3: Generate SQL using existing FHIRPath infrastructure
            if self.dialect:
                generator = SQLGenerator(table_name, json_column, dialect=self.dialect)
                sql = generator.visit(fhirpath_ast)
                
                # Step 4: Apply context filtering to generated SQL
                context_filtered_sql = self.context_manager.current_context.apply_context_to_query(sql, table_name)
                return context_filtered_sql
            else:
                return f"-- CQL Expression (no dialect): {cql_expression}"
                
        except Exception as e:
            logger.error(f"CQL evaluation failed: {e}")
            # Fallback: treat as comment
            return f"-- CQL Expression (error: {e}): {cql_expression}"
    
    def _has_advanced_constructs(self, cql_expression: str) -> bool:
        """
        Check if CQL expression contains advanced constructs (Phase 6).
        
        Args:
            cql_expression: CQL expression to check
            
        Returns:
            True if expression contains with/without clauses or let expressions
        """
        import re
        expression_lower = cql_expression.lower().strip()
        
        # Check for with/without clauses
        with_pattern = r'\bwith\s+\[[^\]]+\]'
        without_pattern = r'\bwithout\s+\[[^\]]+\]'
        let_pattern = r'\blet\s+\w+\s*:'
        
        return bool(re.search(with_pattern, expression_lower) or 
                   re.search(without_pattern, expression_lower) or
                   re.search(let_pattern, expression_lower))
    
    def evaluate_advanced_expression(self, cql_expression: str, table_name: str = "fhir_resources", 
                                   json_column: str = "resource") -> str:
        """
        Evaluate advanced CQL expression using Phase 6 advanced translator.
        
        Args:
            cql_expression: Advanced CQL expression string
            table_name: Database table name
            json_column: JSON column name
            
        Returns:
            SQL query string for advanced constructs
        """
        logger.info(f"CQL Engine evaluating advanced expression: {cql_expression}")
        
        try:
            # Use advanced translator for Phase 6 constructs
            sql = self.advanced_translator.translate_advanced_cql(cql_expression)
            
            # Apply context filtering if needed
            context_filtered_sql = self.context_manager.current_context.apply_context_to_query(sql, table_name)
            return context_filtered_sql
            
        except Exception as e:
            logger.error(f"Advanced CQL evaluation failed: {e}")
            return f"-- Advanced CQL Expression (error: {e}): {cql_expression}"
    
    def load_library(self, library_name: str, library_content: str) -> Dict[str, Any]:
        """
        Load and parse CQL library.
        
        Args:
            library_name: Name of the library
            library_content: CQL library content
            
        Returns:
            Parsed library information
        """
        logger.info(f"Loading CQL library: {library_name}")
        
        try:
            # Parse library
            lexer = CQLLexer(library_content)
            tokens = lexer.tokenize()
            self.parser.tokens = tokens
            self.parser.current = 0
            
            library_ast = self.parser.parse_library()
            
            # Translate library definitions
            translated_library = self.translator.translate_library(library_ast)
            
            # Store library
            self.libraries[library_name] = translated_library
            
            # Add library to evaluation context
            self.evaluation_context.add_library(library_name, translated_library)
            
            return translated_library
            
        except Exception as e:
            logger.error(f"Library loading failed: {e}")
            # Store as raw content for now
            self.libraries[library_name] = library_content
            return {'name': library_name, 'content': library_content, 'error': str(e)}
    
    def set_context(self, context: str):
        """Set evaluation context with enhanced context management."""
        # Handle None context
        if context is None:
            context = "Population"  # Default fallback
            
        # Ensure context is a string before calling upper()
        if not isinstance(context, str):
            context = str(context)
            
        # Update context manager
        try:
            if hasattr(CQLContextType, context.upper()):
                new_context = CQLContext(getattr(CQLContextType, context.upper()))
                self.context_manager.current_context = new_context
                self.evaluation_context = CQLEvaluationContext(new_context)
            else:
                # Fallback for custom contexts
                new_context = CQLContext(context)
                self.context_manager.current_context = new_context
        except AttributeError:
            # If context doesn't have upper() method, treat as string
            new_context = CQLContext(context)
            self.context_manager.current_context = new_context
            
        # Update translator
        self.translator.set_context(context)
        logger.debug(f"CQL context set to: {context}")
    
    def set_patient_context(self, patient_id: str):
        """Set patient-specific context."""
        self.context_manager.current_context.set_patient_context(patient_id)
        logger.debug(f"Set patient context: {patient_id}")
    
    def set_population_context(self, filters: Optional[Dict[str, Any]] = None):
        """Set population-level context with optional filters."""
        self.context_manager.current_context.set_population_context(filters)
        logger.debug(f"Set population context with filters: {filters}")
    
    def reset_to_population_analytics(self):
        """Reset to population-first analytics mode, clearing any single-patient override."""
        self.context_manager.current_context.reset_to_population_analytics()
        logger.debug("CQL engine reset to population analytics mode")
    
    def get_current_context(self) -> CQLContext:
        """Get current evaluation context."""
        return self.context_manager.current_context
    
    def is_single_patient_mode(self) -> bool:
        """Check if engine is in single-patient override mode."""
        return self.context_manager.current_context.is_single_patient_mode()
    
    def is_population_analytics_mode(self) -> bool:
        """Check if engine is in population-first analytics mode."""
        return self.context_manager.current_context.is_population_analytics_mode()
    
    def get_context_mode_description(self) -> str:
        """Get human-readable description of current context mode."""
        return self.context_manager.current_context.get_context_mode()
    
    def with_temporary_context(self, context: str):
        """Create temporary context scope."""
        temp_context = CQLContext(context)
        return self.context_manager.with_context(temp_context)
    
    def set_parameter(self, name: str, value: Any):
        """Set a CQL parameter value."""
        self.evaluation_context.set_parameter(name, value)
        logger.debug(f"Set parameter {name} = {value}")
    
    def get_parameter(self, name: str) -> Any:
        """Get a CQL parameter value."""
        return self.evaluation_context.get_parameter(name)
    
    def get_library_definition(self, library_name: str, definition_name: str) -> Optional[Any]:
        """Get definition from loaded library."""
        if library_name in self.libraries:
            library = self.libraries[library_name]
            if isinstance(library, dict) and 'definitions' in library:
                definitions = library['definitions']
                return definitions.get(definition_name)
        return None
    
    def get_terminology_cache_stats(self) -> Dict[str, Any]:
        """
        Get terminology cache statistics for monitoring and debugging.
        
        Returns:
            Dictionary with cache performance metrics
        """
        return self.terminology.get_cache_stats()
    
    def clear_terminology_cache(self):
        """Clear all terminology cache data."""
        if hasattr(self.terminology.client, 'clear_cache'):
            self.terminology.client.clear_cache()
            logger.info("Cleared terminology cache")
        else:
            logger.warning("No cache to clear")
    
    def clear_expired_terminology_cache(self):
        """Clear only expired terminology cache entries."""
        if hasattr(self.terminology.client, 'clear_expired_cache'):
            self.terminology.client.clear_expired_cache()
            logger.info("Cleared expired terminology cache entries")
        else:
            logger.warning("No cache to clear")
    
    def get_engine_info(self) -> Dict[str, Any]:
        """
        Get comprehensive engine information including cache stats.
        
        Returns:
            Dictionary with engine status and configuration
        """
        info = {
            'dialect': self.dialect,
            'context': {
                'current': str(self.context_manager.current_context.context_type),
                'mode': self.get_context_mode_description(),
                'is_single_patient': self.is_single_patient_mode(),
                'is_population_analytics': self.is_population_analytics_mode()
            },
            'libraries': list(self.libraries.keys()),
            'terminology': self.get_terminology_cache_stats()
        }
        
        return info