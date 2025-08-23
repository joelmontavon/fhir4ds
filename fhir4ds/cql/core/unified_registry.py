"""
Unified Function Registry for CQL and FHIRPath Integration

This module provides a unified function registry that manages all function handlers
across CQL and FHIRPath systems, enabling seamless function routing and access
to advanced CQL capabilities like statistical functions.

Addresses the function routing gap identified in comprehensive repository analysis:
- CQL functions registered in translator but not accessible via FHIRPath generator
- Statistical functions correctly implemented but blocked by routing architecture
- Need for unified access to 82+ implemented CQL functions

Architecture: Preserves existing handler patterns while providing unified access
"""

import logging
from typing import Dict, Any, Optional, List, Union, Type
from enum import Enum

# Import all function handlers
from ..functions.math_functions import CQLMathFunctionHandler
from ..functions.datetime_functions import CQLDateTimeFunctionHandler
from ..functions.interval_functions import CQLIntervalFunctionHandler
from ..functions.nullological_functions import CQLNullologicalFunctionHandler
from ..functions.clinical import ClinicalFunctions, TerminologyFunctions
from ..functions.logical_operators import CQLLogicalOperators

logger = logging.getLogger(__name__)


class FunctionCategory(Enum):
    """Function categories for smart routing and capability detection."""
    MATHEMATICAL = "mathematical"
    STATISTICAL = "statistical"
    DATETIME = "datetime"
    INTERVAL = "interval"
    NULLOLOGICAL = "nullological"
    LOGICAL = "logical"
    CLINICAL = "clinical"
    TERMINOLOGY = "terminology"
    UNKNOWN = "unknown"


class HandlerCapability:
    """Represents the capabilities of a function handler."""
    
    def __init__(self, handler_type: str, supported_functions: List[str], 
                 categories: List[FunctionCategory], dialect_specific: bool = True):
        self.handler_type = handler_type
        self.supported_functions = set(supported_functions)
        self.categories = categories
        self.dialect_specific = dialect_specific
    
    def can_handle(self, function_name: str) -> bool:
        """Check if this handler can handle the specified function."""
        return function_name.lower() in {f.lower() for f in self.supported_functions}


class UnifiedFunctionRegistry:
    """
    Unified function registry managing all CQL and FHIRPath function handlers.
    
    Provides centralized function routing with smart capability detection,
    fallback mechanisms, and comprehensive handler management.
    
    Key Features:
    - Smart function routing based on capabilities
    - Fallback mechanisms for unrecognized functions
    - Dialect-specific routing (DuckDB/PostgreSQL)
    - Comprehensive logging for debugging
    - Backward compatibility with existing patterns
    """
    
    def __init__(self, dialect: Union[str, Any] = "duckdb", terminology_client=None, db_connection=None):
        """
        Initialize unified function registry with all function handlers.
        
        Args:
            dialect: Database dialect ("duckdb" or "postgresql") or dialect object
            terminology_client: Optional terminology client for clinical functions
            db_connection: Optional database connection for caching
        """
        # Convert dialect object to string if needed
        self.dialect = self._normalize_dialect(dialect)
        self.handlers = {}
        self.capabilities = {}
        self.function_map = {}  # Direct function name -> handler mapping
        
        logger.info(f"Initializing UnifiedFunctionRegistry with dialect: {self.dialect}")
        
        # Initialize all function handlers
        self._initialize_handlers(terminology_client, db_connection)
        
        # Build capability registry
        self._build_capability_registry()
        
        # Build direct function mapping for fast lookup
        self._build_function_map()
        
        logger.info(f"UnifiedFunctionRegistry initialized with {len(self.function_map)} functions")
    
    def _normalize_dialect(self, dialect: Union[str, Any]) -> str:
        """
        Convert dialect object to string if needed.
        
        Args:
            dialect: Database dialect string or dialect object
            
        Returns:
            Normalized dialect string ("duckdb" or "postgresql")
        """
        if isinstance(dialect, str):
            return dialect.lower()
        
        # Handle dialect objects by extracting class name
        dialect_class_name = dialect.__class__.__name__.lower()
        if "duckdb" in dialect_class_name:
            logger.debug(f"Converted DuckDB dialect object to string")
            return "duckdb"
        elif "postgresql" in dialect_class_name or "postgres" in dialect_class_name:
            logger.debug(f"Converted PostgreSQL dialect object to string")
            return "postgresql"
        else:
            # Fallback to duckdb for unknown dialect objects
            logger.warning(f"Unknown dialect object type: {dialect_class_name}, defaulting to duckdb")
            return "duckdb"
    
    def _initialize_handlers(self, terminology_client=None, db_connection=None):
        """Initialize all function handlers with proper configuration."""
        try:
            # Mathematical functions (including statistical)
            self.handlers['math'] = CQLMathFunctionHandler(self.dialect)
            logger.debug("Initialized CQL mathematical function handler")
            
            # DateTime functions
            self.handlers['datetime'] = CQLDateTimeFunctionHandler(self.dialect)
            logger.debug("Initialized CQL datetime function handler")
            
            # Interval functions
            self.handlers['interval'] = CQLIntervalFunctionHandler(self.dialect)
            logger.debug("Initialized CQL interval function handler")
            
            # Nullological functions
            self.handlers['nullological'] = CQLNullologicalFunctionHandler(self.dialect)
            logger.debug("Initialized CQL nullological function handler")
            
            # Clinical functions
            self.handlers['clinical'] = ClinicalFunctions(
                terminology_client=terminology_client,
                db_connection=db_connection,
                dialect=self.dialect
            )
            logger.debug("Initialized CQL clinical function handler")
            
            # Terminology functions
            self.handlers['terminology'] = TerminologyFunctions(
                terminology_client=terminology_client,
                db_connection=db_connection,
                dialect=self.dialect
            )
            logger.debug("Initialized CQL terminology function handler")
            
            # Logical operators
            self.handlers['logical'] = CQLLogicalOperators()
            logger.debug("Initialized CQL logical operators handler")
            
        except Exception as e:
            logger.error(f"Failed to initialize function handlers: {e}")
            raise
    
    def _build_capability_registry(self):
        """Build capability registry for smart function routing."""
        try:
            # Mathematical functions (including statistical)
            if 'math' in self.handlers:
                math_functions = self.handlers['math'].get_supported_functions()
                statistical_functions = ['stddev', 'stdev', 'variance', 'median', 'mode', 'percentile']
                
                self.capabilities['math'] = HandlerCapability(
                    handler_type='math',
                    supported_functions=math_functions,
                    categories=[FunctionCategory.MATHEMATICAL, FunctionCategory.STATISTICAL]
                )
                logger.debug(f"Registered {len(math_functions)} mathematical functions")
            
            # DateTime functions
            if 'datetime' in self.handlers:
                datetime_functions = self.handlers['datetime'].get_supported_functions()
                self.capabilities['datetime'] = HandlerCapability(
                    handler_type='datetime',
                    supported_functions=datetime_functions,
                    categories=[FunctionCategory.DATETIME]
                )
                logger.debug(f"Registered {len(datetime_functions)} datetime functions")
            
            # Interval functions
            if 'interval' in self.handlers:
                interval_functions = self.handlers['interval'].get_supported_functions()
                self.capabilities['interval'] = HandlerCapability(
                    handler_type='interval',
                    supported_functions=interval_functions,
                    categories=[FunctionCategory.INTERVAL]
                )
                logger.debug(f"Registered {len(interval_functions)} interval functions")
            
            # Nullological functions
            if 'nullological' in self.handlers:
                nullological_functions = self.handlers['nullological'].get_supported_functions()
                self.capabilities['nullological'] = HandlerCapability(
                    handler_type='nullological',
                    supported_functions=nullological_functions,
                    categories=[FunctionCategory.NULLOLOGICAL]
                )
                logger.debug(f"Registered {len(nullological_functions)} nullological functions")
            
            # Logical operators
            if 'logical' in self.handlers:
                logical_functions = self.handlers['logical'].get_supported_functions()
                self.capabilities['logical'] = HandlerCapability(
                    handler_type='logical',
                    supported_functions=logical_functions,
                    categories=[FunctionCategory.LOGICAL]
                )
                logger.debug(f"Registered {len(logical_functions)} logical functions")
            
        except Exception as e:
            logger.error(f"Failed to build capability registry: {e}")
            raise
    
    def _build_function_map(self):
        """Build direct function name to handler mapping for fast lookup."""
        try:
            for handler_name, capability in self.capabilities.items():
                handler = self.handlers[handler_name]
                for function_name in capability.supported_functions:
                    # Store in lowercase for case-insensitive lookup
                    key = function_name.lower()
                    if key in self.function_map:
                        logger.warning(f"Function '{function_name}' registered by multiple handlers")
                    self.function_map[key] = {
                        'handler': handler,
                        'handler_name': handler_name,
                        'original_name': function_name
                    }
            
            logger.debug(f"Built function map with {len(self.function_map)} function mappings")
            
        except Exception as e:
            logger.error(f"Failed to build function map: {e}")
            raise
    
    def can_handle_function(self, function_name: str) -> bool:
        """
        Check if the registry can handle the specified function.
        
        Args:
            function_name: Name of the function to check
            
        Returns:
            True if function can be handled, False otherwise
        """
        return function_name.lower() in self.function_map
    
    def get_handler_for_function(self, function_name: str) -> Optional[Any]:
        """
        Get the appropriate handler for the specified function.
        
        Args:
            function_name: Name of the function
            
        Returns:
            Function handler instance or None if not found
        """
        key = function_name.lower()
        if key in self.function_map:
            mapping = self.function_map[key]
            logger.debug(f"Routing function '{function_name}' to {mapping['handler_name']} handler")
            return mapping['handler']
        
        logger.debug(f"No handler found for function '{function_name}'")
        return None
    
    def get_handler_info(self, function_name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about the handler for a function.
        
        Args:
            function_name: Name of the function
            
        Returns:
            Dictionary with handler information or None if not found
        """
        key = function_name.lower()
        if key in self.function_map:
            mapping = self.function_map[key]
            capability = self.capabilities[mapping['handler_name']]
            
            return {
                'function_name': function_name,
                'original_name': mapping['original_name'],
                'handler': mapping['handler'],
                'handler_name': mapping['handler_name'],
                'handler_type': capability.handler_type,
                'categories': [cat.value for cat in capability.categories],
                'dialect_specific': capability.dialect_specific
            }
        
        return None
    
    def get_functions_by_category(self, category: FunctionCategory) -> List[str]:
        """
        Get all functions that belong to a specific category.
        
        Args:
            category: Function category to filter by
            
        Returns:
            List of function names in the category
        """
        functions = []
        for handler_name, capability in self.capabilities.items():
            if category in capability.categories:
                functions.extend(capability.supported_functions)
        
        return sorted(functions)
    
    def get_statistical_functions(self) -> List[str]:
        """Get all statistical functions available in the registry."""
        return self.get_functions_by_category(FunctionCategory.STATISTICAL)
    
    def call_function(self, function_name: str, *args, **kwargs) -> Any:
        """
        Call a function through the appropriate handler.
        
        Args:
            function_name: Name of the function to call
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Result of the function call
            
        Raises:
            ValueError: If function is not supported
            Exception: If function call fails
        """
        handler = self.get_handler_for_function(function_name)
        if not handler:
            raise ValueError(f"Function '{function_name}' is not supported")
        
        try:
            # Get the function method from the handler
            if hasattr(handler, 'function_map') and function_name.lower() in handler.function_map:
                func_method = handler.function_map[function_name.lower()]
                logger.debug(f"Calling {function_name} via handler function map")
                return func_method(*args, **kwargs)
            elif hasattr(handler, function_name.lower()):
                func_method = getattr(handler, function_name.lower())
                logger.debug(f"Calling {function_name} via handler method")
                return func_method(*args, **kwargs)
            else:
                raise AttributeError(f"Handler does not implement function '{function_name}'")
                
        except Exception as e:
            logger.error(f"Failed to call function '{function_name}': {e}")
            raise
    
    def get_registry_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics about the registry."""
        stats = {
            'dialect': self.dialect,
            'total_handlers': len(self.handlers),
            'total_functions': len(self.function_map),
            'handler_breakdown': {},
            'category_breakdown': {}
        }
        
        # Handler breakdown
        for handler_name, capability in self.capabilities.items():
            stats['handler_breakdown'][handler_name] = len(capability.supported_functions)
        
        # Category breakdown
        for category in FunctionCategory:
            functions = self.get_functions_by_category(category)
            if functions:
                stats['category_breakdown'][category.value] = len(functions)
        
        return stats
    
    def debug_function_lookup(self, function_name: str) -> Dict[str, Any]:
        """
        Debug function lookup process for troubleshooting.
        
        Args:
            function_name: Function name to debug
            
        Returns:
            Detailed lookup information
        """
        key = function_name.lower()
        debug_info = {
            'function_name': function_name,
            'normalized_key': key,
            'found_in_registry': key in self.function_map,
            'registry_size': len(self.function_map),
            'available_handlers': list(self.handlers.keys()),
            'capabilities_count': len(self.capabilities)
        }
        
        if key in self.function_map:
            mapping = self.function_map[key]
            debug_info.update({
                'handler_name': mapping['handler_name'],
                'original_name': mapping['original_name'],
                'handler_type': type(mapping['handler']).__name__
            })
        else:
            # Find similar function names
            similar = [name for name in self.function_map.keys() if key in name or name in key]
            debug_info['similar_functions'] = similar[:5]  # Limit to 5 for readability
        
        return debug_info


# Global registry instance (initialized when needed)
_global_registry = None


def get_global_registry(dialect: str = "duckdb", **kwargs) -> UnifiedFunctionRegistry:
    """
    Get or create the global unified function registry instance.
    
    Args:
        dialect: Database dialect
        **kwargs: Additional arguments for registry initialization
        
    Returns:
        Global registry instance
    """
    global _global_registry
    
    if _global_registry is None or _global_registry.dialect != dialect:
        logger.info(f"Creating new global registry for dialect: {dialect}")
        _global_registry = UnifiedFunctionRegistry(dialect=dialect, **kwargs)
    
    return _global_registry


def reset_global_registry():
    """Reset the global registry (useful for testing)."""
    global _global_registry
    _global_registry = None
    logger.info("Global registry reset")

