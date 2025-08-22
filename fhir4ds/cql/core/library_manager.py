"""
CQL Library Management System - Advanced library versioning and dependency resolution.

This module provides comprehensive library management for CQL, including:
- Library versioning and dependency resolution
- Parameter declaration and passing
- Public/private access modifiers
- Cross-library function calls
- Library caching and optimization

Phase 6: Library Management System Implementation
"""

import logging
from functools import lru_cache
from weakref import WeakValueDictionary
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass
from packaging import version
import json
import hashlib
from datetime import datetime, timezone


# Custom Exception Classes for Library Management
class LibraryError(Exception):
    """Exception for library management errors."""
    pass


class LibraryDependencyError(LibraryError):
    """Exception for library dependency resolution errors."""
    pass


class LibraryVersionError(LibraryError):
    """Exception for library versioning errors."""
    pass

logger = logging.getLogger(__name__)


@dataclass
class LibraryVersion:
    """Represents a library version with semantic versioning support."""
    major: int
    minor: int
    patch: int
    pre_release: Optional[str] = None
    build: Optional[str] = None
    
    def __str__(self) -> str:
        version_str = f"{self.major}.{self.minor}.{self.patch}"
        if self.pre_release:
            version_str += f"-{self.pre_release}"
        if self.build:
            version_str += f"+{self.build}"
        return version_str
    
    def __lt__(self, other) -> bool:
        if not isinstance(other, LibraryVersion):
            return NotImplemented
        return version.parse(str(self)) < version.parse(str(other))
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, LibraryVersion):
            return NotImplemented
        return str(self) == str(other)
    
    @classmethod
    def parse(cls, version_str: str) -> 'LibraryVersion':
        """Parse version string into LibraryVersion object."""
        parsed = version.parse(version_str)
        return cls(
            major=parsed.major,
            minor=parsed.minor,
            patch=parsed.micro,
            pre_release=parsed.pre if parsed.pre else None,
            build=parsed.local if parsed.local else None
        )
    
    def is_compatible(self, required_version: 'LibraryVersion') -> bool:
        """Check if this version is compatible with required version (semantic versioning rules)."""
        # Same major version, this version >= required version
        return (self.major == required_version.major and 
                version.parse(str(self)) >= version.parse(str(required_version)))


@dataclass
class LibraryDependency:
    """Represents a library dependency with version constraints."""
    library_name: str
    version_constraint: str  # e.g., ">=1.0.0", "~2.1", "^1.5.0"
    alias: Optional[str] = None
    required: bool = True
    
    def is_satisfied_by(self, lib_version: LibraryVersion) -> bool:
        """Check if a library version satisfies this dependency."""
        try:
            # Use packaging library for version constraint matching
            spec = version.SpecifierSet(self.version_constraint)
            return version.parse(str(lib_version)) in spec
        except Exception as e:
            logger.warning(f"Version constraint parsing failed for {self.version_constraint}: {e}")
            return False


@dataclass
class LibraryParameter:
    """Represents a library parameter with type and default value."""
    name: str
    parameter_type: str
    default_value: Optional[Any] = None
    required: bool = True
    description: Optional[str] = None
    
    def validate_value(self, value: Any) -> bool:
        """Validate that a value matches the parameter type."""
        # Basic type validation - can be enhanced with more sophisticated type checking
        type_mappings = {
            'String': str,
            'Integer': int,
            'Decimal': (int, float),
            'Boolean': bool,
            'DateTime': (str, datetime),  # Accept string or datetime
            'Code': dict,  # Expect code objects as dictionaries
            'Concept': dict  # Expect concept objects as dictionaries
        }
        
        expected_type = type_mappings.get(self.parameter_type)
        if expected_type:
            return isinstance(value, expected_type)
        
        # Unknown type - accept anything
        return True


@dataclass 
class LibraryMetadata:
    """Comprehensive library metadata."""
    name: str
    version: LibraryVersion
    description: Optional[str] = None
    author: Optional[str] = None
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
    dependencies: List[LibraryDependency] = None
    parameters: List[LibraryParameter] = None
    contexts: List[str] = None  # Supported contexts
    access_level: str = "PUBLIC"  # PUBLIC, PRIVATE, PROTECTED
    checksum: Optional[str] = None
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.parameters is None:
            self.parameters = []
        if self.contexts is None:
            self.contexts = ["Patient"]  # Default context
        if self.created is None:
            self.created = datetime.now(timezone.utc)


@dataclass
class LoadedLibrary:
    """Represents a loaded library with its AST and metadata."""
    metadata: LibraryMetadata
    ast: Any  # Library AST node
    translated_content: Dict[str, Any]
    source_content: str
    load_time: datetime
    dependencies_resolved: bool = False
    parameter_values: Dict[str, Any] = None
    define_operations: Dict[str, Any] = None  # Store CQLDefineOperation instances
    
    def __post_init__(self):
        if self.parameter_values is None:
            self.parameter_values = {}
        if self.define_operations is None:
            self.define_operations = {}


class DependencyResolver:
    """Resolves library dependencies using topological sorting."""
    
    def resolve_dependencies(self, libraries: Dict[str, LoadedLibrary]) -> List[str]:
        """
        Resolve library dependencies in correct loading order.
        
        Args:
            libraries: Dictionary of library name -> LoadedLibrary
            
        Returns:
            List of library names in dependency order
            
        Raises:
            ValueError: If circular dependencies are detected
        """
        # Build dependency graph
        graph = {}
        in_degree = {}
        
        for lib_name, loaded_lib in libraries.items():
            graph[lib_name] = []
            in_degree[lib_name] = 0
        
        # Add dependency edges
        for lib_name, loaded_lib in libraries.items():
            for dep in loaded_lib.metadata.dependencies:
                if dep.library_name in graph:
                    graph[dep.library_name].append(lib_name)
                    in_degree[lib_name] += 1
        
        # Topological sort using Kahn's algorithm
        queue = [lib for lib, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            current = queue.pop(0)
            result.append(current)
            
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        if len(result) != len(libraries):
            # Circular dependency detected
            remaining = [lib for lib in libraries.keys() if lib not in result]
            raise ValueError(f"Circular dependency detected among libraries: {remaining}")
        
        return result
    
    def validate_dependencies(self, libraries: Dict[str, LoadedLibrary]) -> List[str]:
        """
        Validate that all dependencies are satisfied.
        
        Args:
            libraries: Dictionary of library name -> LoadedLibrary
            
        Returns:
            List of validation errors (empty if all valid)
        """
        errors = []
        
        for lib_name, loaded_lib in libraries.items():
            for dep in loaded_lib.metadata.dependencies:
                if dep.library_name not in libraries:
                    if dep.required:
                        errors.append(f"Library '{lib_name}' requires missing dependency '{dep.library_name}'")
                    continue
                
                dep_lib = libraries[dep.library_name]
                if not dep.is_satisfied_by(dep_lib.metadata.version):
                    errors.append(
                        f"Library '{lib_name}' requires '{dep.library_name}' {dep.version_constraint}, "
                        f"but version {dep_lib.metadata.version} is loaded"
                    )
        
        return errors


class LibraryCache:
    """Caches loaded libraries using standard library LRU cache with WeakValueDictionary for memory management."""
    
    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        # Use WeakValueDictionary for automatic memory cleanup
        self._cache: WeakValueDictionary[str, LoadedLibrary] = WeakValueDictionary()
        # Track access statistics manually since functools.lru_cache doesn't work with methods
        self._hits = 0
        self._misses = 0
        self._access_order: List[str] = []
        
        # Create internal LRU cache function
        self._lru_cache = lru_cache(maxsize=max_size)(self._cache_lookup)
    
    def _cache_lookup(self, key: str) -> Optional[LoadedLibrary]:
        """Internal cache lookup function for LRU cache."""
        return self._cache.get(key)
    
    def get(self, key: str) -> Optional[LoadedLibrary]:
        """Get library from cache with LRU eviction and statistics tracking."""
        result = self._lru_cache(key)
        if result is not None:
            self._hits += 1
            # Update access order for statistics
            if key in self._access_order:
                self._access_order.remove(key)
            self._access_order.append(key)
        else:
            self._misses += 1
        return result
    
    def put(self, key: str, library: LoadedLibrary):
        """Add library to cache with automatic LRU eviction."""
        # Store in weak reference dictionary for memory management
        self._cache[key] = library
        
        # Clear the LRU cache to reset with new data
        self._lru_cache.cache_clear()
        
        # Recreate the LRU cache with updated max size
        self._lru_cache = lru_cache(maxsize=self.max_size)(self._cache_lookup)
        
        # Update access order
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)
        
        # Maintain manual size limit for WeakValueDictionary
        while len(self._access_order) > self.max_size:
            oldest_key = self._access_order.pop(0)
            if oldest_key in self._cache:
                del self._cache[oldest_key]
    
    def clear(self):
        """Clear all cached libraries and reset statistics."""
        self._cache.clear()
        self._access_order.clear()
        self._lru_cache.cache_clear()
        self._hits = 0
        self._misses = 0
    
    def stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics including LRU cache info."""
        lru_info = self._lru_cache.cache_info()
        return {
            'size': len(self._cache),
            'max_size': self.max_size,
            'libraries': list(self._cache.keys()),
            'access_order': self._access_order.copy(),
            'hits': self._hits,
            'misses': self._misses,
            'hit_rate': self._hits / (self._hits + self._misses) if (self._hits + self._misses) > 0 else 0.0,
            'lru_hits': lru_info.hits,
            'lru_misses': lru_info.misses,
            'lru_maxsize': lru_info.maxsize,
            'lru_currsize': lru_info.currsize
        }


class CQLLibraryManager:
    """
    Comprehensive CQL library management system.
    
    Provides advanced library operations including versioning, dependency resolution,
    parameter management, and cross-library function calls.
    """
    
    def __init__(self, cache_size: int = 100):
        """
        Initialize the library manager.
        
        Args:
            cache_size: Maximum number of libraries to cache
        """
        self.libraries: Dict[str, LoadedLibrary] = {}
        self.resolver = DependencyResolver()
        self.cache = LibraryCache(cache_size)
        self.library_paths: List[str] = []  # Search paths for libraries
        
    def add_library_path(self, path: str):
        """Add a search path for libraries."""
        if path not in self.library_paths:
            self.library_paths.append(path)
    
    def load_library(self, name: str, content: str, metadata: Optional[LibraryMetadata] = None) -> LoadedLibrary:
        """
        Load a library with comprehensive dependency and parameter management.
        
        Args:
            name: Library name
            content: Library CQL source content
            metadata: Optional library metadata
            
        Returns:
            Loaded library object
            
        Raises:
            ValueError: If library loading fails
        """
        logger.info(f"Loading library '{name}' with advanced management")
        
        try:
            # Generate cache key
            cache_key = self._generate_cache_key(name, content)
            
            # Check cache first
            cached_lib = self.cache.get(cache_key)
            if cached_lib:
                logger.debug(f"Library '{name}' loaded from cache")
                self.libraries[name] = cached_lib
                return cached_lib
            
            # Create metadata if not provided
            if metadata is None:
                metadata = self._extract_metadata_from_content(name, content)
            
            # Parse and translate library (this would integrate with existing parser/translator)
            ast, translated_content = self._parse_and_translate_library(name, content)
            
            # Extract define operations
            define_operations = translated_content.get('define_operations', {})
            
            # Create loaded library
            loaded_lib = LoadedLibrary(
                metadata=metadata,
                ast=ast,
                translated_content=translated_content,
                source_content=content,
                load_time=datetime.now(timezone.utc),
                define_operations=define_operations
            )
            
            # Store in libraries and cache
            self.libraries[name] = loaded_lib
            self.cache.put(cache_key, loaded_lib)
            
            logger.info(f"Successfully loaded library '{name}' version {metadata.version}")
            return loaded_lib
            
        except Exception as e:
            logger.error(f"Failed to load library '{name}': {e}")
            raise ValueError(f"Library loading failed: {e}")
    
    def resolve_all_dependencies(self) -> List[str]:
        """
        Resolve dependencies for all loaded libraries.
        
        Returns:
            List of library names in dependency order
            
        Raises:
            ValueError: If dependency resolution fails
        """
        logger.info("Resolving dependencies for all loaded libraries")
        
        # Validate dependencies
        validation_errors = self.resolver.validate_dependencies(self.libraries)
        if validation_errors:
            raise ValueError(f"Dependency validation failed: {'; '.join(validation_errors)}")
        
        # Resolve dependency order
        dependency_order = self.resolver.resolve_dependencies(self.libraries)
        
        # Mark all libraries as having dependencies resolved
        for lib_name in self.libraries:
            self.libraries[lib_name].dependencies_resolved = True
        
        logger.info(f"Dependencies resolved. Load order: {' -> '.join(dependency_order)}")
        return dependency_order
    
    def set_library_parameter(self, library_name: str, parameter_name: str, value: Any) -> bool:
        """
        Set parameter value for a library.
        
        Args:
            library_name: Name of the library
            parameter_name: Name of the parameter
            value: Parameter value
            
        Returns:
            True if parameter was set successfully
        """
        if library_name not in self.libraries:
            logger.error(f"Library '{library_name}' not found")
            return False
        
        library = self.libraries[library_name]
        
        # Find parameter definition
        param_def = None
        for param in library.metadata.parameters:
            if param.name == parameter_name:
                param_def = param
                break
        
        if not param_def:
            logger.error(f"Parameter '{parameter_name}' not found in library '{library_name}'")
            return False
        
        # Validate parameter value
        if not param_def.validate_value(value):
            logger.error(f"Invalid value for parameter '{parameter_name}': {value}")
            return False
        
        # Set parameter value
        library.parameter_values[parameter_name] = value
        logger.info(f"Set parameter '{parameter_name}' = {value} for library '{library_name}'")
        return True
    
    def get_library_definition(self, library_name: str, definition_name: str) -> Optional[Any]:
        """
        Get definition from a loaded library.
        
        Args:
            library_name: Name of the library
            definition_name: Name of the definition
            
        Returns:
            Definition object or None if not found
        """
        if library_name not in self.libraries:
            return None
        
        library = self.libraries[library_name]
        
        # First check for pipeline-based define operations
        if definition_name in library.define_operations:
            return library.define_operations[definition_name]
        
        # Fallback to legacy translated content
        definitions = library.translated_content.get('definitions', {})
        return definitions.get(definition_name)
    
    def list_library_definitions(self, library_name: str, access_level: str = "PUBLIC") -> List[str]:
        """
        List all definitions in a library with specified access level.
        
        Args:
            library_name: Name of the library
            access_level: Access level filter (PUBLIC, PRIVATE, PROTECTED)
            
        Returns:
            List of definition names
        """
        if library_name not in self.libraries:
            return []
        
        library = self.libraries[library_name]
        definitions = library.translated_content.get('definitions', {})
        
        # Filter by access level (this would need to be tracked in the translated content)
        # For now, return all definitions
        return list(definitions.keys())
    
    def resolve_define_dependencies(self, library_name: str) -> List[str]:
        """
        Resolve dependencies for define statements in a library.
        
        Args:
            library_name: Name of the library to analyze
            
        Returns:
            List of missing define dependencies
        """
        if library_name not in self.libraries:
            return [f"Library '{library_name}' not found"]
        
        library = self.libraries[library_name]
        missing_deps = []
        
        # Analyze each define operation for dependencies
        for define_name, define_op in library.define_operations.items():
            # Check if the define operation references other defines
            # This would require analyzing the pipeline operations
            # For now, we'll implement basic dependency tracking
            dependencies = self._extract_define_dependencies(define_op)
            
            for dep in dependencies:
                if not self._is_define_available(dep, library_name):
                    missing_deps.append(f"Define '{define_name}' requires '{dep}'")
        
        return missing_deps
    
    def get_cross_library_define(self, library_name: str, define_name: str, access_level: str = "PUBLIC") -> Optional[Any]:
        """
        Get a define statement from another library with access level checking.
        
        Args:
            library_name: Name of the library containing the define
            define_name: Name of the define statement
            access_level: Required access level
            
        Returns:
            Define operation or None if not accessible
        """
        if library_name not in self.libraries:
            logger.warning(f"Library '{library_name}' not found for cross-library access")
            return None
        
        library = self.libraries[library_name]
        
        if define_name not in library.define_operations:
            logger.warning(f"Define '{define_name}' not found in library '{library_name}'")
            return None
        
        define_op = library.define_operations[define_name]
        
        # Check access level
        if hasattr(define_op, 'access_level'):
            if define_op.access_level == "PRIVATE" and access_level != "PRIVATE":
                logger.warning(f"Define '{define_name}' in library '{library_name}' is private")
                return None
        
        return define_op
    
    def list_library_defines(self, library_name: str, access_level: str = "PUBLIC") -> List[str]:
        """
        List all define statements in a library with specified access level.
        
        Args:
            library_name: Name of the library
            access_level: Access level filter
            
        Returns:
            List of define statement names
        """
        if library_name not in self.libraries:
            return []
        
        library = self.libraries[library_name]
        defines = []
        
        for define_name, define_op in library.define_operations.items():
            # Check access level
            if hasattr(define_op, 'access_level'):
                if access_level == "ALL" or define_op.access_level == access_level:
                    defines.append(define_name)
            else:
                # Default to PUBLIC if no access level specified
                if access_level in ["PUBLIC", "ALL"]:
                    defines.append(define_name)
        
        return defines
    
    def get_library_stats(self) -> Dict[str, Any]:
        """Get comprehensive library management statistics."""
        total_libs = len(self.libraries)
        resolved_deps = sum(1 for lib in self.libraries.values() if lib.dependencies_resolved)
        
        version_distribution = {}
        total_defines = 0
        
        for lib in self.libraries.values():
            version_str = str(lib.metadata.version)
            version_distribution[version_str] = version_distribution.get(version_str, 0) + 1
            total_defines += len(lib.define_operations)
        
        return {
            'total_libraries': total_libs,
            'total_defines': total_defines,
            'dependencies_resolved': resolved_deps,
            'cache_stats': self.cache.stats(),
            'version_distribution': version_distribution,
            'library_paths': self.library_paths.copy(),
            'libraries': {
                name: {
                    'version': str(lib.metadata.version),
                    'dependencies': len(lib.metadata.dependencies),
                    'parameters': len(lib.metadata.parameters),
                    'defines': len(lib.define_operations),
                    'load_time': lib.load_time.isoformat()
                }
                for name, lib in self.libraries.items()
            }
        }
    
    def _generate_cache_key(self, name: str, content: str) -> str:
        """Generate cache key for library content."""
        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
        return f"{name}:{content_hash}"
    
    def _extract_metadata_from_content(self, name: str, content: str) -> LibraryMetadata:
        """Extract metadata from library content (simplified implementation)."""
        # This would parse the library header to extract version, dependencies, etc.
        # For now, create basic metadata
        return LibraryMetadata(
            name=name,
            version=LibraryVersion.parse("1.0.0"),
            description=f"CQL Library: {name}",
            created=datetime.now(timezone.utc)
        )
    
    def _parse_and_translate_library(self, name: str, content: str) -> Tuple[Any, Dict[str, Any]]:
        """Parse and translate library content using existing CQLParser and CQLTranslator."""
        from .parser import CQLParser, CQLLexer
        from .translator import CQLTranslator
        from ..pipeline.converters.cql_converter import CQLToPipelineConverter
        
        try:
            # Step 1: Tokenize the CQL content
            logger.debug(f"Tokenizing CQL library content for {name}")
            lexer = CQLLexer(content)
            tokens = lexer.tokenize()
            
            # Step 2: Parse the tokens into an AST
            logger.debug(f"Parsing CQL library {name}")
            parser = CQLParser(tokens)
            ast = parser.parse_library()
            
            # Step 3: Translate the AST to executable format
            logger.debug(f"Translating CQL library {name}")
            translator = CQLTranslator(dialect=getattr(self, 'dialect', 'duckdb'))
            translated = translator.translate_library(ast)
            
            # Step 4: Convert define statements to pipeline operations
            logger.debug(f"Converting define statements to pipeline operations for {name}")
            pipeline_converter = CQLToPipelineConverter(dialect=getattr(self, 'dialect', 'duckdb'))
            
            # Extract define operations from the library
            define_operations = {}
            if hasattr(ast, 'definitions'):
                for definition in ast.definitions:
                    from .parser import DefineNode
                    if isinstance(definition, DefineNode):
                        define_op = pipeline_converter.convert(definition)
                        define_operations[definition.name] = define_op
                        logger.debug(f"Converted define '{definition.name}' to pipeline operation")
            
            # Store define operations in translated content for compatibility
            translated['define_operations'] = define_operations
            
            logger.info(f"Successfully parsed and translated CQL library: {name} with {len(define_operations)} define operations")
            return ast, translated
            
        except ValueError as e:
            logger.error(f"Parse error in CQL library {name}: {e}")
            raise LibraryError(f"Failed to parse CQL library {name}: {e}")
        except AttributeError as e:
            logger.error(f"Translation error in CQL library {name}: {e}")
            raise LibraryError(f"Failed to translate CQL library {name}: {e}")
        except ImportError as e:
            logger.error(f"Missing dependencies for CQL parsing/translation: {e}")
            raise LibraryError(f"CQL parser/translator dependencies not available: {e}")
        except Exception as e:
            logger.error(f"Unexpected error processing CQL library {name}: {e}")
            raise LibraryError(f"Unexpected error in library processing for {name}: {e}")
    
    def _extract_define_dependencies(self, define_operation: Any) -> List[str]:
        """
        Extract define dependencies from a define operation.
        
        Args:
            define_operation: CQLDefineOperation to analyze
            
        Returns:
            List of define names that this operation depends on
        """
        dependencies = []
        
        # This is a simplified implementation
        # In practice, we would need to traverse the pipeline operation tree
        # to find references to other define statements
        
        if hasattr(define_operation, 'definition_metadata'):
            metadata = define_operation.definition_metadata
            if isinstance(metadata, dict):
                original_expr = metadata.get('original_expression', '')
                # Look for define references in the expression string
                # This is a basic pattern matching approach
                import re
                define_pattern = r'\b([A-Z][a-zA-Z0-9_]*)\b'
                potential_defines = re.findall(define_pattern, str(original_expr))
                dependencies.extend(potential_defines)
        
        return dependencies
    
    def _is_define_available(self, define_name: str, requesting_library: str) -> bool:
        """
        Check if a define statement is available to the requesting library.
        
        Args:
            define_name: Name of the define statement
            requesting_library: Name of the library making the request
            
        Returns:
            True if the define is available
        """
        # First check in the same library
        if requesting_library in self.libraries:
            library = self.libraries[requesting_library]
            if define_name in library.define_operations:
                return True
        
        # Check in included libraries
        for lib_name, library in self.libraries.items():
            if lib_name != requesting_library:
                if define_name in library.define_operations:
                    define_op = library.define_operations[define_name]
                    # Check if it's public or accessible
                    if hasattr(define_op, 'access_level'):
                        if define_op.access_level in ['PUBLIC', 'PROTECTED']:
                            return True
                    else:
                        # Default to public if no access level specified
                        return True
        
        return False