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
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass
from packaging import version
import json
import hashlib
from datetime import datetime, timezone

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
    
    def __post_init__(self):
        if self.parameter_values is None:
            self.parameter_values = {}


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
    """Caches loaded libraries for performance optimization."""
    
    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.cache: Dict[str, LoadedLibrary] = {}
        self.access_order: List[str] = []
    
    def get(self, key: str) -> Optional[LoadedLibrary]:
        """Get library from cache, updating access order."""
        if key in self.cache:
            # Update access order (LRU)
            self.access_order.remove(key)
            self.access_order.append(key)
            return self.cache[key]
        return None
    
    def put(self, key: str, library: LoadedLibrary):
        """Add library to cache with LRU eviction."""
        if key in self.cache:
            # Update existing entry
            self.access_order.remove(key)
        elif len(self.cache) >= self.max_size:
            # Evict least recently used
            lru_key = self.access_order.pop(0)
            del self.cache[lru_key]
        
        self.cache[key] = library
        self.access_order.append(key)
    
    def clear(self):
        """Clear all cached libraries."""
        self.cache.clear()
        self.access_order.clear()
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'libraries': list(self.cache.keys()),
            'access_order': self.access_order.copy()
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
            
            # Create loaded library
            loaded_lib = LoadedLibrary(
                metadata=metadata,
                ast=ast,
                translated_content=translated_content,
                source_content=content,
                load_time=datetime.now(timezone.utc)
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
    
    def get_library_stats(self) -> Dict[str, Any]:
        """Get comprehensive library management statistics."""
        total_libs = len(self.libraries)
        resolved_deps = sum(1 for lib in self.libraries.values() if lib.dependencies_resolved)
        
        version_distribution = {}
        for lib in self.libraries.values():
            version_str = str(lib.metadata.version)
            version_distribution[version_str] = version_distribution.get(version_str, 0) + 1
        
        return {
            'total_libraries': total_libs,
            'dependencies_resolved': resolved_deps,
            'cache_stats': self.cache.stats(),
            'version_distribution': version_distribution,
            'library_paths': self.library_paths.copy(),
            'libraries': {
                name: {
                    'version': str(lib.metadata.version),
                    'dependencies': len(lib.metadata.dependencies),
                    'parameters': len(lib.metadata.parameters),
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
        """Parse and translate library content (placeholder for integration with existing system)."""
        # This would integrate with the existing CQLParser and CQLTranslator
        # For now, return minimal structure
        ast = {"type": "library", "name": name, "content": content}
        translated = {
            "name": name,
            "definitions": {},
            "parameters": {},
            "context": "Patient"
        }
        return ast, translated