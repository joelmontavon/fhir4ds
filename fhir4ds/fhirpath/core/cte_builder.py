"""
CTEBuilder - Centralized CTE Management System

This module provides a robust, debuggable CTE (Common Table Expression) system
for the FHIR4DS FHIRPath-to-SQL translator. It replaces the scattered CTE logic
with explicit lifecycle management and dependency tracking.

Key Features:
- Centralized CTE creation and naming
- Dependency resolution with circular dependency detection
- Automatic deduplication of identical CTEs
- Proper ordering of CTEs in final SQL
- Comprehensive debugging information
"""

from collections import OrderedDict
from typing import Optional, Dict, List, Set
import hashlib
import re


class CTEBuilder:
    """
    Centralized CTE management with explicit lifecycle.
    
    This class handles all aspects of CTE management:
    - Creating and naming CTEs with unique identifiers
    - Tracking dependencies between CTEs
    - Providing references to CTEs for use in other expressions
    - Building final SQL with proper CTE ordering
    - Deduplication of identical CTE definitions
    - Debugging support with comprehensive state information
    
    Usage:
        builder = CTEBuilder()
        cte_name = builder.create_cte("filter", "SELECT * FROM table WHERE active = true")
        reference = builder.reference(cte_name, "active_count")
        final_sql = builder.build_final_query("SELECT COUNT(*) FROM main_table")
    """
    
    def __init__(self):
        """Initialize empty CTE builder."""
        # Ordered dictionary to maintain CTE creation order
        self.ctes: OrderedDict[str, str] = OrderedDict()
        
        # Counter for generating unique CTE names
        self.cte_counter: int = 0
        
        # Dependency tracking: cte_name -> [dependency_names]
        self.dependencies: Dict[str, List[str]] = {}
        
        # Deduplication tracking: sql_hash -> cte_name
        self._sql_hashes: Dict[str, str] = {}
        
        # Debug information tracking
        self._creation_order: List[str] = []
        self._operation_types: Dict[str, str] = {}
    
    def create_cte(self, operation: str, sql: str, dependencies: Optional[List[str]] = None) -> str:
        """
        Create new CTE with auto-generated unique name.
        
        This is the primary method for creating CTEs. It generates a unique name
        based on the operation type and an incrementing counter.
        
        Args:
            operation: Type of operation for naming (e.g., 'where_filter', 'select_transform')
                      Used to create readable CTE names like 'where_filter_1', 'select_transform_2'
            sql: SQL expression for the CTE body
                Must be a valid SELECT statement
            dependencies: List of CTE names this CTE depends on (optional)
                         Used for proper ordering in final SQL
            
        Returns:
            CTE name that can be used in references (e.g., 'where_filter_1')
            
        Raises:
            ValueError: If SQL is empty or dependencies reference non-existent CTEs
            
        Example:
            cte_name = builder.create_cte(
                "filter", 
                "SELECT id, name FROM patients WHERE active = true",
                dependencies=[]
            )
            # Returns: "filter_1"
        """
        if not sql or not sql.strip():
            raise ValueError("CTE SQL cannot be empty")
        
        # Validate dependencies exist
        if dependencies:
            missing_deps = [dep for dep in dependencies if dep not in self.ctes]
            if missing_deps:
                raise ValueError(f"Dependencies not found: {missing_deps}. Available CTEs: {list(self.ctes.keys())}")
        
        # Generate unique name
        self.cte_counter += 1
        cte_name = f"{operation}_{self.cte_counter}"
        
        # Store CTE and metadata
        self.ctes[cte_name] = sql.strip()
        self.dependencies[cte_name] = dependencies or []
        self._creation_order.append(cte_name)
        self._operation_types[cte_name] = operation
        
        # Update hash tracking for deduplication
        sql_hash = self._hash_sql(sql)
        self._sql_hashes[sql_hash] = cte_name
        
        return cte_name
    
    def add_cte(self, name: str, sql: str, dependencies: Optional[List[str]] = None) -> str:
        """
        Add CTE with explicit name, with automatic deduplication.
        
        This method allows specifying an explicit CTE name rather than generating one.
        It also performs deduplication - if a CTE with identical SQL already exists,
        it returns the existing CTE name instead of creating a duplicate.
        
        Args:
            name: Explicit name for the CTE
            sql: SQL expression for the CTE body
            dependencies: List of CTE names this CTE depends on (optional)
            
        Returns:
            CTE name (either the provided name or existing name if deduplicated)
            
        Example:
            # First call creates new CTE
            name1 = builder.add_cte("user_filter", "SELECT * FROM users WHERE active = true")
            # Returns: "user_filter"
            
            # Second call with same SQL returns existing name
            name2 = builder.add_cte("active_users", "SELECT * FROM users WHERE active = true") 
            # Returns: "user_filter" (deduplicated)
        """
        if not sql or not sql.strip():
            raise ValueError("CTE SQL cannot be empty")
        
        # Check for existing CTE with same SQL (deduplication)
        sql_hash = self._hash_sql(sql)
        if sql_hash in self._sql_hashes:
            existing_name = self._sql_hashes[sql_hash]
            # Update dependencies for existing CTE if provided
            if dependencies:
                existing_deps = set(self.dependencies.get(existing_name, []))
                new_deps = set(dependencies)
                self.dependencies[existing_name] = list(existing_deps | new_deps)
            return existing_name
        
        # Validate dependencies exist  
        if dependencies:
            missing_deps = [dep for dep in dependencies if dep not in self.ctes]
            if missing_deps:
                raise ValueError(f"Dependencies not found: {missing_deps}. Available CTEs: {list(self.ctes.keys())}")
        
        # Add new CTE
        self.ctes[name] = sql.strip()
        self.dependencies[name] = dependencies or []
        self._creation_order.append(name)
        self._operation_types[name] = "explicit"
        self._sql_hashes[sql_hash] = name
        
        return name
    
    def reference(self, cte_name: str, column: Optional[str] = None) -> str:
        """
        Generate SQL reference to an existing CTE.
        
        This method creates a subquery that references a CTE, optionally selecting
        a specific column. The reference can be used in other SQL expressions.
        
        Args:
            cte_name: Name of CTE to reference (must exist)
            column: Specific column to select from CTE (optional)
                   If not provided, selects all columns with SELECT *
                   
        Returns:
            SQL expression referencing the CTE as a subquery
            
        Raises:
            ValueError: If CTE name doesn't exist
            
        Examples:
            # Reference entire CTE
            ref = builder.reference("filter_1")
            # Returns: "(SELECT * FROM filter_1)"
            
            # Reference specific column
            ref = builder.reference("filter_1", "patient_count")
            # Returns: "(SELECT patient_count FROM filter_1)"
        """
        if cte_name not in self.ctes:
            available = list(self.ctes.keys())
            raise ValueError(f"CTE '{cte_name}' not found. Available CTEs: {available}")
        
        if column:
            return f"(SELECT {column} FROM {cte_name})"
        else:
            return f"(SELECT * FROM {cte_name})"
    
    def has_ctes(self) -> bool:
        """
        Check if any CTEs have been created.
        
        Returns:
            True if CTEs exist, False otherwise
        """
        return len(self.ctes) > 0
    
    def get_cte_count(self) -> int:
        """
        Get the number of CTEs created.
        
        Returns:
            Number of CTEs in the builder
        """
        return len(self.ctes)
    
    def _hash_sql(self, sql: str) -> str:
        """
        Generate hash for SQL deduplication.
        
        Creates a normalized hash of SQL for deduplication purposes.
        Normalizes whitespace to avoid false duplicates.
        
        Args:
            sql: SQL string to hash
            
        Returns:
            8-character hash string
        """
        # Normalize whitespace for consistent hashing
        normalized = ' '.join(sql.split())
        return hashlib.md5(normalized.encode()).hexdigest()[:8]
    
    def _resolve_dependencies(self) -> List[str]:
        """
        Resolve CTE dependencies to determine proper ordering.
        
        Uses topological sorting to order CTEs so that dependencies appear
        before CTEs that depend on them. Detects circular dependencies.
        
        Returns:
            List of CTE names in dependency order (dependencies first)
            
        Raises:
            ValueError: If circular dependencies or missing dependencies detected
            
        Algorithm:
            1. Find CTEs with no unresolved dependencies
            2. Add them to ordered list
            3. Remove them from remaining set
            4. Repeat until all CTEs processed
            5. If no progress made, detect circular dependencies
        """
        ordered = []
        remaining = set(self.ctes.keys())
        
        # Safety counter to prevent infinite loops
        max_iterations = len(self.ctes) + 1
        iteration = 0
        
        while remaining and iteration < max_iterations:
            iteration += 1
            
            # Find CTEs with no unresolved dependencies
            ready = [
                name for name in remaining 
                if all(dep in ordered for dep in self.dependencies.get(name, []))
            ]
            
            if not ready:
                # No progress possible - check for issues
                missing_deps = []
                circular_candidates = []
                
                for name in remaining:
                    for dep in self.dependencies.get(name, []):
                        if dep not in ordered and dep not in remaining:
                            missing_deps.append(f"{name} -> {dep}")
                        elif dep in remaining:
                            circular_candidates.append(f"{name} -> {dep}")
                
                if missing_deps:
                    raise ValueError(f"Missing CTE dependencies: {missing_deps}")
                else:
                    raise ValueError(f"Circular dependency detected in CTEs. Candidates: {circular_candidates}")
            
            # Add ready CTEs to ordered list
            ordered.extend(ready)
            remaining -= set(ready)
        
        if remaining:
            raise ValueError(f"Failed to resolve all CTE dependencies. Remaining: {remaining}")
        
        return ordered
    
    def build_final_query(self, main_sql: str) -> str:
        """
        Build complete SQL query with CTEs in proper dependency order.
        
        This is the final step that combines all CTEs into a complete SQL query.
        CTEs are ordered according to their dependencies and combined with the
        main SQL statement.
        
        Args:
            main_sql: Main SELECT statement that may reference CTEs
                     Should be a complete SELECT statement
                     
        Returns:
            Complete SQL with WITH clause if CTEs exist, otherwise just main_sql
            
        Raises:
            ValueError: If dependency resolution fails
            
        Example:
            main_sql = "SELECT COUNT(*) as total FROM filtered_patients"
            final_query = builder.build_final_query(main_sql)
            # Returns:
            # WITH filter_1 AS (SELECT * FROM patients WHERE active = true),
            #      count_2 AS (SELECT COUNT(*) as cnt FROM filter_1)
            # SELECT COUNT(*) as total FROM filtered_patients
        """
        if not self.ctes:
            return main_sql
        
        # Resolve dependencies and build WITH clause
        ordered_names = self._resolve_dependencies()
        
        cte_clauses = []
        for name in ordered_names:
            sql = self.ctes[name]
            cte_clauses.append(f"{name} AS ({sql})")
        
        with_clause = "WITH " + ",\n     ".join(cte_clauses)
        return f"{with_clause}\n{main_sql}"
    
    def merge_from(self, other: 'CTEBuilder') -> None:
        """
        Merge CTEs from another CTEBuilder instance.
        
        This is useful when combining CTEs from nested operations or
        when multiple generators need to share CTEs.
        
        Args:
            other: Another CTEBuilder instance to merge from
            
        Note:
            - Deduplication is applied during merge
            - Dependencies are preserved
            - Counter is updated to avoid name conflicts
        """
        if not isinstance(other, CTEBuilder):
            raise ValueError("Can only merge from another CTEBuilder instance")
        
        # Update counter to avoid conflicts
        self.cte_counter = max(self.cte_counter, other.cte_counter)
        
        # Merge CTEs with deduplication
        for name, sql in other.ctes.items():
            self.add_cte(name, sql, other.dependencies.get(name, []))
        
        # Merge creation order and operation types
        self._creation_order.extend(other._creation_order)
        self._operation_types.update(other._operation_types)
    
    def debug_info(self) -> Dict:
        """
        Return comprehensive debugging information about current CTE state.
        
        Provides detailed information about CTEs, dependencies, and internal state
        for debugging and monitoring purposes.
        
        Returns:
            Dictionary with comprehensive debugging information including:
            - Basic statistics (count, names)
            - Dependency information
            - SQL snippets for inspection
            - Creation order
            - Operation types
            - Hash information for deduplication tracking
            
        Example:
            debug_info = builder.debug_info()
            print(f"Created {debug_info['cte_count']} CTEs")
            print(f"Names: {debug_info['cte_names']}")
        """
        return {
            # Basic statistics
            'cte_count': len(self.ctes),
            'cte_names': list(self.ctes.keys()),
            'creation_order': self._creation_order.copy(),
            
            # Dependency information
            'dependencies': self.dependencies.copy(),
            'has_dependencies': any(deps for deps in self.dependencies.values()),
            
            # SQL information
            'sql_snippets': {
                name: (sql[:100] + "..." if len(sql) > 100 else sql)
                for name, sql in self.ctes.items()
            },
            
            # Operation tracking
            'operation_types': self._operation_types.copy(),
            'operations_summary': self._summarize_operations(),
            
            # Deduplication tracking
            'unique_sql_count': len(self._sql_hashes),
            'deduplication_ratio': 1.0 - (len(self._sql_hashes) / max(len(self.ctes), 1)) if len(self.ctes) > 0 else 0.0,
            
            # Internal state
            'next_counter': self.cte_counter + 1,
        }
    
    def _summarize_operations(self) -> Dict[str, int]:
        """
        Summarize operation types for debugging.
        
        Returns:
            Dictionary mapping operation types to counts
        """
        summary = {}
        for operation in self._operation_types.values():
            summary[operation] = summary.get(operation, 0) + 1
        return summary
    
    def validate_state(self) -> List[str]:
        """
        Validate internal state consistency and return any issues found.
        
        Performs comprehensive validation of the CTEBuilder state to detect
        potential issues or inconsistencies.
        
        Returns:
            List of validation issues (empty if no issues found)
            
        Example:
            issues = builder.validate_state()
            if issues:
                print("CTEBuilder validation issues:")
                for issue in issues:
                    print(f"  - {issue}")
        """
        issues = []
        
        # Check that all dependencies exist
        for cte_name, deps in self.dependencies.items():
            if cte_name not in self.ctes:
                issues.append(f"Dependency entry for non-existent CTE: {cte_name}")
            for dep in deps:
                if dep not in self.ctes:
                    issues.append(f"CTE {cte_name} depends on non-existent CTE: {dep}")
        
        # Check hash consistency
        for cte_name, sql in self.ctes.items():
            expected_hash = self._hash_sql(sql)
            if expected_hash not in self._sql_hashes:
                issues.append(f"Missing hash entry for CTE {cte_name}")
            elif self._sql_hashes[expected_hash] != cte_name:
                issues.append(f"Hash mismatch for CTE {cte_name}")
        
        # Check creation order consistency
        for cte_name in self._creation_order:
            if cte_name not in self.ctes:
                issues.append(f"Creation order references non-existent CTE: {cte_name}")
        
        # Check for orphaned metadata
        for cte_name in self._operation_types:
            if cte_name not in self.ctes:
                issues.append(f"Operation type entry for non-existent CTE: {cte_name}")
        
        return issues
    
    def clear(self) -> None:
        """
        Clear all CTEs and reset builder to initial state.
        
        Useful for reusing the same builder instance or for testing.
        """
        self.ctes.clear()
        self.cte_counter = 0
        self.dependencies.clear()
        self._sql_hashes.clear()
        self._creation_order.clear()
        self._operation_types.clear()
    
    def __str__(self) -> str:
        """
        String representation for debugging.
        
        Returns:
            Human-readable summary of CTEBuilder state
        """
        if not self.ctes:
            return "CTEBuilder(empty)"
        
        return f"CTEBuilder({len(self.ctes)} CTEs: {list(self.ctes.keys())})"
    
    def __repr__(self) -> str:
        """
        Detailed representation for debugging.
        
        Returns:
            Detailed string representation including CTEs and dependencies
        """
        return f"CTEBuilder(ctes={len(self.ctes)}, dependencies={len(self.dependencies)}, counter={self.cte_counter})"