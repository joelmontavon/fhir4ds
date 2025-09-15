"""
Path navigation operations for FHIRPath pipelines.

These operations handle JSON path navigation, which is the core
functionality that was scattered throughout the old SQLGenerator.
"""

from typing import Optional, Union, List
import logging
from ..core.base import PipelineOperation, SQLState, ExecutionContext, ContextMode

logger = logging.getLogger(__name__)

class PathNavigationOperation(PipelineOperation[SQLState]):
    """
    Navigate to a path segment in JSON structure.
    
    This operation handles navigation from one JSON path to another,
    such as Patient → Patient.name → Patient.name.family.
    
    This replaces the path navigation logic that was embedded in
    the old SQLGenerator.visit_path() method.
    """
    
    def __init__(self, path_segment: str):
        """
        Initialize path navigation operation.
        
        Args:
            path_segment: Path segment to navigate to (e.g., 'name', 'family', 'given')
        """
        self.path_segment = path_segment
        self._validate_path_segment()
    
    def _validate_path_segment(self) -> None:
        """Validate path segment is acceptable."""
        if not self.path_segment:
            raise ValueError("Path segment cannot be empty")
        
        if not isinstance(self.path_segment, str):
            raise ValueError("Path segment must be a string")
        
        # Basic validation - can be enhanced later
        if self.path_segment.startswith('.') or self.path_segment.endswith('.'):
            raise ValueError("Path segment cannot start or end with '.'")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute path navigation.
        
        Args:
            input_state: Current SQL state
            context: Execution context with dialect
            
        Returns:
            New SQL state with path navigation applied
        """
        logger.debug(f"Navigating to path segment: {self.path_segment}")
        
        # Determine if this path yields a collection
        yields_collection = self._path_yields_collection(self.path_segment)
        
        # Update context mode based on result type
        new_context_mode = (
            ContextMode.COLLECTION if yields_collection else input_state.context_mode
        )
        
        # Build new JSON path - handle pipeline chaining correctly
        if input_state.sql_fragment and input_state.sql_fragment != f"{input_state.base_table}.{input_state.json_column}":
            # We have a result from a previous operation - path should be relative to that result
            if input_state.is_collection:
                # For collections, use array access syntax
                new_path = f"$[*].{self.path_segment}"
            else:
                # For single objects, use direct property access
                new_path = f"$.{self.path_segment}"
        else:
            # First operation - build path normally using path context
            new_path = self._build_json_path(input_state.path_context, self.path_segment, input_state.is_collection)
        
        # Generate SQL for path extraction using the NEW context mode, not the old one
        sql_fragment = self._generate_path_sql_with_mode(input_state, new_path, context, new_context_mode)
        
        # Special case: If accessing a field on a collection result
        if (input_state.is_collection and 
            input_state.sql_fragment and 
            ("json_group_array" in input_state.sql_fragment or "jsonb_path_query_array" in input_state.sql_fragment) and
            new_path.startswith("$[*].") and
            not yields_collection):  # Only for non-collection fields
            
            field_name = new_path[5:]  # Remove "$[*]." prefix (5 chars)
            base_expr = input_state.sql_fragment
            
            # Detect if this came from a collection function (skip, take, tail) vs where() filtering
            # Collection functions should return arrays of field values, where() should return scalar
            is_collection_function = ("rn >" in input_state.sql_fragment or  # skip/tail pattern
                                    "rn <= " in input_state.sql_fragment or  # take pattern  
                                    "ROW_NUMBER() OVER" in input_state.sql_fragment)  # general collection function
            
            if is_collection_function:
                # For collection functions: extract field from ALL elements and return as array
                if context.dialect.name.upper() == 'DUCKDB':
                    sql_fragment = f"""
                    CASE 
                        WHEN {base_expr} IS NULL OR json_array_length({base_expr}) = 0 THEN NULL
                        ELSE (
                            SELECT json_group_array(json_extract_string(value, '$.{field_name}'))
                            FROM json_each({base_expr}, '$') 
                            WHERE json_extract_string(value, '$.{field_name}') IS NOT NULL
                        )
                    END
                    """.strip()
                else:  # PostgreSQL
                    sql_fragment = f"""
                    CASE 
                        WHEN {base_expr} IS NULL OR jsonb_array_length({base_expr}) = 0 THEN NULL
                        ELSE (
                            SELECT jsonb_agg(elem ->> '{field_name}')
                            FROM jsonb_array_elements({base_expr}) AS elem
                            WHERE elem ->> '{field_name}' IS NOT NULL
                        )
                    END
                    """.strip()
                
                # Collection functions accessing fields should yield collections
                yields_collection = True
            else:
                # For where() filtering: extract field from first matching item (original logic)
                if context.dialect.name.upper() == 'DUCKDB':
                    sql_fragment = f"""
                    CASE 
                        WHEN {base_expr} IS NULL OR json_array_length({base_expr}) = 0 THEN NULL
                        ELSE json_extract_string(json_extract({base_expr}, '$[0]'), '$.{field_name}')
                    END
                    """.strip()
                else:  # PostgreSQL
                    sql_fragment = f"""
                    CASE 
                        WHEN {base_expr} IS NULL OR jsonb_array_length({base_expr}) = 0 THEN NULL
                        ELSE ({base_expr} -> 0 ->> '{field_name}')
                    END
                    """.strip()
                
                # where() filtering returns scalar value
                yields_collection = False
        
        return input_state.evolve(
            sql_fragment=sql_fragment,
            path_context=new_path,
            is_collection=yields_collection,
            context_mode=new_context_mode
        )
    
    def _build_json_path(self, current_path: str, segment: str, input_is_collection: bool = False) -> str:
        """
        Build new JSON path by appending segment.
        
        Handles array traversal for FHIR resources properly.
        
        Args:
            current_path: Current JSON path (e.g., "$" or "$.name")
            segment: Path segment to append
            input_is_collection: Whether the input is a collection (array)
            
        Returns:
            New JSON path (e.g., "$.name.family" or "$.extension[*].valueBoolean")
        """
        if current_path == "$":
            # If we're at the root and the input is a collection, use array syntax
            if input_is_collection:
                # Use [0] instead of [*] to get scalar result instead of array
                # This matches FHIRPath behavior where field access on filtered collection returns scalar
                return f"$[0].{segment}"
            else:
                return f"$.{segment}"
        else:
            # Check if we need array traversal for collection fields
            # Extract the last segment from current path to check if it's a collection
            last_segment = current_path.split('.')[-1].split('[')[0]  # Handle existing array syntax
            
            # Use array syntax if the previous segment is a collection and we're not already using it
            if (self._path_yields_collection(last_segment) and 
                '[' not in current_path):
                # Check if this looks like a field access within array elements
                # (e.g., extension.valueBoolean where extension is an array)
                return f"{current_path}[*].{segment}"
            else:
                # Standard path navigation
                return f"{current_path}.{segment}"
    
    def _is_collection_method_call(self, segment: str) -> bool:
        """Check if segment is a collection method that shouldn't use array syntax."""
        # Collection methods like first(), last(), etc. should not use array syntax
        # as they operate on the collection itself
        collection_methods = ['first', 'last', 'count', 'exists', 'empty', 'single']
        return segment in collection_methods
    
    def _generate_path_sql(self, input_state: SQLState, json_path: str, 
                          context: ExecutionContext) -> str:
        """
        Generate SQL for path extraction based on dialect and context.
        
        Args:
            input_state: Current SQL state
            json_path: JSON path to extract
            context: Execution context
            
        Returns:
            SQL expression for path extraction
        """
        return self._generate_path_sql_with_mode(input_state, json_path, context, input_state.context_mode)
    
    def _generate_path_sql_with_mode(self, input_state: SQLState, json_path: str, 
                                   context: ExecutionContext, context_mode: 'ContextMode') -> str:
        """
        Generate SQL for path extraction with explicit context mode.
        
        Args:
            input_state: Current SQL state
            json_path: JSON path to extract
            context: Execution context
            context_mode: Explicit context mode to use
            
        Returns:
            SQL expression for path extraction
        """
        # Use the current SQL fragment if it exists (from previous operations)
        # or fall back to the effective base (original table.column)
        if input_state.sql_fragment and input_state.sql_fragment != f"{input_state.base_table}.{input_state.json_column}":
            # We have a result from a previous operation, use it as the base
            base_expr = input_state.sql_fragment
        else:
            # This is the first operation or no previous result, use the table base
            base_expr = input_state.get_effective_base()
        
        # Use dialect-specific JSON path extraction
        if hasattr(context.dialect, 'extract_json_path'):
            return context.dialect.extract_json_path(
                base_expr, 
                json_path, 
                context_mode
            )
        else:
            # Fallback to generic extraction
            return self._generate_generic_path_sql(base_expr, json_path, context)
    
    def _generate_generic_path_sql(self, base_expr: str, json_path: str, 
                                  context: ExecutionContext) -> str:
        """
        Generate generic SQL for path extraction when dialect doesn't provide specific method.
        
        Args:
            base_expr: Base expression to extract from
            json_path: JSON path to extract
            context: Execution context
            
        Returns:
            SQL expression for path extraction
        """
        dialect_name = getattr(context.dialect, 'name', 'unknown').upper()
        
        if dialect_name == 'DUCKDB':
            return f"json_extract_string({base_expr}, '{json_path}')"
        elif dialect_name == 'POSTGRESQL':
            # Convert JSONPath to PostgreSQL syntax
            pg_path = json_path.replace('$.', '')
            if '.' in pg_path:
                # Handle nested paths like "name.family"
                parts = pg_path.split('.')
                pg_expr = base_expr
                for part in parts:
                    pg_expr = f"({pg_expr} -> '{part}')"
                return f"({pg_expr} ->> 0)"  # Extract as text
            else:
                return f"({base_expr} -> '{pg_path}' ->> 0)"
        else:
            # Generic fallback
            return f"json_extract({base_expr}, '{json_path}')"
    
    def _path_yields_collection(self, segment: str) -> bool:
        """
        Determine if this path segment yields a collection.
        
        Args:
            segment: Path segment to analyze
            
        Returns:
            True if segment typically yields a collection in FHIR
        """
        # Import FHIR schema manager to check if field is an array
        try:
            from ...schema import fhir_schema
            return fhir_schema.is_array_field(segment)
        except ImportError:
            # Fallback to hardcoded list if schema not available
            collection_paths = {
                'name', 'telecom', 'address', 'identifier', 'extension',
                'given', 'contact', 'communication', 'link', 'photo',
                'qualification', 'role', 'specialty', 'location',
                'availableTime', 'notAvailable', 'endpoint',
                'category', 'coding', 'component', 'performer',
                'interpretation', 'referenceRange', 'related', 'bodySite',
                'dosage', 'ingredient', 'specimen', 'result', 'image', 'media',
                'participant', 'diagnosis', 'reason', 'modifierExtension', 'contained'
            }
            return segment in collection_paths
    
    def optimize_for_dialect(self, dialect) -> 'PathNavigationOperation':
        """
        Optimize path navigation for specific dialect.
        
        Path operations are generally dialect-agnostic since the
        actual SQL generation is delegated to the dialect.
        
        Args:
            dialect: Target database dialect
            
        Returns:
            Potentially optimized path operation (usually unchanged)
        """
        # Path operations are inherently dialect-agnostic
        # Optimization happens in the dialect's extract_json_path method
        return self
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        return f"path({self.path_segment})"
    
    def validate_preconditions(self, input_state: SQLState, context: ExecutionContext) -> None:
        """
        Validate that path navigation can be performed.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Raises:
            ValueError: If preconditions not met
        """
        if not input_state.sql_fragment:
            raise ValueError("Cannot navigate path on empty SQL fragment")
        
        # Check if dialect supports JSON path extraction
        dialect_name = getattr(context.dialect, 'name', 'unknown').upper()
        if dialect_name not in ['DUCKDB', 'POSTGRESQL'] and not hasattr(context.dialect, 'extract_json_path'):
            raise ValueError(f"Dialect {dialect_name} does not support JSON path extraction")
    
    def estimate_complexity(self, input_state: SQLState, context: ExecutionContext) -> int:
        """
        Estimate complexity of path navigation.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            Complexity score (0-10)
        """
        base_complexity = 2  # Path navigation is relatively simple
        
        # Collection paths are more complex
        if self._path_yields_collection(self.path_segment):
            base_complexity += 2
        
        # Complex expressions are more expensive
        if len(input_state.sql_fragment) > 100:
            base_complexity += 1
        
        return min(base_complexity, 10)

class IndexerOperation(PipelineOperation[SQLState]):
    """
    Array indexing operation for JSON arrays.
    
    This handles expressions like:
    - Patient.name[0] (first element)
    - Patient.name[*] (all elements)  
    - Patient.name[Patient.name.count() - 1] (last element)
    
    This replaces the indexer logic from SQLGenerator.visit_indexer().
    """
    
    def __init__(self, index_expr: Union[int, str]):
        """
        Initialize indexer operation.
        
        Args:
            index_expr: Index expression - can be:
                       - int: Numeric index (0, 1, -1)
                       - str: Special index ('*', 'last()')
        """
        self.index_expr = index_expr
        self._validate_index_expr()
    
    def _validate_index_expr(self) -> None:
        """Validate index expression."""
        if isinstance(self.index_expr, int):
            # Numeric indices are always valid
            pass
        elif isinstance(self.index_expr, str):
            # Validate string indices
            valid_string_indices = {'*', 'last()', 'first()'}
            if self.index_expr not in valid_string_indices and not self.index_expr.isdigit():
                raise ValueError(f"Invalid string index: {self.index_expr}")
        else:
            raise ValueError(f"Invalid index expression type: {type(self.index_expr)}")
    
    def execute(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Execute array indexing.
        
        Args:
            input_state: Current SQL state
            context: Execution context
            
        Returns:
            New SQL state with indexing applied
        """
        logger.debug(f"Applying index: {self.index_expr}")
        
        # Ensure we're working with a collection
        if not input_state.is_collection:
            # Convert single value to collection for indexing
            collection_state = self._ensure_collection(input_state, context)
        else:
            collection_state = input_state
        
        # Generate SQL based on index type
        if isinstance(self.index_expr, int):
            sql_fragment = self._generate_numeric_index_sql(
                collection_state, self.index_expr, context
            )
            result_is_collection = False
        elif self.index_expr == '*':
            sql_fragment = collection_state.sql_fragment  # Return whole collection
            result_is_collection = True
        elif isinstance(self.index_expr, str):
            sql_fragment = self._generate_string_index_sql(
                collection_state, self.index_expr, context
            )
            result_is_collection = False
        
        return collection_state.evolve(
            sql_fragment=sql_fragment,
            is_collection=result_is_collection,
            context_mode=ContextMode.COLLECTION if result_is_collection else ContextMode.SINGLE_VALUE
        )
    
    def _ensure_collection(self, input_state: SQLState, context: ExecutionContext) -> SQLState:
        """
        Ensure input state represents a collection.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            SQL state guaranteed to represent a collection
        """
        from ..core.base import ensure_collection_context
        return ensure_collection_context(input_state, context.dialect)
    
    def _generate_numeric_index_sql(self, collection_state: SQLState, 
                                   index: int, context: ExecutionContext) -> str:
        """
        Generate SQL for numeric array indexing.
        
        Args:
            collection_state: State with collection
            index: Numeric index
            context: Execution context
            
        Returns:
            SQL for extracting array element
        """
        dialect_name = getattr(context.dialect, 'name', 'unknown').upper()
        
        if dialect_name == 'DUCKDB':
            return f"json_extract({collection_state.sql_fragment}, '$[{index}]')"
        elif dialect_name == 'POSTGRESQL':
            return f"({collection_state.sql_fragment} -> {index})"
        else:
            # Generic fallback
            return f"json_extract({collection_state.sql_fragment}, '$[{index}]')"
    
    def _generate_string_index_sql(self, collection_state: SQLState,
                                  index_str: str, context: ExecutionContext) -> str:
        """
        Generate SQL for string-based indexing.
        
        Args:
            collection_state: State with collection
            index_str: String index expression
            context: Execution context
            
        Returns:
            SQL for string-based indexing
        """
        if index_str == 'first()' or index_str == '0':
            return self._generate_numeric_index_sql(collection_state, 0, context)
        elif index_str == 'last()':
            dialect_name = getattr(context.dialect, 'name', 'unknown').upper()
            
            if dialect_name == 'DUCKDB':
                return f"""
                json_extract({collection_state.sql_fragment}, 
                            '$[' || (json_array_length({collection_state.sql_fragment}) - 1) || ']')
                """
            elif dialect_name == 'POSTGRESQL':
                return f"""
                ({collection_state.sql_fragment} -> (jsonb_array_length({collection_state.sql_fragment}) - 1))
                """
            else:
                # Generic fallback
                return f"json_extract({collection_state.sql_fragment}, '$[-1]')"
        else:
            raise ValueError(f"Unsupported string index: {index_str}")
    
    def optimize_for_dialect(self, dialect) -> 'IndexerOperation':
        """
        Optimize indexing for specific dialect.
        
        Args:
            dialect: Target database dialect
            
        Returns:
            Potentially optimized indexer operation
        """
        # Most optimizations happen in dialect-specific SQL generation
        return self
    
    def get_operation_name(self) -> str:
        """Get human-readable operation name."""
        return f"index({self.index_expr})"
    
    def validate_preconditions(self, input_state: SQLState, context: ExecutionContext) -> None:
        """
        Validate indexing preconditions.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Raises:
            ValueError: If preconditions not met
        """
        if not input_state.sql_fragment:
            raise ValueError("Cannot apply indexing to empty SQL fragment")
        
        # Check if dialect supports array operations
        dialect_name = getattr(context.dialect, 'name', 'unknown').upper()
        if dialect_name not in ['DUCKDB', 'POSTGRESQL']:
            raise ValueError(f"Dialect {dialect_name} does not support array indexing")
    
    def estimate_complexity(self, input_state: SQLState, context: ExecutionContext) -> int:
        """
        Estimate complexity of indexing operation.
        
        Args:
            input_state: Input SQL state
            context: Execution context
            
        Returns:
            Complexity score (0-10)
        """
        base_complexity = 3  # Indexing is moderately complex
        
        # String indices like 'last()' are more complex
        if isinstance(self.index_expr, str) and self.index_expr not in ['*', '0']:
            base_complexity += 2
        
        return min(base_complexity, 10)