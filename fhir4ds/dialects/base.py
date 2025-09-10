"""
Base database dialect interface for FHIR4DS.

This module defines the abstract base class that all database dialects
must implement to support FHIR data storage and querying.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from ..datastore import QueryResult
    from .context import ExtractionContext, ComparisonContext
import re

# Import FHIR type registry for configuration-driven type handling
from ..fhir.type_registry import FHIRTypeRegistry


class DatabaseDialect(ABC):
    """Abstract base class for database dialect implementations"""
    
    # FHIR type to SQL type mapping (common across dialects)
    FHIR_TYPE_MAP = {
        "base64Binary": "text",
        "boolean": "boolean", 
        "canonical": "text",
        "code": "text",
        "date": "date",
        "dateTime": "timestamp",
        "decimal": "decimal",
        "id": "text",
        "instant": "timestamp",
        "integer": "integer",
        "integer64": "bigint",
        "markdown": "text",
        "oid": "text",
        "string": "text",
        "positiveInt": "integer",
        "time": "time",
        "unsignedInt": "integer",
        "uri": "text",
        "url": "text",
        "uuid": "text",
        "xhtml": "text"
    }
    
    def __init__(self):
        """Initialize dialect with default properties"""
        self.name = self.__class__.__name__.replace('Dialect', '').upper()
        self.supports_jsonb = False
        self.supports_json_functions = True
        self.fhir_types = FHIRTypeRegistry()
        self.json_type = "JSON"
        self.json_extract_function = "json_extract"
        self.json_extract_string_function = "json_extract_string"
        self.json_array_function = "json_array"
        self.json_object_function = "json_object"
        self.json_type_function = "json_type"
        self.json_array_length_function = "json_array_length"
        self.json_each_function = "json_each"
        self.array_agg_function = "array_agg"
        self.string_agg_function = "string_agg"
        self.regex_function = "regexp_extract"
        self.cast_syntax = "::"
        self.quote_char = '"'
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def _handle_operation_error(self, operation: str, error: Exception, sql: str = None) -> None:
        """Standard error handling for dialect operations"""
        error_msg = f"{self.name} {operation} failed: {error}"
        if sql:
            error_msg += f"\nSQL: {sql[:200]}..." if len(sql) > 200 else f"\nSQL: {sql}"
        self.logger.error(error_msg)
        raise RuntimeError(error_msg) from error
    
    def _handle_fallback_warning(self, operation: str, error: Exception, fallback_description: str) -> None:
        """Standard warning logging for fallback operations"""
        self.logger.warning(f"{self.name} {operation} failed, using {fallback_description}: {error}")
    
    @abstractmethod
    def get_connection(self) -> Any:
        """Get the underlying database connection"""
        pass
    
    @abstractmethod
    def execute_sql(self, sql: str, view_def: Optional[Dict] = None) -> 'QueryResult':
        """Execute SQL and return results"""
        pass
    
    @abstractmethod
    def create_fhir_table(self, table_name: str, json_col: str) -> None:
        """Create the FHIR resources table"""
        pass
    
    @abstractmethod
    def bulk_load_json(self, file_path: str, table_name: str, json_col: str) -> int:
        """Attempt bulk loading from JSON file, return number of resources loaded"""
        pass
    
    @abstractmethod
    def insert_resource(self, resource: Dict[str, Any], table_name: str, json_col: str) -> None:
        """Insert a single FHIR resource"""
        pass
    
    @abstractmethod
    def get_resource_counts(self, table_name: str, json_col: str) -> Dict[str, int]:
        """Get counts by resource type"""
        pass
    
    @abstractmethod
    def execute_query(self, sql: str) -> Any:
        """Execute a query and return raw results for FHIRResultSet"""
        pass
    
    @abstractmethod
    def get_query_description(self, connection: Any) -> Any:
        """Get column descriptions from last executed query"""
        pass
    
    # SQL Function Translation Methods
    
    def json_extract(self, column: str, path: str) -> str:
        """Generate JSON extraction SQL for the dialect"""
        return f"{self.json_extract_function}({column}, '{path}')"
    
    def json_extract_string(self, column: str, path: str) -> str:
        """Generate JSON string extraction SQL for the dialect"""
        return f"{self.json_extract_string_function}({column}, '{path}')"
    
    def json_type_check(self, column: str, path: str = None) -> str:
        """Generate JSON type checking SQL for the dialect"""
        if path:
            return f"{self.json_type_function}({self.json_extract_function}({column}, '{path}'))"
        return f"{self.json_type_function}({column})"
    
    def json_array_length(self, column: str, path: str = None) -> str:
        """Generate JSON array length SQL for the dialect"""
        if path:
            return f"{self.json_array_length_function}({self.json_extract_function}({column}, '{path}'))"
        return f"{self.json_array_length_function}({column})"
    
    def json_each(self, column: str, path: str = None) -> str:
        """Generate JSON each iteration SQL for the dialect"""
        if path:
            return f"{self.json_each_function}({self.json_extract_function}({column}, '{path}'))"
        return f"{self.json_each_function}({column})"
    
    @abstractmethod
    def get_json_array_element(self, json_expr: str, index: int) -> str:
        """Extract element at index from JSON array."""
        pass
    
    @abstractmethod
    def get_json_extract_string(self, json_expr: str, path: str) -> str:
        """Extract string value from JSON path."""
        pass
    
    def generate_fhir_type_display(self, input_sql: str, fhir_type: str, 
                                   array_handling: str = "first") -> str:
        """
        Generate SQL for displaying FHIR complex types.
        
        Args:
            input_sql: Base SQL expression 
            fhir_type: FHIR type (HumanName, Address, etc.)
            array_handling: How to handle arrays ("first", "concat", "all")
        """
        type_structure = self.fhir_types.get_type_structure(fhir_type)
        if not type_structure:
            return input_sql
            
        return self._generate_type_display_sql(input_sql, type_structure, array_handling)
    
    @abstractmethod
    def _generate_type_display_sql(self, input_sql: str, type_structure: Dict[str, Any], 
                                   array_handling: str) -> str:
        """Dialect-specific implementation of type display generation."""
        pass
    
    def _apply_display_template(self, template: str, field_extractions: Dict[str, str]) -> str:
        """
        Apply display template with field extractions.
        
        Args:
            template: Display template string (e.g., "{family}, {given.join(' ')}")
            field_extractions: Dictionary mapping field names to SQL expressions
            
        Returns:
            SQL expression implementing the template
        """
        # Simple template implementation for now - can be enhanced later
        # Replace {field} patterns with corresponding SQL expressions
        result = template
        for field, sql_expr in field_extractions.items():
            simple_pattern = f"{{{field}}}"
            if simple_pattern in result:
                result = result.replace(simple_pattern, sql_expr)
        
        return result
    
    # Abstract methods for function-specific SQL generation
    # These replace hardcoded dialect conditionals in functions.py
    
    @abstractmethod
    def generate_array_contains(self, array_sql: str, element_sql: str) -> str:
        """Generate SQL to check if array contains element."""
        pass

    @abstractmethod  
    def generate_array_length(self, array_sql: str) -> str:
        """Generate SQL to get array length."""
        pass

    @abstractmethod
    def generate_substring_sql(self, string_sql: str, start_pos: str, length: Optional[str] = None) -> str:
        """Generate SQL for substring operation.""" 
        pass

    @abstractmethod
    def generate_string_split(self, string_sql: str, delimiter: str) -> str:
        """Generate SQL to split string into array."""
        pass

    @abstractmethod
    def generate_array_element_at(self, array_sql: str, index: int) -> str:
        """Generate SQL to get array element at index."""
        pass

    @abstractmethod
    def generate_case_insensitive_like(self, field_sql: str, pattern: str) -> str:
        """Generate SQL for case-insensitive pattern matching."""
        pass

    @abstractmethod
    def generate_regexp_match(self, field_sql: str, pattern: str) -> str:
        """Generate SQL for regex pattern matching."""
        pass

    @abstractmethod
    def generate_date_arithmetic(self, date_sql: str, interval: str, unit: str) -> str:
        """Generate SQL for date arithmetic operations."""
        pass

    @abstractmethod
    def generate_cast_to_numeric(self, value_sql: str) -> str:
        """Generate SQL to cast value to numeric type."""
        pass

    @abstractmethod
    def generate_null_coalesce(self, *expressions: str) -> str:
        """Generate SQL for null coalescing."""
        pass
    
    @abstractmethod
    def generate_json_typeof(self, json_expr: str) -> str:
        """Generate SQL to get JSON value type."""
        pass
    
    @abstractmethod
    def generate_string_concat(self, *expressions: str) -> str:
        """Generate SQL for string concatenation."""
        pass
    
    @abstractmethod
    def generate_json_array_length(self, json_expr: str) -> str:
        """Generate SQL to get JSON array length."""
        pass
    
    @abstractmethod
    def generate_json_extract(self, json_expr: str, path: str) -> str:
        """Generate SQL to extract JSON path."""
        pass
    
    @abstractmethod
    def generate_json_extract_last(self, json_expr: str) -> str:
        """Generate SQL to extract last element from JSON array."""
        pass
    
    @abstractmethod
    def generate_collection_contains_element(self, collection_expr: str, element_expr: str) -> str:
        """Generate SQL to check if collection contains specific element."""
        pass
    
    @abstractmethod 
    def generate_element_in_collection(self, element_expr: str, collection_expr: str) -> str:
        """Generate SQL to check if element exists in collection."""
        pass
    
    @abstractmethod
    def generate_logical_combine(self, left_condition: str, operator: str, right_condition: str) -> str:
        """Generate SQL to combine two logical conditions with AND/OR operator."""
        pass
    
    @abstractmethod
    def generate_collection_combine(self, first_collection: str, second_collection: str) -> str:
        """Generate SQL to combine two collections."""
        pass
    
    @abstractmethod
    def generate_where_clause_filter(self, collection_expr: str, condition_sql: str) -> str:
        """Generate SQL to filter collection elements based on condition."""
        pass
    
    @abstractmethod
    def generate_select_transformation(self, collection_expr: str, transform_path: str) -> str:
        """Generate SQL to transform collection elements by extracting a path."""
        pass
    
    @abstractmethod
    def generate_json_path_query_array(self, json_expr: str, path_condition: str) -> str:
        """Generate SQL for JSON path query with filtering."""
        pass
    
    @abstractmethod
    def generate_json_group_array_with_condition(self, collection_expr: str, condition: str, value_expr: str = "value") -> str:
        """Generate SQL to create JSON array from filtered elements."""
        pass
    
    @abstractmethod
    def generate_single_element_check(self, collection_expr: str) -> str:
        """Generate SQL to check if collection has exactly one element and return it."""
        pass
    
    @abstractmethod
    def generate_array_slice_operation(self, array_expr: str, start_index: int, count: int = None) -> str:
        """Generate SQL to slice array (skip/take operations)."""
        pass
    
    @abstractmethod
    def generate_array_tail_operation(self, array_expr: str) -> str:
        """Generate SQL to get all elements except first (tail operation)."""
        pass
    
    @abstractmethod
    def generate_array_distinct_operation(self, array_expr: str) -> str:
        """Generate SQL to get distinct elements from array."""
        pass
    
    @abstractmethod
    def generate_descendants_operation(self, base_expr: str) -> str:
        """Generate SQL to get all descendant elements recursively."""
        pass
    
    @abstractmethod
    def generate_union_operation(self, first_collection: str, second_collection: str) -> str:
        """Generate SQL to union two collections."""
        pass
    
    @abstractmethod
    def generate_mathematical_function(self, func_name: str, operand: str) -> str:
        """Generate SQL for mathematical functions (sqrt, ln, exp, etc.)."""
        pass
    
    @abstractmethod
    def generate_date_time_now(self) -> str:
        """Generate SQL for current timestamp."""
        pass
    
    @abstractmethod
    def generate_date_time_today(self) -> str:
        """Generate SQL for current date."""
        pass
    
    @abstractmethod 
    def generate_conditional_expression(self, condition: str, true_expr: str, false_expr: str) -> str:
        """Generate SQL for conditional (iif) expression."""
        pass
    
    @abstractmethod
    def generate_power_operation(self, base_expr: str, exponent_expr: str) -> str:
        """Generate SQL for power operation with two operands."""
        pass
    
    @abstractmethod
    def generate_conversion_functions(self, conversion_type: str, operand: str) -> str:
        """Generate SQL for type conversion functions."""
        pass
    
    @abstractmethod
    def generate_recursive_descendants_with_cte(self, base_expr: str, max_levels: int = 5) -> str:
        """Generate SQL for recursive descendants using CTE."""
        pass
    
    @abstractmethod
    def generate_type_filtering_operation(self, collection_expr: str, type_criteria: str) -> str:
        """Generate SQL for filtering collection by type criteria."""
        pass
    
    @abstractmethod
    def generate_set_intersection_operation(self, first_set: str, second_set: str) -> str:
        """Generate SQL for set intersection operation."""
        pass
    
    @abstractmethod
    def generate_aggregate_with_condition(self, collection_expr: str, aggregate_type: str, condition: str) -> str:
        """Generate SQL for aggregation with filtering condition."""
        pass
    
    @abstractmethod
    def generate_all_elements_match_criteria(self, collection_expr: str, criteria: str) -> str:
        """Generate SQL to check if all elements match criteria."""
        pass
    
    @abstractmethod
    def generate_path_condition_comparison(self, left_expr: str, operator: str, right_expr: str, context_item: str = '$ITEM') -> str:
        """Generate SQL for path-based condition comparison."""
        pass
    
    @abstractmethod
    def generate_field_equality_condition(self, field_path: str, value: str, context_item: str = '$ITEM') -> str:
        """Generate SQL for field equality condition (e.g., use = 'official')."""
        pass
    
    @abstractmethod
    def generate_field_extraction(self, item_placeholder: str, field_name: str) -> str:
        """Generate SQL for field extraction from JSON."""
        pass
    
    @abstractmethod
    def generate_field_exists_check(self, item_placeholder: str, field_name: str) -> str:
        """Generate SQL to check if field exists (is not null)."""
        pass
    
    @abstractmethod
    def generate_field_count_operation(self, item_placeholder: str, field_name: str) -> str:
        """Generate SQL to count elements in a field array."""
        pass
    
    @abstractmethod
    def generate_field_length_operation(self, item_placeholder: str, field_name: str) -> str:
        """Generate SQL to get length of a field string."""
        pass
    
    @abstractmethod
    def generate_path_expression_extraction(self, item_placeholder: str, path_expr: str) -> str:
        """Generate SQL for complex path expression extraction (e.g., name.family.value)."""
        pass
    
    @abstractmethod
    def generate_exclude_operation(self, first_collection: str, second_collection: str) -> str:
        """Generate SQL for exclude operation - elements in first but not in second collection."""
        pass
    
    @abstractmethod
    def generate_boolean_all_true(self, collection_expr: str) -> str:
        """Generate SQL to check if all elements in collection are true."""
        pass
    
    @abstractmethod
    def generate_boolean_all_false(self, collection_expr: str) -> str:
        """Generate SQL to check if all elements in collection are false."""
        pass
    
    @abstractmethod
    def generate_boolean_any_true(self, collection_expr: str) -> str:
        """Generate SQL to check if any element in collection is true."""
        pass
    
    @abstractmethod
    def generate_boolean_any_false(self, collection_expr: str) -> str:
        """Generate SQL to check if any element in collection is false."""
        pass
    
    @abstractmethod
    def generate_children_extraction(self, collection_expr: str) -> str:
        """Generate SQL to extract all immediate children from JSON object/array."""
        pass
    
    @abstractmethod
    def generate_tail_operation(self, array_expr: str) -> str:
        """Generate SQL to get all elements except the first (tail operation)."""
        pass
    
    @abstractmethod
    def generate_collection_contains_element(self, collection_expr: str, search_element: str) -> str:
        """Generate SQL to check if collection contains specific element."""
        pass
    
    @abstractmethod
    def generate_subset_check(self, first_collection: str, second_collection: str) -> str:
        """Generate SQL to check if first collection is subset of second collection."""
        pass
    
    @abstractmethod
    def generate_superset_check(self, first_collection: str, second_collection: str) -> str:
        """Generate SQL to check if first collection is superset of second collection."""
        pass
    
    @abstractmethod
    def generate_repeat_operation(self, input_expr: str, expression: str, max_iterations: int = 10) -> str:
        """Generate SQL for repeat operation with recursive CTE."""
        pass
    
    @abstractmethod
    def generate_aggregate_operation(self, collection_expr: str, aggregator_expr: str, init_value: str = "NULL") -> str:
        """Generate SQL for aggregate/fold operation."""
        pass
    
    @abstractmethod
    def generate_iif_expression(self, condition: str, true_result: str, false_result: str) -> str:
        """Generate SQL for inline if expression."""
        pass
    
    def array_agg(self, expression: str, distinct: bool = False) -> str:
        """Generate array aggregation SQL for the dialect"""
        if distinct:
            return f"{self.array_agg_function}(DISTINCT {expression})"
        return f"{self.array_agg_function}({expression})"
    
    def string_agg(self, expression: str, separator: str, distinct: bool = False) -> str:
        """Generate string aggregation SQL for the dialect"""
        if distinct:
            return f"{self.string_agg_function}(DISTINCT {expression}, {separator})"
        return f"{self.string_agg_function}({expression}, {separator})"
    
    def regex_extract(self, column: str, pattern: str, group: int = 1) -> str:
        """Generate regex extraction SQL for the dialect"""
        return f"{self.regex_function}({column}, '{pattern}', {group})"
    
    def cast_to_type(self, expression: str, target_type: str) -> str:
        """Generate type casting SQL for the dialect"""
        return f"{expression}{self.cast_syntax}{target_type}"
    
    def quote_identifier(self, identifier: str) -> str:
        """Quote an identifier for the dialect"""
        return f"{self.quote_char}{identifier}{self.quote_char}"
    
    # Pipeline-specific dialect methods for refactoring hardcoded logic
    
    def string_split_reference(self, input_sql: str) -> str:
        """Split reference string like 'ResourceType/id' to extract id part"""
        # Default implementation - to be overridden by specific dialects
        raise NotImplementedError(f"string_split_reference not implemented for {self.name}")
    
    def starts_with_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate starts_with condition for field matching"""
        # Default implementation - to be overridden by specific dialects  
        raise NotImplementedError(f"starts_with_condition not implemented for {self.name}")
    
    def ends_with_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate ends_with condition for field matching"""
        # Default implementation - to be overridden by specific dialects
        raise NotImplementedError(f"ends_with_condition not implemented for {self.name}")
    
    def contains_condition(self, item_expr: str, field_name: str, value: str) -> str:
        """Generate contains condition for field matching"""
        # Default implementation - to be overridden by specific dialects
        raise NotImplementedError(f"contains_condition not implemented for {self.name}")
    
    def get_value_primitive_sql(self, input_sql: str) -> str:
        """Generate SQL for getValue() function handling primitive and complex types"""
        # Default implementation - to be overridden by specific dialects
        raise NotImplementedError(f"get_value_primitive_sql not implemented for {self.name}")
    
    def resolve_reference_sql(self, input_sql: str) -> str:
        """Generate SQL for resolve() function to resolve FHIR references"""
        # Default implementation - to be overridden by specific dialects
        raise NotImplementedError(f"resolve_reference_sql not implemented for {self.name}")
    
    def translate_sql(self, sql: str) -> str:
        """
        Translate SQL from DuckDB format to this dialect.
        This is the main function that converts SQL queries.
        """
        translated = sql
        
        # Replace function calls
        function_mappings = {
            'json_extract': self.json_extract_function,
            'json_extract_string': self.json_extract_string_function, 
            'json_type': self.json_type_function,
            'json_array_length': self.json_array_length_function,
            'json_each': self.json_each_function,
            'json_array': self.json_array_function,
            'json_object': self.json_object_function,
            'array_agg': self.array_agg_function,
            'string_agg': self.string_agg_function
        }
        
        for duckdb_func, dialect_func in function_mappings.items():
            if duckdb_func != dialect_func:
                # Use word boundaries to avoid partial replacements
                pattern = r'\b' + re.escape(duckdb_func) + r'\b'
                translated = re.sub(pattern, dialect_func, translated)
        
        return self._apply_dialect_specific_transforms(translated)
    
    def _apply_dialect_specific_transforms(self, sql: str) -> str:
        """Apply dialect-specific transformations. Override in subclasses."""
        return sql
    
    def supports_feature(self, feature: str) -> bool:
        """Check if dialect supports a specific feature"""
        features = {
            'jsonb': self.supports_jsonb,
            'json_functions': self.supports_json_functions,
            'regex': hasattr(self, 'regex_function'),
            'array_functions': True  # Most dialects support basic array functions
        }
        return features.get(feature, False)
    
    # Abstract dialect-specific SQL generation methods
    # These should be implemented by each dialect
    
    @abstractmethod
    def extract_json_field(self, column: str, path: str) -> str:
        """Extract a JSON field as text - database specific implementation"""
        pass
    
    @abstractmethod
    def extract_json_text(self, column: str, path: str) -> str:
        """
        Extract a JSON field as text/string type for string comparisons and display.
        
        This method should return SQL that extracts JSON values as text/string type,
        suitable for string comparisons, display, and text operations.
        
        Args:
            column: The column containing JSON data
            path: JSONPath expression (e.g., '$.field', '$.nested.field')
            
        Returns:
            SQL expression that extracts the JSON value as text type
        """
        pass
    
    @abstractmethod 
    def extract_json_object(self, column: str, path: str) -> str:
        """Extract a JSON object - database specific implementation"""
        pass
    
    @abstractmethod
    def iterate_json_array(self, column: str, path: str) -> str:
        """Iterate over JSON array elements - database specific implementation"""
        pass
    
    @abstractmethod
    def check_json_exists(self, column: str, path: str) -> str:
        """Check if JSON path exists - database specific implementation"""
        pass
    
    @abstractmethod
    def get_json_type(self, column: str) -> str:
        """Get JSON value type - database specific implementation"""
        pass
    
    def json_type(self, column: str) -> str:
        """Alias for get_json_type for backward compatibility"""
        return self.get_json_type(column)
    
    def extract_json_smart(self, column: str, path: str, 
                          context: Optional['ExtractionContext'] = None,
                          comparison_context: Optional['ComparisonContext'] = None) -> str:
        """
        Smart JSON extraction that selects appropriate method based on context.
        
        This method determines whether to use extract_json_object() or extract_json_text()
        based on the extraction context and comparison context.
        
        Args:
            column: The column containing JSON data
            path: JSONPath expression
            context: Explicit extraction context (overrides auto-detection)
            comparison_context: Context information for comparison operations
            
        Returns:
            SQL expression using appropriate extraction method
        """
        # Import here to avoid circular imports
        from .context import ExtractionContext, determine_extraction_context
        
        # Determine context if not explicitly provided
        if context is None:
            context = determine_extraction_context(comparison_context)
        
        # Route to appropriate extraction method
        if context in [ExtractionContext.TEXT_COMPARISON, ExtractionContext.TEXT_DISPLAY]:
            return self.extract_json_text(column, path)
        else:
            return self.extract_json_object(column, path)

    # Backward compatibility alias
    def extract_json_field(self, column: str, path: str) -> str:
        """Backward compatibility - delegates to extract_json_text()"""
        return self.extract_json_text(column, path)
    
    @abstractmethod
    def get_json_array_length(self, column: str, path: str = None) -> str:
        """Get JSON array length - database specific implementation"""
        pass
    
    def get_json_type_constant(self, json_type: str) -> str:
        """Get the correct type constant for comparison with get_json_type()"""
        # Default implementation - return uppercase for consistency
        return json_type.upper()
    
    def get_collection_count_expression(self, base_expr: str) -> str:
        """Generate database-specific count expression for collection functions"""
        # Default implementation using standard JSON type checking
        return f"""
        CASE 
            WHEN {self.get_json_type(base_expr)} = 'ARRAY' THEN {self.get_json_array_length(base_expr)}
            WHEN {base_expr} IS NOT NULL THEN 1
            ELSE 0
        END
        """
    
    def generate_from_json_each(self, column: str) -> str:
        """Generate FROM clause for JSON each iteration - database specific implementation"""
        # Default implementation - subclasses should override for optimal performance
        return f"json_each({column})"
    
    def iterate_json_elements_indexed(self, column: str) -> str:
        """Iterate JSON elements with proper indexing for both arrays and objects - database specific"""
        # Default implementation - subclasses should override for optimal performance  
        return f"json_each({column})"
    
    @abstractmethod
    def aggregate_to_json_array(self, expression: str) -> str:
        """Aggregate values into a JSON array - database specific implementation"""
        pass
    
    @abstractmethod
    def create_json_array(self, *args) -> str:
        """Create a JSON array from arguments - database specific implementation"""
        pass
    
    @abstractmethod
    def create_json_object(self, *args) -> str:
        """Create a JSON object from key-value pairs - database specific implementation"""
        pass
    
    @abstractmethod
    def aggregate_values(self, expression: str, distinct: bool = False) -> str:
        """Array aggregation function - database specific implementation"""
        pass
    
    @abstractmethod
    def aggregate_strings(self, expression: str, separator: str) -> str:
        """String aggregation function - database specific implementation"""
        pass
    def coalesce_empty_array(self, expression: str) -> str:
        """COALESCE with empty array - database specific implementation"""
        pass

    @abstractmethod 
    def join_array_elements(self, base_expr: str, separator_sql: str) -> str:
        """Join array elements with separator - database specific implementation"""
        pass
    
    @abstractmethod
    def extract_nested_array_path(self, json_base: str, current_path: str, identifier_name: str, new_path: str) -> str:
        """Extract path from nested array structures with proper flattening - database specific implementation"""
        pass
    
    @abstractmethod
    def split_string(self, expression: str, delimiter: str) -> str:
        """Split string into array using delimiter - database specific implementation"""
        pass
    
    @abstractmethod
    def substring(self, expression: str, start: str, length: str) -> str:
        """Extract substring from string - database specific implementation"""
        pass
    
    @abstractmethod
    def string_position(self, search_str: str, target_str: str) -> str:
        """Find position of search string in target string - database specific implementation"""
        pass
    
    @abstractmethod
    def string_concat(self, left: str, right: str) -> str:
        """Concatenate two strings - database specific implementation"""
        pass
    
    # New abstract methods for FHIRPath function operations
    
    @abstractmethod
    def try_cast(self, expression: str, target_type: str) -> str:
        """Safe type conversion that returns NULL on failure"""
        pass
    
    @abstractmethod
    def string_to_char_array(self, expression: str) -> str:
        """Split string into array of individual characters"""
        pass
    
    @abstractmethod
    def regex_matches(self, string_expr: str, pattern: str) -> str:
        """Test if string matches regex pattern"""
        pass
    
    @abstractmethod
    def regex_replace(self, string_expr: str, pattern: str, replacement: str) -> str:
        """Replace regex pattern matches with replacement string"""
        pass
    
    @abstractmethod
    def json_group_array(self, value_expr: str, from_clause: str = None) -> str:
        """Aggregate values into JSON array"""
        pass
    
    @abstractmethod
    def json_each(self, json_expr: str, path: str = None) -> str:
        """Iterate over JSON array elements"""
        pass
    
    @abstractmethod
    def json_typeof(self, json_expr: str) -> str:
        """Get JSON type of expression"""
        pass
    
    @abstractmethod
    def json_array_elements(self, json_expr: str, with_ordinality: bool = False) -> str:
        """Extract array elements from JSON"""
        pass
    
    @abstractmethod
    def cast_to_timestamp(self, expression: str) -> str:
        """Cast expression to timestamp/datetime type"""
        pass
    
    @abstractmethod
    def cast_to_time(self, expression: str) -> str:
        """Cast expression to time type"""
        pass
    
    def optimize_cte_definition(self, cte_name: str, cte_expr: str) -> str:
        """Apply dialect-specific CTE optimizations - database specific implementation"""
        # Default implementation - no optimization
        return f"{cte_name} AS ({cte_expr})"
    
    # Pipeline-specific methods for new immutable pipeline architecture
    
    def extract_json_path(self, base_expr: str, json_path: str, context_mode: 'ContextMode') -> str:
        """
        Extract JSON path with context mode awareness.
        
        This method is used by PathNavigationOperation to generate
        dialect-specific SQL for JSON path extraction.
        
        Args:
            base_expr: Base SQL expression (e.g., "table.resource")
            json_path: JSON path to extract (e.g., "$.name.family")
            context_mode: Execution context mode
            
        Returns:
            SQL expression for path extraction
        """
        from ..pipeline.core.base import ContextMode
        
        if context_mode == ContextMode.COLLECTION:
            # Collection context - may need to use json_each or similar
            return self._extract_collection_path(base_expr, json_path)
        elif context_mode == ContextMode.WHERE_CLAUSE:
            # WHERE clause context - optimize for boolean evaluation
            return self._extract_boolean_path(base_expr, json_path)
        else:
            # Single value context - standard extraction
            return self.extract_json_text(base_expr, json_path)
    
    def _extract_collection_path(self, base_expr: str, json_path: str) -> str:
        """Extract JSON path for collection context."""
        # Default implementation - can be overridden by specific dialects
        return self.extract_json_text(base_expr, json_path)
    
    def _extract_boolean_path(self, base_expr: str, json_path: str) -> str:
        """Extract JSON path for boolean context (WHERE clauses)."""
        # Default implementation - check if path exists and is not null
        return f"({self.extract_json_text(base_expr, json_path)} IS NOT NULL)"
    
    def extract_array_element(self, array_expr: str, index: int) -> str:
        """
        Extract element from JSON array by index.
        
        Used by IndexerOperation for array access.
        
        Args:
            array_expr: SQL expression that evaluates to JSON array
            index: Zero-based array index
            
        Returns:
            SQL expression for array element extraction
        """
        # Default implementation using json_extract with array index
        return f"json_extract({array_expr}, '$[{index}]')"
    
    def extract_last_array_element(self, array_expr: str) -> str:
        """
        Extract last element from JSON array.
        
        Args:
            array_expr: SQL expression that evaluates to JSON array
            
        Returns:
            SQL expression for last array element extraction
        """
        # Default implementation - get array length and use index
        return f"json_extract({array_expr}, '$[' || ({self.get_json_array_length(array_expr)} - 1) || ']')"
    
    @abstractmethod
    def generate_flatten_operation(self, collection_expr: str) -> str:
        """Generate SQL to flatten nested collections."""
        pass

    @abstractmethod
    def generate_all_criteria_check(self, collection_expr: str, criteria: str) -> str:
        """Generate SQL to check if all elements in collection satisfy criteria."""
        pass

    @abstractmethod
    def generate_converts_to_check(self, input_expr: str, target_type: str) -> str:
        """Generate SQL to check if value can be converted to target type."""
        pass

    @abstractmethod
    def generate_type_cast(self, input_expr: str, target_type: str) -> str:
        """Generate SQL to cast value to target type."""
        pass

    @abstractmethod
    def generate_of_type_filter(self, collection_expr: str, target_type: str) -> str:
        """Generate SQL to filter collection by type."""
        pass

    @abstractmethod
    def generate_equivalent_check(self, left_expr: str, right_expr: str) -> str:
        """Generate SQL for equivalent (~) operator with type-aware comparison."""
        pass

    @abstractmethod
    def generate_age_in_years(self, birthdate_expr: str) -> str:
        """Generate SQL to calculate age in years from birth date."""
        pass

    @abstractmethod
    def generate_age_in_years_at(self, birthdate_expr: str, as_of_date_expr: str) -> str:
        """Generate SQL to calculate age in years at specific date."""
        pass

    @abstractmethod
    def generate_resource_query(self, input_state: 'SQLState', resource_type: str) -> str:
        """Generate SQL for resource retrieval."""
        pass

    @abstractmethod
    def generate_max_operation(self, operands: list) -> str:
        """Generate SQL for max() function with multiple operands."""
        pass

    @abstractmethod
    def generate_min_operation(self, operands: list) -> str:
        """Generate SQL for min() function with multiple operands."""
        pass

    @abstractmethod
    def generate_median_operation(self, operands: list) -> str:
        """Generate SQL for median() function."""
        pass

    @abstractmethod
    def generate_population_stddev(self, operands: list) -> str:
        """Generate SQL for population standard deviation."""
        pass

    @abstractmethod
    def generate_population_variance(self, operands: list) -> str:
        """Generate SQL for population variance."""
        pass

    @abstractmethod
    def generate_datetime_creation(self, year: str, month: str, day: str, hour: str, minute: str, second: str) -> str:
        """Generate SQL for datetime creation."""
        pass

    @abstractmethod
    def generate_union_operation(self, first_collection: str, second_collection: str) -> str:
        """Generate SQL for union of two collections."""
        pass

    @abstractmethod
    def generate_exists_check(self, fragment: str, is_collection: bool) -> str:
        """Generate SQL for exists/empty checks."""
        pass

    @abstractmethod
    def generate_join_operation(self, collection_expr: str, separator: str) -> str:
        """Generate SQL for join operation (concatenate array elements)."""
        pass

    def optimize_pipeline(self, pipeline: 'FHIRPathPipeline') -> 'FHIRPathPipeline':
        """
        Apply dialect-specific optimizations to a pipeline.
        
        This method can be overridden by specific dialects to apply
        optimizations like merging operations, using dialect-specific
        functions, or restructuring for better performance.
        
        Args:
            pipeline: Pipeline to optimize
            
        Returns:
            Optimized pipeline
        """
        # Default implementation - no optimization
        return pipeline