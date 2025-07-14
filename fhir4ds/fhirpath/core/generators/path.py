"""
Path handling for FHIRPath expressions.

This module handles path construction and JSON navigation from FHIRPath AST
into SQL path expressions and JSON extraction operations.
"""

from typing import Any, Optional, List


class PathHandler:
    """Handles path node processing for FHIRPath to SQL conversion."""
    
    def __init__(self, ast_nodes, dialect, json_column: str = "resource", fhir_choice_types=None):
        """
        Initialize the path handler.
        
        Args:
            ast_nodes: Dictionary of AST node classes for type checking
            dialect: Database dialect for specific SQL generation
            json_column: Name of the JSON column in the database
            fhir_choice_types: Dictionary of FHIR choice types for handling
        """
        self.IdentifierNode = ast_nodes['IdentifierNode']
        self.PathNode = ast_nodes['PathNode']
        self.IndexerNode = ast_nodes['IndexerNode']
        self.FunctionCallNode = ast_nodes.get('FunctionCallNode')
        self.dialect = dialect
        self.json_column = json_column
        self.fhir_choice_types = fhir_choice_types or {}
        
        # Configuration flags that will be set by main generator
        self.enable_cte = True
        self.in_where_context = False
        
    def set_generator_context(self, generator):
        """Set context from the main generator for complex operations."""
        self.generator = generator
        self.enable_cte = getattr(generator, 'enable_cte', True)
        self.in_where_context = getattr(generator, 'in_where_context', False)
    
    def visit_identifier(self, node, resource_type: Optional[str] = None) -> str:
        """
        Visit an identifier node and convert to SQL.
        
        Args:
            node: Identifier AST node
            resource_type: Current FHIR resource type for choice type handling
            
        Returns:
            SQL expression for the identifier
        """
        if resource_type == 'Observation' and node.name == 'value':
            # Handle Observation.value choice type by coalescing possible value[x] fields.
            # This makes 'Observation.value' resolve to the actual typed data if present.
            possible_value_fields = [
                'valueQuantity', 'valueCodeableConcept', 'valueString', 'valueBoolean',
                'valueInteger', 'valueRange', 'valueRatio', 'valueSampledData',
                'valueTime', 'valueDateTime', 'valuePeriod'
            ]
            # COALESCE returns the first non-null expression.
            coalesce_args = ", ".join([self.extract_json_object(self.json_column, f"$.{field}") for field in possible_value_fields])
            return f"COALESCE({coalesce_args})"
        elif resource_type == 'Patient' and node.name == 'deceased':
            # Handle Patient.deceased choice type by coalescing possible deceased[x] fields.
            possible_deceased_fields = ['deceasedBoolean', 'deceasedDateTime']
            coalesce_args = ", ".join([self.extract_json_object(self.json_column, f"$.{field}") for field in possible_deceased_fields])
            return f"COALESCE({coalesce_args})"

        # Default behavior for other identifiers
        json_path = f"$.{node.name}"
        return self.extract_json_object(self.json_column, json_path)
    
    def visit_path(self, node) -> str:
        """
        Visit a path node and build the JSON path sequentially.
        
        Args:
            node: Path AST node with segments
            
        Returns:
            SQL expression for the complete path
        """
        if len(node.segments) == 0:
            return self.json_column
        
        return self.build_path_expression(node.segments)
    
    def build_path_expression(self, segments) -> str:
        """
        Build SQL expression from path segments.
        
        For complex path building, delegate to main generator to avoid circular dependencies.
        
        Args:
            segments: List of path segments (identifiers, indexers, etc.)
            
        Returns:
            SQL expression for the complete path
        """
        # Delegate complex path building to main generator for now
        # This avoids circular dependency issues with function calls in paths
        if hasattr(self, 'generator'):
            return self.generator.build_path_expression(segments)
        else:
            # Simple fallback for basic identifier-only paths
            if not segments:
                return self.json_column
            
            current_sql = self.json_column
            for segment in segments:
                if isinstance(segment, self.IdentifierNode):
                    current_sql = self._apply_identifier_segment(segment.name, current_sql)
                else:
                    raise ValueError(f"Cannot handle complex path segment without generator context: {type(segment)}")
            
            return current_sql
    
    def _apply_identifier_segment(self, identifier_name: str, base_sql: str) -> str:
        """
        Apply an identifier segment to a base SQL expression with proper array flattening.
        
        Args:
            identifier_name: Name of the identifier segment
            base_sql: Base SQL expression to extend
            
        Returns:
            SQL expression with the identifier segment applied
        """
        
        # Optimize complex base expressions early
        if hasattr(self, 'generator') and self.generator._is_complex_expression(base_sql):
            base_sql = self.generator._create_optimized_expression(base_sql, f"field_{identifier_name}")
        
        # Handle optimized index markers from arithmetic indexing
        if base_sql.startswith('__OPTIMIZED_INDEX__'):
            # Parse the marker: __OPTIMIZED_INDEX__<base>__<path>__
            marker_content = base_sql[len('__OPTIMIZED_INDEX__'):]
            if marker_content.endswith('__'):
                marker_content = marker_content[:-2]  # Remove trailing __
            
            # Split on the pattern __$. to separate base and path
            if '__$.' in marker_content:
                json_base, path_part = marker_content.split('__$.', 1)
                # Append the new identifier to the path
                new_path = f"$.{path_part}.{identifier_name}"
                # Use json_extract_string for leaf fields (likely string values)
                return self.extract_json_field(json_base, new_path)
            elif '__' in marker_content:
                # Fallback parsing
                parts = marker_content.split('__')
                if len(parts) >= 2:
                    json_base = parts[0]
                    current_path = parts[1]
                    new_path = f"{current_path}.{identifier_name}"
                    return self.extract_json_field(json_base, new_path)
        
        # Clean array-aware field extraction using database JSON capabilities
        # Handle arrays properly at each segment without complex pattern matching
        if 'json_extract(' in base_sql and not 'json_group_array' in base_sql and not 'json_each' in base_sql:
            # Extract the base expression and current path from the json_extract
            import re
            match = re.match(r"json_extract\(([^,]+),\s*'([^']+)'\)", base_sql.strip())
            if match:
                json_base, current_path = match.groups()
                new_path = f"{current_path}.{identifier_name}"
                
                # Use CTE approach for complex expressions in json_extract pattern (but not in WHERE context)
                if (self.enable_cte and 
                    not self.in_where_context and 
                    hasattr(self, 'generator') and
                    self.generator._should_use_cte_unified(base_sql, 'array_extraction')):
                    try:
                        return self.generator._generate_array_extraction_with_cte(base_sql, identifier_name)
                    except Exception as e:
                        # Fallback to direct dialect method if CTE generation fails
                        print(f"CTE generation failed for json_extract array extraction, falling back to direct method: {e}")
                
                # Use dialect-specific array-aware extraction pattern
                # For arrays, use [*] on the parent, not the child: $.address[*].line not $.address.line[*]
                return self.dialect.extract_nested_array_path(json_base, current_path, identifier_name, new_path)
        
        # Use CTE approach for complex array extraction operations (but not in WHERE context)
        if (self.enable_cte and 
            not self.in_where_context and 
            hasattr(self, 'generator') and
            self.generator._should_use_cte_unified(base_sql, 'array_extraction')):
            try:
                return self.generator._generate_array_extraction_with_cte(base_sql, identifier_name)
            except Exception as e:
                # Fallback to direct dialect method if CTE generation fails
                print(f"CTE generation failed for array extraction, falling back to direct method: {e}")
        
        # Use dialect-specific array-aware path extraction for all cases
        # This replaces the complex database-specific fallback logic with a cleaner approach
        return self.dialect.extract_nested_array_path(base_sql, "$", identifier_name, f"$.{identifier_name}")
    
    def _apply_indexer_segment(self, indexer_node, base_sql: str) -> str:
        """
        Apply an indexer segment to a base SQL expression.
        
        Args:
            indexer_node: Indexer AST node with index expression
            base_sql: Base SQL expression to extend
            
        Returns:
            SQL expression with the indexer segment applied
        """
        # For now, delegate to the main generator's indexer handling
        # This could be extracted further if needed
        if hasattr(self, 'generator'):
            return self.generator._apply_indexer_to_expression(indexer_node, base_sql)
        else:
            # Simple fallback for basic indexing
            index_sql = str(indexer_node.index)  # Simplified
            return f"json_extract({base_sql}, '$[{index_sql}]')"
    
    def extract_json_field(self, column: str, path: str) -> str:
        """Extract a JSON field as text using dialect-specific methods"""
        return self.dialect.extract_json_field(column, path)
    
    def extract_json_object(self, column: str, path: str) -> str:
        """Extract a JSON object using dialect-specific methods"""
        return self.dialect.extract_json_object(column, path)
    
    def _path_to_string(self, node) -> str:
        """
        Convert a path node to string representation for pattern matching.
        
        Args:
            node: Path AST node
            
        Returns:
            String representation of the path
        """
        if isinstance(node, self.PathNode):
            segments = []
            for segment in node.segments:
                if isinstance(segment, self.IdentifierNode):
                    segments.append(segment.name)
            return '.'.join(segments)
        return ""