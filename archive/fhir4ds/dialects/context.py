"""
Context system for determining appropriate JSON extraction method.
"""
from enum import Enum
from typing import Optional, Any
from dataclasses import dataclass

class ExtractionContext(Enum):
    """Defines the context in which JSON extraction is being performed."""
    OBJECT_OPERATION = "object"    # For further JSON operations (default)
    TEXT_COMPARISON = "text"       # For string literal comparisons
    TEXT_DISPLAY = "text"         # For output/display purposes
    FILTERED_SINGLE = "filtered"   # For results of where() that should return scalars
    COLLECTION_OPERATION = "collection"  # For collection functions that need arrays
    UNKNOWN = "object"            # Default fallback

@dataclass
class ComparisonContext:
    """Context information for comparison operations."""
    operator: str                 # '=', '!=', '<>', etc.
    left_node: Any               # AST node for left operand
    right_node: Any              # AST node for right operand
    left_sql: str                # Generated SQL for left operand
    right_sql: str               # Generated SQL for right operand
    
    def is_string_literal_comparison(self) -> bool:
        """Determine if this is a comparison with a string literal."""
        # Check if right operand is a quoted string literal
        if self.right_sql.strip().startswith(("'", '"')):
            return True
        
        # Check if left operand is a quoted string literal  
        if self.left_sql.strip().startswith(("'", '"')):
            return True
            
        return False
    
    def should_use_text_extraction(self) -> bool:
        """Determine if text extraction should be used for this comparison."""
        return (self.operator in ['=', '!=', '<>', '~', '!~'] and 
                self.is_string_literal_comparison())

def determine_extraction_context(comparison_context: Optional[ComparisonContext] = None) -> ExtractionContext:
    """Determine appropriate extraction context based on usage."""
    if comparison_context and comparison_context.should_use_text_extraction():
        return ExtractionContext.TEXT_COMPARISON
    
    return ExtractionContext.OBJECT_OPERATION