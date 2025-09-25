"""
Custom exception classes for FHIRPath SQL generation.

This module provides a hierarchy of custom exceptions that provide better
error handling and debugging capabilities for FHIRPath expression processing.
"""

class FHIRPathError(Exception):
    """Base exception for all FHIRPath-related errors."""
    pass


class FHIRPathSyntaxError(FHIRPathError):
    """Exception raised for FHIRPath syntax errors."""
    
    def __init__(self, message: str, expression: str = None, position: int = None):
        super().__init__(message)
        self.expression = expression
        self.position = position
        
    def __str__(self):
        if self.expression and self.position is not None:
            return f"{super().__str__()} in expression '{self.expression}' at position {self.position}"
        elif self.expression:
            return f"{super().__str__()} in expression '{self.expression}'"
        return super().__str__()


class FHIRPathValidationError(FHIRPathError):
    """Exception raised for validation errors in FHIRPath expressions."""
    
    def __init__(self, message: str, function_name: str = None, arg_count: int = None):
        super().__init__(message)
        self.function_name = function_name
        self.arg_count = arg_count
        
    def __str__(self):
        if self.function_name and self.arg_count is not None:
            return f"{super().__str__()} - Function '{self.function_name}' called with {self.arg_count} arguments"
        elif self.function_name:
            return f"{super().__str__()} - Function '{self.function_name}'"
        return super().__str__()


class FHIRPathTypeError(FHIRPathError):
    """Exception raised for type-related errors in FHIRPath expressions."""
    
    def __init__(self, message: str, expected_type: str = None, actual_type: str = None):
        super().__init__(message)
        self.expected_type = expected_type
        self.actual_type = actual_type
        
    def __str__(self):
        if self.expected_type and self.actual_type:
            return f"{super().__str__()} - Expected {self.expected_type}, got {self.actual_type}"
        return super().__str__()


class FHIRPathGenerationError(FHIRPathError):
    """Exception raised when SQL generation fails."""
    
    def __init__(self, message: str, node_type: str = None, fallback_available: bool = False):
        super().__init__(message)
        self.node_type = node_type
        self.fallback_available = fallback_available
        
    def __str__(self):
        base_msg = super().__str__()
        if self.node_type:
            base_msg += f" (Node type: {self.node_type})"
        if self.fallback_available:
            base_msg += " (Fallback available)"
        return base_msg


class FHIRPathOptimizationError(FHIRPathError):
    """Exception raised when optimization fails but fallback is available."""
    
    def __init__(self, message: str, optimization_type: str = None):
        super().__init__(message)
        self.optimization_type = optimization_type
        
    def __str__(self):
        if self.optimization_type:
            return f"{super().__str__()} (Optimization: {self.optimization_type})"
        return super().__str__()


class FHIRPathResourceError(FHIRPathError):
    """Exception raised for resource-related errors."""
    
    def __init__(self, message: str, resource_type: str = None):
        super().__init__(message)
        self.resource_type = resource_type
        
    def __str__(self):
        if self.resource_type:
            return f"{super().__str__()} (Resource type: {self.resource_type})"
        return super().__str__()