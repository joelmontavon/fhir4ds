"""
Error handling utilities for FHIRPath SQL generation.

This module provides standardized error handling patterns and utilities
for consistent error management throughout the FHIRPath codebase.
"""

import logging
from typing import Optional, Any, Callable, TypeVar, Type
from functools import wraps

from .exceptions import (
    FHIRPathError,
    FHIRPathValidationError,
    FHIRPathGenerationError,
    FHIRPathOptimizationError
)

T = TypeVar('T')


def validate_argument_count(
    function_name: str, 
    expected_count: int, 
    actual_count: int, 
    allow_variable: bool = False
) -> None:
    """
    Validate the number of arguments for a FHIRPath function.
    
    Args:
        function_name: Name of the function being validated
        expected_count: Expected number of arguments
        actual_count: Actual number of arguments provided
        allow_variable: Whether to allow variable number of arguments (>= expected_count)
    
    Raises:
        FHIRPathValidationError: If argument count validation fails
    """
    if allow_variable:
        if actual_count < expected_count:
            raise FHIRPathValidationError(
                f"Function '{function_name}' requires at least {expected_count} arguments, got {actual_count}",
                function_name=function_name,
                arg_count=actual_count
            )
    else:
        if actual_count != expected_count:
            raise FHIRPathValidationError(
                f"Function '{function_name}' requires exactly {expected_count} arguments, got {actual_count}",
                function_name=function_name,
                arg_count=actual_count
            )


def validate_argument_range(
    function_name: str, 
    min_count: int, 
    max_count: int, 
    actual_count: int
) -> None:
    """
    Validate that the number of arguments falls within a specific range.
    
    Args:
        function_name: Name of the function being validated
        min_count: Minimum number of arguments
        max_count: Maximum number of arguments
        actual_count: Actual number of arguments provided
    
    Raises:
        FHIRPathValidationError: If argument count is outside the valid range
    """
    if actual_count < min_count or actual_count > max_count:
        raise FHIRPathValidationError(
            f"Function '{function_name}' requires between {min_count} and {max_count} arguments, got {actual_count}",
            function_name=function_name,
            arg_count=actual_count
        )


def with_fallback(
    primary_func: Callable[..., T],
    fallback_func: Callable[..., T],
    logger: Optional[logging.Logger] = None,
    optimization_type: str = "unknown"
) -> Callable[..., T]:
    """
    Decorator that provides fallback behavior when primary function fails.
    
    Args:
        primary_func: The primary function to try first
        fallback_func: The fallback function to use if primary fails
        logger: Optional logger for recording fallback usage
        optimization_type: Type of optimization being attempted
    
    Returns:
        Function that tries primary first, then fallback
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            try:
                return primary_func(*args, **kwargs)
            except Exception as e:
                if logger:
                    logger.debug(f"Optimization failed for {optimization_type}, using fallback: {e}")
                
                # Raise optimization error but don't stop execution
                optimization_error = FHIRPathOptimizationError(
                    f"Optimization failed: {e}",
                    optimization_type=optimization_type
                )
                
                if logger:
                    logger.warning(f"Optimization error: {optimization_error}")
                
                return fallback_func(*args, **kwargs)
        return wrapper
    return decorator


def safe_evaluate(
    func: Callable[..., T],
    default_value: T,
    logger: Optional[logging.Logger] = None,
    error_context: str = "evaluation"
) -> Callable[..., T]:
    """
    Safely evaluate a function, returning default value on error.
    
    Args:
        func: Function to evaluate safely
        default_value: Value to return if function fails
        logger: Optional logger for error reporting
        error_context: Context description for error logging
    
    Returns:
        Function that safely evaluates with default fallback
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if logger:
                logger.debug(f"Safe evaluation failed in {error_context}: {e}")
            return default_value
    return wrapper


def handle_generation_error(
    func: Callable[..., T],
    node_type: str = None,
    fallback_available: bool = False
) -> Callable[..., T]:
    """
    Decorator for handling SQL generation errors consistently.
    
    Args:
        func: Function that performs SQL generation
        node_type: Type of AST node being processed
        fallback_available: Whether a fallback method is available
    
    Returns:
        Decorated function with consistent error handling
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        try:
            return func(*args, **kwargs)
        except FHIRPathError:
            # Re-raise FHIRPath errors as-is
            raise
        except Exception as e:
            # Convert generic exceptions to FHIRPathGenerationError
            raise FHIRPathGenerationError(
                f"SQL generation failed: {e}",
                node_type=node_type,
                fallback_available=fallback_available
            ) from e
    return wrapper


class ErrorContext:
    """Context manager for error handling with additional context."""
    
    def __init__(self, context: str, logger: Optional[logging.Logger] = None):
        self.context = context
        self.logger = logger
        
    def __enter__(self):
        if self.logger:
            self.logger.debug(f"Entering {self.context}")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if self.logger:
                self.logger.error(f"Error in {self.context}: {exc_val}")
            
            # Re-raise with additional context if it's not already a FHIRPathError
            if not isinstance(exc_val, FHIRPathError):
                raise FHIRPathGenerationError(
                    f"Error in {self.context}: {exc_val}"
                ) from exc_val
        
        if self.logger:
            self.logger.debug(f"Exiting {self.context}")
        return False  # Don't suppress exceptions


def create_validation_error(message: str, function_name: str = None, **kwargs) -> FHIRPathValidationError:
    """Helper function to create validation errors with consistent formatting."""
    return FHIRPathValidationError(message, function_name=function_name, **kwargs)


def create_generation_error(message: str, node_type: str = None, **kwargs) -> FHIRPathGenerationError:
    """Helper function to create generation errors with consistent formatting."""
    return FHIRPathGenerationError(message, node_type=node_type, **kwargs)