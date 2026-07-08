"""
Logging Configuration for FHIR4DS

Provides centralized logging configuration with appropriate levels and formatters
for different components of the FHIR4DS system.

Features:
- Structured logging with consistent format
- Different log levels for different components
- File and console output options
- Performance logging capabilities
- Debug mode support

Usage:
    from fhir4ds.utils.logging_config import get_logger
    
    logger = get_logger(__name__)
    logger.info("Processing FHIRPath expression")
    logger.debug("Generated SQL: %s", sql)
    logger.warning("Fallback to legacy implementation")
    logger.error("Failed to parse expression: %s", error)
"""

import logging
import os
import sys
from typing import Optional, Dict, Any
from pathlib import Path


# Environment variable for controlling log level
LOG_LEVEL_ENV_VAR = 'FHIR4DS_LOG_LEVEL'

# Default logging configuration
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DEBUG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'

# Component-specific log levels
COMPONENT_LOG_LEVELS = {
    'fhir4ds.fhirpath': logging.INFO,
    'fhir4ds.datastore': logging.INFO,
    'fhir4ds.view_runner': logging.INFO,
    'fhir4ds.utils.performance': logging.INFO,
    'fhir4ds.helpers.performance': logging.INFO,
    # Debug level for development components
    'fhir4ds.fhirpath.core.generators': logging.DEBUG,
    'fhir4ds.fhirpath.core.cte_builder': logging.DEBUG,
}

# Global logger cache
_loggers: Dict[str, logging.Logger] = {}
_configured = False


def _get_log_level_from_env() -> int:
    """
    Get log level from environment variable.
    
    Returns:
        Log level integer (defaults to INFO if not set or invalid)
    """
    env_level = os.environ.get(LOG_LEVEL_ENV_VAR, '').upper()
    
    level_mapping = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    return level_mapping.get(env_level, DEFAULT_LOG_LEVEL)


def configure_logging(
    log_level: Optional[int] = None,
    log_file: Optional[str] = None,
    console_output: bool = True,
    debug_mode: bool = False,
    component_levels: Optional[Dict[str, int]] = None
) -> None:
    """
    Configure logging for the FHIR4DS application.
    
    Args:
        log_level: Default log level (default: from FHIR4DS_LOG_LEVEL env var or INFO)
        log_file: Optional file path for log output
        console_output: Whether to output to console (default: True)
        debug_mode: Enable debug mode with detailed formatting
        component_levels: Override log levels for specific components
    """
    global _configured
    
    if log_level is None:
        log_level = _get_log_level_from_env()
    
    # Choose formatter based on debug mode
    formatter = logging.Formatter(DEBUG_FORMAT if debug_mode else DEFAULT_FORMAT)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add console handler if requested
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Add file handler if requested
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Apply component-specific log levels
    levels_to_apply = COMPONENT_LOG_LEVELS.copy()
    if component_levels:
        levels_to_apply.update(component_levels)
    
    for component, level in levels_to_apply.items():
        logger = logging.getLogger(component)
        logger.setLevel(level)
    
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    global _configured
    
    # Auto-configure if not done yet
    if not _configured:
        configure_logging()
    
    # Return cached logger if available
    if name in _loggers:
        return _loggers[name]
    
    # Create new logger
    logger = logging.getLogger(name)
    
    # Apply component-specific settings if available
    for component, level in COMPONENT_LOG_LEVELS.items():
        if name.startswith(component):
            logger.setLevel(level)
            break
    
    _loggers[name] = logger
    return logger


def get_performance_logger(name: str) -> logging.Logger:
    """
    Get a logger optimized for performance monitoring.
    
    Args:
        name: Logger name
        
    Returns:
        Logger configured for performance logging
    """
    logger = get_logger(f"{name}.performance")
    logger.setLevel(logging.INFO)
    return logger


def set_debug_mode(enabled: bool = True) -> None:
    """
    Enable or disable debug mode for all loggers.
    
    Args:
        enabled: Whether to enable debug mode
    """
    level = logging.DEBUG if enabled else logging.INFO
    
    # Update root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Update all cached loggers
    for logger in _loggers.values():
        logger.setLevel(level)
    
    # Update component loggers
    for component in COMPONENT_LOG_LEVELS:
        logger = logging.getLogger(component)
        logger.setLevel(level)


def log_execution_time(logger: logging.Logger, operation: str, duration: float) -> None:
    """
    Log execution time for an operation.
    
    Args:
        logger: Logger instance to use
        operation: Name of the operation
        duration: Duration in seconds
    """
    if duration > 1.0:
        logger.warning("Slow operation: %s took %.3f seconds", operation, duration)
    else:
        logger.debug("Operation: %s completed in %.3f seconds", operation, duration)


def log_sql_generation(logger: logging.Logger, expression: str, sql: str, 
                      duration: Optional[float] = None) -> None:
    """
    Log SQL generation details.
    
    Args:
        logger: Logger instance to use
        expression: FHIRPath expression
        sql: Generated SQL
        duration: Optional generation time
    """
    if duration:
        logger.debug("Generated SQL for '%s' in %.3f seconds", expression, duration)
    else:
        logger.debug("Generated SQL for '%s'", expression)
    
    # Log SQL at debug level
    logger.debug("Generated SQL: %s", sql)


def log_fallback_usage(logger: logging.Logger, component: str, reason: str) -> None:
    """
    Log when fallback implementations are used.
    
    Args:
        logger: Logger instance to use
        component: Component using fallback
        reason: Reason for fallback
    """
    logger.warning("Using fallback implementation in %s: %s", component, reason)


def log_optimization_disabled(logger: logging.Logger, feature: str, reason: str) -> None:
    """
    Log when optimizations are disabled.
    
    Args:
        logger: Logger instance to use
        feature: Feature that was disabled
        reason: Reason for disabling
    """
    logger.info("Optimization disabled for %s: %s", feature, reason)


def log_error_with_context(logger: logging.Logger, error: Exception, 
                          context: Dict[str, Any]) -> None:
    """
    Log an error with additional context information.
    
    Args:
        logger: Logger instance to use
        error: Exception that occurred
        context: Additional context information
    """
    logger.error("Error occurred: %s", str(error))
    for key, value in context.items():
        logger.error("  %s: %s", key, value)


# Initialize logging on import
configure_logging()