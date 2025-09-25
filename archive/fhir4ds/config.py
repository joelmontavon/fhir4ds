"""
FHIR4DS Configuration Management

Centralized configuration management for FHIR4DS using environment variables
and .env file support.
"""

import os
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Try to load python-dotenv if available
try:
    from dotenv import load_dotenv
    _DOTENV_AVAILABLE = True
except ImportError:
    _DOTENV_AVAILABLE = False

# Global flag to track if .env has been loaded
_ENV_LOADED = False


def load_environment() -> None:
    """
    Load environment variables from .env file if available.

    This function is idempotent - it will only load the .env file once
    per application runtime.
    """
    global _ENV_LOADED

    if _ENV_LOADED:
        return

    if _DOTENV_AVAILABLE:
        env_file = Path(__file__).parent.parent / '.env'
        if env_file.exists():
            load_dotenv(env_file)
            logger.debug(f"Loaded environment variables from {env_file}")
        else:
            logger.debug("No .env file found, using system environment variables")
    else:
        logger.debug("python-dotenv not available, using system environment variables only")

    _ENV_LOADED = True


def get_env(key: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """
    Get environment variable with .env file support.

    Args:
        key: Environment variable name
        default: Default value if not found
        required: If True, raise ValueError if not found and no default

    Returns:
        Environment variable value or default

    Raises:
        ValueError: If required=True and variable not found with no default
    """
    # Ensure .env file is loaded
    load_environment()

    value = os.getenv(key, default)

    if required and value is None:
        raise ValueError(f"Required environment variable '{key}' not found")

    return value


def get_database_url(dialect: str = "postgresql") -> str:
    """
    Get database connection URL for specified dialect.

    Args:
        dialect: Database dialect ("postgresql" or "duckdb")

    Returns:
        Database connection URL
    """
    load_environment()

    if dialect.lower() == "postgresql":
        # Check for test environment first
        if os.getenv('PYTEST_CURRENT_TEST') or os.getenv('TESTING'):
            test_url = get_env('TEST_DATABASE_URL')
            if test_url:
                return test_url

        # Use main database URL
        db_url = get_env('DATABASE_URL')
        if db_url:
            return db_url

        # Fallback to default for Docker PostgreSQL
        return 'postgresql://postgres:postgres@localhost:5432/postgres'

    elif dialect.lower() == "duckdb":
        # DuckDB uses in-memory or file-based storage
        return get_env('DUCKDB_DATABASE', ':memory:')

    else:
        raise ValueError(f"Unsupported database dialect: {dialect}")


def get_vsac_api_key() -> Optional[str]:
    """
    Get VSAC API key from environment.

    Checks multiple environment variable names for compatibility.

    Returns:
        VSAC API key or None if not found
    """
    load_environment()

    # Check multiple possible environment variable names
    for key in ['VSAC_API_KEY', 'UMLS_API_KEY']:
        api_key = get_env(key)
        if api_key and api_key.strip() and api_key != 'your_vsac_api_key_here':
            logger.debug(f"VSAC API key loaded from {key} environment variable")
            return api_key.strip()

    return None


def get_terminology_config() -> dict:
    """
    Get terminology service configuration.

    Returns:
        Dictionary with terminology configuration
    """
    load_environment()

    return {
        'api_key': get_vsac_api_key(),
        'base_url': get_env('TERMINOLOGY_BASE_URL', 'https://cts.nlm.nih.gov/fhir'),
        'cache_enabled': get_env('TERMINOLOGY_CACHE_ENABLED', 'true').lower() == 'true',
    }


def get_debug_config() -> dict:
    """
    Get debug and development configuration.

    Returns:
        Dictionary with debug configuration
    """
    load_environment()

    return {
        'debug_mode': get_env('DEBUG_MODE', 'false').lower() == 'true',
        'log_level': get_env('LOG_LEVEL', 'INFO').upper(),
    }


def is_testing_environment() -> bool:
    """
    Check if running in a testing environment.

    Returns:
        True if in testing environment
    """
    load_environment()

    return bool(
        os.getenv('PYTEST_CURRENT_TEST') or
        os.getenv('TESTING') or
        get_env('DEBUG_MODE', 'false').lower() == 'true'
    )


def print_config_summary() -> None:
    """
    Print summary of current configuration (for debugging).

    Note: Does not print sensitive values like API keys.
    """
    load_environment()

    print("FHIR4DS Configuration Summary:")
    print(f"  Database URL: {get_database_url('postgresql')}")
    print(f"  DuckDB Path: {get_database_url('duckdb')}")
    print(f"  VSAC API Key: {'✅ Configured' if get_vsac_api_key() else '❌ Not configured'}")
    print(f"  Terminology Base URL: {get_env('TERMINOLOGY_BASE_URL', 'https://cts.nlm.nih.gov/fhir')}")
    print(f"  Cache Enabled: {get_env('TERMINOLOGY_CACHE_ENABLED', 'true')}")
    print(f"  Debug Mode: {get_env('DEBUG_MODE', 'false')}")
    print(f"  Log Level: {get_env('LOG_LEVEL', 'INFO')}")
    print(f"  Testing Environment: {is_testing_environment()}")


if __name__ == "__main__":
    # Print configuration when run directly
    print_config_summary()