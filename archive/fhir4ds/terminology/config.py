"""
Terminology Service Configuration

Handles configuration for terminology services including API key management
and client initialization.
"""

import os
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def load_vsac_api_key() -> Optional[str]:
    """
    Load VSAC API key from secure location.

    Checks multiple sources in order of preference:
    1. .env file (preferred method)
    2. .vsac_api_key file in project root (legacy support)
    3. VSAC_API_KEY environment variable
    4. UMLS_API_KEY environment variable (alternative name)

    Returns:
        VSAC API key string, or None if not found
    """
    # First try centralized configuration (which handles .env files)
    try:
        from ..config import get_vsac_api_key
        api_key = get_vsac_api_key()
        if api_key:
            return api_key
    except ImportError:
        # Fallback if centralized config not available
        pass

    # Legacy support: Try .vsac_api_key file in project root
    try:
        key_file = Path(__file__).parent.parent.parent / '.vsac_api_key'
        if key_file.exists():
            api_key = key_file.read_text().strip()
            if api_key and api_key != 'your_vsac_api_key_here':
                logger.debug("VSAC API key loaded from .vsac_api_key file")
                return api_key
    except Exception as e:
        logger.warning(f"Failed to read .vsac_api_key file: {e}")

    # Try environment variables directly
    for env_var in ['VSAC_API_KEY', 'UMLS_API_KEY']:
        api_key = os.getenv(env_var)
        if api_key and api_key.strip() and api_key != 'your_vsac_api_key_here':
            logger.debug(f"VSAC API key loaded from {env_var} environment variable")
            return api_key.strip()

    logger.warning("No VSAC API key found. Terminology services will be unavailable.")
    logger.info("To enable VSAC integration:")
    logger.info("  1. Add VSAC_API_KEY to your .env file (recommended)")
    logger.info("  2. Or create .vsac_api_key file in project root with your UMLS API key")
    logger.info("  3. Or set VSAC_API_KEY environment variable")
    logger.info("  4. Get API key from: https://uts.nlm.nih.gov/uts/signup-login")

    return None


def get_default_terminology_client(db_connection=None, dialect: str = "duckdb"):
    """
    Get default terminology client with API key and caching using actual database.
    
    Args:
        db_connection: Database connection to use for caching
        dialect: Database dialect ("duckdb" or "postgresql")
    
    Returns:
        CachedVSACClient instance if API key available, None otherwise
    """
    api_key = load_vsac_api_key()
    if api_key:
        try:
            from .client.cached_vsac_client import CachedVSACClient
            client = CachedVSACClient(
                api_key=api_key,
                db_connection=db_connection,
                dialect=dialect
            )
            logger.info(f"Default VSAC terminology client with {dialect} caching initialized")
            return client
        except Exception as e:
            logger.error(f"Failed to initialize cached VSAC client: {e}")
            return None
    
    logger.info("No terminology client available - VSAC API key not found")
    return None


def get_vsac_client(api_key: str = None, base_url: str = None, 
                   enable_caching: bool = True, db_connection=None,
                   dialect: str = "duckdb") -> Optional['CachedVSACClient']:
    """
    Get VSAC client with optional custom parameters and caching.
    
    Args:
        api_key: Custom API key (uses default if None)
        base_url: Custom base URL (uses default if None)
        enable_caching: Whether to enable caching (default True)
        db_connection: Database connection to use for caching
        dialect: Database dialect ("duckdb" or "postgresql")
        
    Returns:
        CachedVSACClient instance or None if API key unavailable
    """
    if not api_key:
        api_key = load_vsac_api_key()
    
    if not api_key:
        return None
    
    try:
        from .client.cached_vsac_client import CachedVSACClient
        kwargs = {
            'api_key': api_key, 
            'enable_caching': enable_caching,
            'db_connection': db_connection,
            'dialect': dialect
        }
        if base_url:
            kwargs['base_url'] = base_url
            
        return CachedVSACClient(**kwargs)
    except Exception as e:
        logger.error(f"Failed to create cached VSAC client: {e}")
        return None


def test_vsac_connection(api_key: str = None) -> bool:
    """
    Test connection to VSAC service.
    
    Args:
        api_key: API key to test (uses default if None)
        
    Returns:
        True if connection successful, False otherwise
    """
    client = get_vsac_client(api_key)
    if not client:
        logger.error("Cannot test VSAC connection - no API key available")
        return False
    
    try:
        return client.test_connection()
    except Exception as e:
        logger.error(f"VSAC connection test failed: {e}")
        return False


def setup_default_registry(db_connection=None, dialect: str = "duckdb"):
    """
    Set up default terminology service registry with available clients.
    
    Args:
        db_connection: Database connection to use for caching
        dialect: Database dialect ("duckdb" or "postgresql")
        
    Returns:
        Configured TerminologyServiceRegistry
    """
    from .registry import TerminologyServiceRegistry
    from .client.mock_client import MockTerminologyClient
    
    registry = TerminologyServiceRegistry()
    
    # Register VSAC client if available
    vsac_client = get_default_terminology_client(db_connection, dialect)
    if vsac_client:
        registry.register_client('vsac', vsac_client, is_default=True)
        
        # Register common URL patterns for VSAC
        registry.register_url_pattern('urn:oid:*', 'vsac')
        registry.register_url_pattern('http://cts.nlm.nih.gov/fhir/ValueSet/*', 'vsac')
        registry.register_url_pattern('https://cts.nlm.nih.gov/fhir/ValueSet/*', 'vsac')
        
        # Register common VSAC code systems
        vsac_systems = [
            'http://snomed.info/sct',
            'http://hl7.org/fhir/sid/icd-10-cm',
            'http://hl7.org/fhir/sid/icd-9-cm',
            'http://www.nlm.nih.gov/research/umls/rxnorm',
            'http://loinc.org',
            'http://hl7.org/fhir/sid/cpt'
        ]
        
        for system in vsac_systems:
            registry.register_system_mapping(system, 'vsac')
        
        logger.info("Registered VSAC client with common system mappings")
    
    # Always register mock client for testing
    mock_client = MockTerminologyClient("Test Terminology Server")
    registry.register_client('mock', mock_client, is_default=(vsac_client is None))
    
    # Register test patterns for mock client
    registry.register_url_pattern('http://example.org/fhir/ValueSet/*', 'mock')
    registry.register_url_pattern('http://test.example.com/*', 'mock')
    
    logger.info(f"Set up terminology registry with {len(registry.clients)} clients")
    return registry


def get_mock_client() -> 'MockTerminologyClient':
    """
    Get a mock terminology client for testing.
    
    Returns:
        MockTerminologyClient instance
    """
    from .client.mock_client import MockTerminologyClient
    return MockTerminologyClient("Test Terminology Server")