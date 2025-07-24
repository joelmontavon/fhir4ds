"""
Cached VSAC Client

Integrates the VSAC terminology client with multi-tier caching for improved
performance and reduced API calls.
"""

import logging
from typing import Dict, Any, Optional, List

from .vsac_client import VSACClient
from ..cache.cache_manager import TerminologyCache

logger = logging.getLogger(__name__)


class CachedVSACClient(VSACClient):
    """
    VSAC client with multi-tier caching support.
    
    Extends the base VSAC client to add intelligent caching of terminology
    operations, significantly reducing API calls and improving performance.
    """
    
    def __init__(self, api_key: str, cache_manager: TerminologyCache = None, 
                 enable_caching: bool = True, db_connection=None, 
                 dialect: str = "duckdb", **kwargs):
        """
        Initialize cached VSAC client.
        
        Args:
            api_key: UMLS API key for authentication
            cache_manager: Custom cache manager (creates default if None)
            enable_caching: Whether to enable caching (useful for testing)
            db_connection: Database connection to use for caching
            dialect: Database dialect ("duckdb" or "postgresql")
            **kwargs: Additional arguments passed to VSACClient
        """
        super().__init__(api_key, **kwargs)
        
        self.enable_caching = enable_caching
        self.cache = cache_manager if enable_caching else None
        
        # Create default cache if none provided and caching is enabled
        if enable_caching and cache_manager is None:
            self.cache = TerminologyCache(
                db_connection=db_connection,
                dialect=dialect
            )
        
        if enable_caching:
            logger.info(f"Initialized CachedVSACClient with {dialect} caching enabled")
        else:
            logger.info("Initialized CachedVSACClient with caching disabled")
    
    def expand_valueset(self, valueset_url: str, version: str = None, 
                       parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Expand valueset with caching support.
        
        Checks cache first, then falls back to API call if not cached.
        Caches successful results for future use.
        
        Args:
            valueset_url: ValueSet canonical URL or VSAC OID
            version: Specific version (optional)
            parameters: Additional parameters (count, offset, etc.)
            
        Returns:
            FHIR ValueSet resource with expansion
            
        Raises:
            TerminologyServiceError: If expansion fails
        """
        if not self.enable_caching or not self.cache:
            # No caching - direct API call
            self._increment_api_calls()
            return super().expand_valueset(valueset_url, version, parameters)
        
        # Try cache first
        cached_result = self.cache.get_valueset_expansion(valueset_url, version, parameters)
        if cached_result:
            logger.debug(f"Cache hit for valueset expansion: {valueset_url}")
            return cached_result
        
        # Cache miss - call API
        logger.debug(f"Cache miss for valueset expansion: {valueset_url}")
        self._increment_api_calls()
        
        try:
            result = super().expand_valueset(valueset_url, version, parameters)
            
            # Cache successful result
            self.cache.cache_valueset_expansion(
                valueset_url=valueset_url,
                result=result,
                version=version,
                parameters=parameters,
                ttl=self._get_expansion_ttl(result)
            )
            
            logger.debug(f"Cached valueset expansion: {valueset_url}")
            return result
            
        except Exception as e:
            logger.warning(f"VSAC expansion failed for {valueset_url}: {e}")
            raise
    
    def validate_code(self, code: str, system: str, valueset_url: str = None,
                     display: str = None) -> Dict[str, Any]:
        """
        Validate code with caching support.
        
        Args:
            code: Code to validate
            system: Code system URL
            valueset_url: ValueSet URL (optional)
            display: Display name to validate (optional)
            
        Returns:
            FHIR Parameters resource with validation result
        """
        if not self.enable_caching or not self.cache:
            # No caching - direct API call
            self._increment_api_calls()
            return super().validate_code(code, system, valueset_url, display)
        
        # Try cache first
        cached_validation = self.cache.get_code_validation(code, system, valueset_url)
        if cached_validation is not None:
            logger.debug(f"Cache hit for code validation: {code} in {system}")
            # Convert boolean to FHIR Parameters format
            return self._create_validation_response(cached_validation, code, display)
        
        # Cache miss - call API
        logger.debug(f"Cache miss for code validation: {code} in {system}")
        self._increment_api_calls()
        
        try:
            result = super().validate_code(code, system, valueset_url, display)
            
            # Extract validation result and cache it
            is_valid = self._extract_validation_result(result)
            self.cache.cache_code_validation(
                code=code,
                system=system,
                is_valid=is_valid,
                valueset_url=valueset_url,
                display=display,
                ttl=86400  # 1 day for validations
            )
            
            logger.debug(f"Cached code validation: {code} in {system} = {is_valid}")
            return result
            
        except Exception as e:
            logger.warning(f"VSAC validation failed for {code}: {e}")
            raise
    
    def lookup_code(self, code: str, system: str, 
                   properties: List[str] = None) -> Dict[str, Any]:
        """
        Lookup code with caching support.
        
        Args:
            code: Code to lookup
            system: Code system URL
            properties: Properties to return (optional)
            
        Returns:
            FHIR Parameters resource with code details
        """
        # For code lookup, we could add caching but it's less critical
        # than expansions and validations. For now, call API directly.
        self._increment_api_calls()
        return super().lookup_code(code, system, properties)
    
    def subsumes(self, code_a: str, code_b: str, system: str) -> Dict[str, Any]:
        """
        Test subsumption with caching support.
        
        Args:
            code_a: First code (potential parent)
            code_b: Second code (potential child)
            system: Code system URL
            
        Returns:
            FHIR Parameters resource with subsumption result
        """
        # For subsumption, we could add caching but it's less common
        # For now, call API directly.
        self._increment_api_calls()
        return super().subsumes(code_a, code_b, system)
    
    def _increment_api_calls(self):
        """Increment API call counter in cache stats."""
        if self.cache:
            self.cache.stats['api_calls'] += 1
    
    def _get_expansion_ttl(self, expansion_result: Dict[str, Any]) -> int:
        """
        Determine appropriate TTL for a valueset expansion.
        
        Args:
            expansion_result: FHIR ValueSet expansion result
            
        Returns:
            TTL in seconds
        """
        # Default TTL is 7 days
        default_ttl = 604800
        
        try:
            # Check if the expansion includes version info
            expansion = expansion_result.get('expansion', {})
            
            # If it's a versioned expansion, cache longer
            if expansion.get('timestamp') or expansion.get('parameter'):
                return default_ttl
            
            # For unversioned expansions, shorter TTL
            return 86400  # 1 day
            
        except Exception:
            return default_ttl
    
    def _extract_validation_result(self, validation_response: Dict[str, Any]) -> bool:
        """
        Extract boolean validation result from FHIR Parameters response.
        
        Args:
            validation_response: FHIR Parameters response from $validate-code
            
        Returns:
            Boolean validation result
        """
        try:
            # FHIR Parameters response format
            parameters = validation_response.get('parameter', [])
            for param in parameters:
                if param.get('name') == 'result':
                    return param.get('valueBoolean', False)
            return False
        except Exception:
            return False
    
    def _create_validation_response(self, is_valid: bool, code: str, 
                                  display: str = None) -> Dict[str, Any]:
        """
        Create FHIR Parameters response for cached validation result.
        
        Args:
            is_valid: Boolean validation result
            code: Code that was validated
            display: Display name (optional)
            
        Returns:
            FHIR Parameters resource
        """
        parameters = [
            {
                "name": "result",
                "valueBoolean": is_valid
            }
        ]
        
        if code:
            parameters.append({
                "name": "code",
                "valueCode": code
            })
        
        if display:
            parameters.append({
                "name": "display",
                "valueString": display
            })
        
        return {
            "resourceType": "Parameters",
            "parameter": parameters
        }
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive cache statistics.
        
        Returns:
            Dictionary with cache performance metrics
        """
        if not self.cache:
            return {
                "caching_enabled": False,
                "message": "Caching is disabled"
            }
        
        stats = self.cache.get_cache_stats()
        stats["caching_enabled"] = True
        stats["cache_type"] = "multi-tier"
        
        return stats
    
    def clear_cache(self):
        """Clear all cached data."""
        if self.cache:
            self.cache.clear_all_caches()
            logger.info("Cleared all VSAC cache data")
        else:
            logger.warning("No cache to clear - caching is disabled")
    
    def clear_expired_cache(self):
        """Clear only expired cache entries."""
        if self.cache:
            self.cache.clear_expired()
            logger.info("Cleared expired VSAC cache entries")
        else:
            logger.warning("No cache to clear - caching is disabled")
    
    def get_service_info(self) -> Dict[str, Any]:
        """Get VSAC service information including cache details."""
        info = super().get_service_info()
        
        # Add cache information
        info['caching'] = {
            'enabled': self.enable_caching,
            'type': 'multi-tier' if self.enable_caching else None,
            'stats': self.get_cache_stats() if self.enable_caching else None
        }
        
        return info