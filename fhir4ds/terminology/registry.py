"""
Terminology Service Registry

Provides a registry system for managing multiple terminology services,
enabling extensible terminology architecture that can route requests
to appropriate services based on code systems and ValueSet URLs.
"""

import logging
from typing import Dict, Any, Optional, List, Type
from urllib.parse import urlparse

from .client.base_client import BaseTerminologyClient, TerminologyServiceError

logger = logging.getLogger(__name__)


class TerminologyServiceRegistry:
    """
    Registry for managing multiple terminology services.
    
    Supports routing terminology operations to appropriate services
    based on code systems, ValueSet URLs, and service capabilities.
    """
    
    def __init__(self):
        """Initialize the terminology service registry."""
        self.clients: Dict[str, BaseTerminologyClient] = {}
        self.system_mappings: Dict[str, str] = {}
        self.url_patterns: Dict[str, str] = {}
        self.default_client: Optional[str] = None
        
        logger.info("Initialized TerminologyServiceRegistry")
    
    def register_client(self, name: str, client: BaseTerminologyClient, 
                       is_default: bool = False) -> None:
        """
        Register a terminology client.
        
        Args:
            name: Unique name for this client
            client: Terminology client instance
            is_default: Whether this should be the default client
        """
        self.clients[name] = client
        
        if is_default or self.default_client is None:
            self.default_client = name
        
        logger.info(f"Registered terminology client: {name} (default: {is_default})")
    
    def register_system_mapping(self, code_system_url: str, client_name: str) -> None:
        """
        Map a code system URL to a specific client.
        
        Args:
            code_system_url: Code system URL to map
            client_name: Name of client to handle this code system
        """
        if client_name not in self.clients:
            raise ValueError(f"Client '{client_name}' not registered")
        
        self.system_mappings[code_system_url] = client_name
        logger.info(f"Mapped code system {code_system_url} to client {client_name}")
    
    def register_url_pattern(self, url_pattern: str, client_name: str) -> None:
        """
        Map URL patterns to specific clients.
        
        Args:
            url_pattern: URL pattern (supports wildcards)
            client_name: Name of client to handle URLs matching this pattern
        """
        if client_name not in self.clients:
            raise ValueError(f"Client '{client_name}' not registered")
        
        self.url_patterns[url_pattern] = client_name
        logger.info(f"Mapped URL pattern {url_pattern} to client {client_name}")
    
    def get_client_for_system(self, code_system_url: str) -> BaseTerminologyClient:
        """
        Get appropriate client for a code system.
        
        Args:
            code_system_url: Code system URL
            
        Returns:
            Appropriate terminology client
            
        Raises:
            TerminologyServiceError: If no appropriate client found
        """
        # Direct system mapping
        if code_system_url in self.system_mappings:
            client_name = self.system_mappings[code_system_url]
            return self.clients[client_name]
        
        # URL pattern matching
        for pattern, client_name in self.url_patterns.items():
            if self._matches_pattern(code_system_url, pattern):
                return self.clients[client_name]
        
        # Default client
        if self.default_client and self.default_client in self.clients:
            return self.clients[self.default_client]
        
        raise TerminologyServiceError(
            f"No terminology client found for code system: {code_system_url}"
        )
    
    def get_client_for_valueset(self, valueset_url: str) -> BaseTerminologyClient:
        """
        Get appropriate client for a ValueSet URL.
        
        Args:
            valueset_url: ValueSet canonical URL or identifier
            
        Returns:
            Appropriate terminology client
            
        Raises:
            TerminologyServiceError: If no appropriate client found
        """
        # URL pattern matching for ValueSets
        for pattern, client_name in self.url_patterns.items():
            if self._matches_pattern(valueset_url, pattern):
                return self.clients[client_name]
        
        # Default client
        if self.default_client and self.default_client in self.clients:
            return self.clients[self.default_client]
        
        raise TerminologyServiceError(
            f"No terminology client found for ValueSet: {valueset_url}"
        )
    
    def get_client_by_name(self, name: str) -> Optional[BaseTerminologyClient]:
        """
        Get client by name.
        
        Args:
            name: Client name
            
        Returns:
            Client instance or None if not found
        """
        return self.clients.get(name)
    
    def list_clients(self) -> List[Dict[str, Any]]:
        """
        List all registered clients with their information.
        
        Returns:
            List of client information dictionaries
        """
        clients_info = []
        for name, client in self.clients.items():
            info = client.get_service_info()
            info['registry_name'] = name
            info['is_default'] = name == self.default_client
            clients_info.append(info)
        
        return clients_info
    
    def expand_valueset(self, valueset_url: str, version: str = None,
                       parameters: Dict[str, Any] = None,
                       preferred_client: str = None) -> Dict[str, Any]:
        """
        Expand ValueSet using appropriate client.
        
        Args:
            valueset_url: ValueSet canonical URL
            version: Specific version (optional)
            parameters: Additional parameters
            preferred_client: Preferred client name (optional)
            
        Returns:
            FHIR ValueSet resource with expansion
        """
        if preferred_client and preferred_client in self.clients:
            client = self.clients[preferred_client]
        else:
            client = self.get_client_for_valueset(valueset_url)
        
        logger.debug(f"Expanding ValueSet {valueset_url} using client {client.__class__.__name__}")
        return client.expand_valueset(valueset_url, version, parameters)
    
    def validate_code(self, code: str, system: str, valueset_url: str = None,
                     display: str = None, preferred_client: str = None) -> Dict[str, Any]:
        """
        Validate code using appropriate client.
        
        Args:
            code: Code to validate
            system: Code system URL
            valueset_url: ValueSet URL (optional)
            display: Display name (optional)
            preferred_client: Preferred client name (optional)
            
        Returns:
            Validation result
        """
        if preferred_client and preferred_client in self.clients:
            client = self.clients[preferred_client]
        elif valueset_url:
            client = self.get_client_for_valueset(valueset_url)
        else:
            client = self.get_client_for_system(system)
        
        logger.debug(f"Validating code {code} in {system} using client {client.__class__.__name__}")
        return client.validate_code(code, system, valueset_url, display)
    
    def lookup_code(self, code: str, system: str, properties: List[str] = None,
                   preferred_client: str = None) -> Dict[str, Any]:
        """
        Lookup code using appropriate client.
        
        Args:
            code: Code to lookup
            system: Code system URL
            properties: Properties to return (optional)
            preferred_client: Preferred client name (optional)
            
        Returns:
            FHIR Parameters resource with code details
        """
        if preferred_client and preferred_client in self.clients:
            client = self.clients[preferred_client]
        else:
            client = self.get_client_for_system(system)
        
        logger.debug(f"Looking up code {code} in {system} using client {client.__class__.__name__}")
        return client.lookup_code(code, system, properties)
    
    def subsumes(self, code_a: str, code_b: str, system: str,
                preferred_client: str = None) -> Dict[str, Any]:
        """
        Test subsumption using appropriate client.
        
        Args:
            code_a: First code (potential parent)
            code_b: Second code (potential child)
            system: Code system URL
            preferred_client: Preferred client name (optional)
            
        Returns:
            Subsumption result
        """
        if preferred_client and preferred_client in self.clients:
            client = self.clients[preferred_client]
        else:
            client = self.get_client_for_system(system)
        
        logger.debug(f"Testing subsumption {code_a} subsumes {code_b} in {system} using client {client.__class__.__name__}")
        return client.subsumes(code_a, code_b, system)
    
    def get_registry_statistics(self) -> Dict[str, Any]:
        """
        Get registry statistics.
        
        Returns:
            Statistics about registered clients and mappings
        """
        return {
            'total_clients': len(self.clients),
            'default_client': self.default_client,
            'system_mappings': len(self.system_mappings),
            'url_patterns': len(self.url_patterns),
            'registered_clients': list(self.clients.keys())
        }
    
    def _matches_pattern(self, url: str, pattern: str) -> bool:
        """
        Check if URL matches pattern.
        
        Args:
            url: URL to check
            pattern: Pattern to match against (supports * wildcards)
            
        Returns:
            True if URL matches pattern
        """
        # Simple wildcard matching
        if '*' in pattern:
            # Convert wildcard pattern to regex-like matching
            import re
            regex_pattern = pattern.replace('*', '.*')
            return bool(re.match(regex_pattern, url))
        else:
            # Exact match or prefix match
            return url == pattern or url.startswith(pattern)


# Global registry instance
_global_registry: Optional[TerminologyServiceRegistry] = None


def get_global_registry() -> TerminologyServiceRegistry:
    """
    Get the global terminology service registry.
    
    Returns:
        Global registry instance
    """
    global _global_registry
    if _global_registry is None:
        _global_registry = TerminologyServiceRegistry()
    return _global_registry


def register_default_clients() -> None:
    """
    Register default terminology clients in the global registry.
    
    This function sets up the registry with VSAC and other default clients.
    """
    registry = get_global_registry()
    
    try:
        # Register VSAC client as default
        from .config import get_default_terminology_client
        vsac_client = get_default_terminology_client()
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
        
    except Exception as e:
        logger.warning(f"Failed to register VSAC client: {e}")


def setup_terminology_routing() -> TerminologyServiceRegistry:
    """
    Set up terminology routing with default clients.
    
    Returns:
        Configured terminology service registry
    """
    register_default_clients()
    return get_global_registry()